"""
Pipeline 3 — Генерация видео.

Принимает batch_id, определяет нужно ли возобновление (video_pending)
или новая генерация (story_ready), отправляет промпт в fal.ai,
поллит статус и сохраняет video_url в batches.

Статусы батча:
  story_ready → video_generating → video_pending → video_ready
                                                  → video_error
"""

import time

import common.environment as environment
from db import (
    db_set_batch_video_model,
    db_get,
    db_get_batch_by_id,
    db_get_active_targets,
    db_set_batch_video_generating_by_id,
    db_get_story_text,
    db_get_active_video_models,
    db_get_video_model_by_id,
    db_set_batch_video_pending,
    db_set_batch_video_ready,
    db_set_batch_status,
    db_set_batch_story_ready_from_error,
    db_set_batch_original_video,
    db_get_random_real_original_video,
    db_set_batch_story_id,
    db_get_movie_from_donor,
)
from log import write_log, db_log_update, write_log_entry
from pipelines.base import check_cancelled, iterate_models
from common.exceptions import AppException
from clients import falai
from clients.falai import ProviderFatalError
from utils.utils import fmt_id_msg, nearest_allowed_duration


class AspectRatioConflictError(Exception):
    """Raised when active targets have different aspect ratios."""


def run(batch_id, log_id):
    try:
        batch = db_get_batch_by_id(batch_id)
        if not batch:
            return

        status = batch['status']

        if status == 'video_pending':
            resumed = True
        elif status in ('story_ready', 'video_generating'):
            resumed = False
            if status == 'story_ready':
                if not db_set_batch_video_generating_by_id(batch_id):
                    return
        else:
            return

        is_probe        = batch['type'] == 'movie_probe'
        if is_probe:
            target = 'пробный'
            ar_x   = 9
            ar_y   = 16
        else:
            active_targets = db_get_active_targets()
            if len(active_targets) > 1:
                ratios = {
                    (t.get('aspect_ratio_x') or 9, t.get('aspect_ratio_y') or 16)
                    for t in active_targets
                }
                if len(ratios) > 1:
                    details = ', '.join(
                        f"{t.get('name','?')} → {t.get('aspect_ratio_x') or 9}:{t.get('aspect_ratio_y') or 16}"
                        for t in active_targets
                    )
                    raise AspectRatioConflictError(
                        f"Конфликт соотношений сторон у активных таргетов: {details}"
                    )
            tgt    = active_targets[0] if active_targets else {}
            target = tgt.get('name') or 'adhoc'
            ar_x   = tgt.get('aspect_ratio_x') or 9
            ar_y   = tgt.get('aspect_ratio_y') or 16
        if not resumed and not is_probe:
            batch_data = batch.get('data') or {}
            donor_batch_id = batch_data.get('donor_batch_id') if isinstance(batch_data, dict) else None
            if donor_batch_id:
                if batch.get('movie_id') is not None:
                    write_log_entry(None, fmt_id_msg("[video] Батч {} — донор, movie_id уже перенесён (возобновление)", batch_id))
                    return
                if not check_cancelled('video', batch_id, batch, log_id):
                    write_log_entry(None, fmt_id_msg("[video] Батч {} — режим донора, переносим видео от {}", batch_id, donor_batch_id))
                    db_log_update(log_id, 'Видео получено от донора', 'running')
                    result_donor_id = db_get_movie_from_donor(donor_batch_id, batch_id)
                    if result_donor_id:
                        updated = db_get_batch_by_id(batch_id)
                        new_status = updated['status'] if updated else None
                        detail = fmt_id_msg("Видео перенесено от донора {}", donor_batch_id)
                        db_log_update(log_id, detail, 'ok')
                        write_log_entry(log_id, detail)
                        if new_status == 'transcode_ready':
                            # Обоснованное исключение: транскод-пайплайн не будет запущен,
                            # т.к. статус уже transcode_ready; запись создаётся здесь,
                            # чтобы в мониторе отображался лог для пропущенного шага.
                            tr_log_id = write_log(
                                'transcode', 'Транскодирование пропущено — видео получено от донора',
                                status='ok', batch_id=batch_id,
                            )
                            write_log_entry(tr_log_id, detail)
                        write_log_entry(None, fmt_id_msg("[video] Батч {} — видео от донора, новый статус: {}", batch_id, new_status))
                    else:
                        msg = fmt_id_msg("Не удалось перенести видео от донора {}", donor_batch_id)
                        db_log_update(log_id, msg, 'error')
                        write_log_entry(log_id, msg, level='error')
                        write_log_entry(None, f"[video] {msg}")
                        raise AppException(batch_id, 'video', msg, log_id)
                    return

        story_id        = batch.get('story_id')
        if story_id is None:
            raise RuntimeError('story_id отсутствует у батча в статусе story_ready/video_generating — ошибка логики')
        story_id        = str(story_id)
        pinned_model_id = batch.get('video_model_id')

        try:
            video_duration = max(1, min(60, int(db_get('video_duration', '6'))))
        except (ValueError, TypeError):
            video_duration = 6

        if not is_probe and check_cancelled('video', batch_id, batch, log_id):
            return

        if environment.emulation_mode:
            write_log_entry(None, fmt_id_msg("[video] Батч {} — эмуляция генерации видео", batch_id))
            db_log_update(log_id, 'Видео [эмуляция]', 'running')
            result = db_get_random_real_original_video()
            if result is None:
                msg = '[эмуляция] Нет видео в пуле — невозможно скопировать оригинал'
                db_log_update(log_id, msg, 'error')
                write_log_entry(log_id, msg, level='error')
                write_log_entry(None, f"[video] {msg}")
                raise AppException(batch_id, 'video', msg, log_id)
            sample, donor_batch_id, donor_story_id = result
            write_log_entry(log_id, fmt_id_msg("Видео заимствовано из батча: {}", donor_batch_id), level='info')
            db_set_batch_original_video(batch_id, sample)
            if donor_story_id:
                db_set_batch_story_id(batch_id, donor_story_id)
                write_log_entry(log_id, fmt_id_msg("story_id подменён: {} → {}", story_id, donor_story_id), level='info')
            db_set_batch_video_ready(batch_id, 'emulation://skipped')
            db_log_update(log_id, 'Видео [эмуляция]', 'ok')
            return

        write_log_entry(None, fmt_id_msg("[video] Батч {} ({}) — {} генерации видео",
                                      batch_id, target, 'возобновление' if resumed else 'начало'))

        if not falai.is_configured():
            msg = 'FAL_API_KEY не задан — генерация невозможна'
            db_log_update(log_id, msg, 'error')
            write_log_entry(None, f"[video] {msg}")
            raise AppException(batch_id, 'video', msg, log_id)

        try:
            fails_to_next = max(1, int(db_get('video_fails_to_next', '3')))
        except (ValueError, TypeError):
            fails_to_next = 3

        if pinned_model_id:
            pinned_m = db_get_video_model_by_id(pinned_model_id)
            if not pinned_m:
                msg = fmt_id_msg('Пробная модель {} не найдена', pinned_model_id)
                db_log_update(log_id, msg, 'error')
                write_log_entry(None, f"[video] {msg}")
                raise AppException(batch_id, 'video', msg, log_id)
            models = [pinned_m]
        else:
            models = db_get_active_video_models()
            if not models:
                msg = 'Нет активных video-моделей в ai_models (type=text-to-video)'
                db_log_update(log_id, msg, 'error')
                write_log_entry(None, f"[video] {msg}")
                raise AppException(batch_id, 'video', msg, log_id)

        story_text = db_get_story_text(story_id)
        if not story_text:
            msg = fmt_id_msg('Не найден сюжет story_id={}', story_id)
            db_log_update(log_id, msg, 'error')
            write_log_entry(None, f"[video] {msg}")
            raise AppException(batch_id, 'video', msg, log_id)

        video_post_prompt = db_get('video_post_prompt', '').strip()
        if video_post_prompt:
            video_post_prompt = video_post_prompt.replace('{продолжительность}', str(video_duration))
            story_text = story_text + '\n\n' + video_post_prompt

        db_log_update(log_id, f"Генерация видео… {'(возобновление)' if resumed else ''}".strip(), 'running')
        write_log_entry(log_id, f"Соотношение сторон: {ar_x}:{ar_y}")
        if is_probe:
            write_log_entry(log_id, f"Пробная модель: {models[0]['name']}, попыток: {fails_to_next}")
        else:
            write_log_entry(log_id, f"Моделей: {len(models)}, попыток на модель: {fails_to_next}")

        used_model    = None
        used_model_id = None
        attempt_counts = {}

        def video_callback(m):
            """Один шаг перебора: resume-поллинг или свежий submit → поллинг.

            Читает request_id из актуального batch.data:
            - Если не None — это возобновление (или уже сохранённый handshake),
              передаёт управление существующей логике поллинга.
            - Если None — обычный submit через falai.submit().

            При любом неуспехе безусловно сбрасывает все четыре поля
            (request_id, status_url, response_url, model_id) в batch.data,
            чтобы следующая попытка шла через submit() заново.

            Возвращает video_url при успехе или None при неудаче.
            """
            nonlocal used_model, used_model_id
            model_name = m['name']
            cnt = attempt_counts.get(model_name, 0)
            attempt_counts[model_name] = cnt + 1

            current_batch = db_get_batch_by_id(batch_id)
            current_data  = (current_batch.get('data') or {}) if current_batch else {}
            saved_request_id   = current_data.get('request_id')
            saved_status_url   = current_data.get('status_url')
            saved_response_url = current_data.get('response_url')
            saved_model_id     = current_data.get('model_id')

            if saved_request_id:
                write_log_entry(log_id, fmt_id_msg("Возобновление: request_id={}", saved_request_id))
                video_url, poll_err = falai.poll(log_id, saved_status_url, saved_response_url)
                if video_url:
                    used_model_id = saved_model_id
                    return video_url
                write_log_entry(log_id, f"Поллинг не дал результата, сброс handshake", level='warn')
                db_set_batch_video_pending(batch_id, {
                    'request_id':   None,
                    'status_url':   None,
                    'response_url': None,
                    'model_id':     None,
                })
                return None

            if cnt == 0:
                write_log_entry(log_id, f"Модель: {model_name}")
                write_log_entry(None, f"[video] Запрос к провайдеру: модель={model_name}, ar={ar_x}:{ar_y}")
            else:
                write_log_entry(log_id, f"[{model_name}] попытка {cnt + 1}/{fails_to_next}", level='warn')

            allowed = m.get('allowed_durations') or [0]
            actual_duration = nearest_allowed_duration(video_duration, allowed)
            if actual_duration != video_duration:
                write_log_entry(log_id, f"Длительность скорректирована: {video_duration}с → {actual_duration}с (модель поддерживает: {allowed})", level='warn')

            sr = falai.submit(
                log_id, model_name, m['submit_url'], m['platform_url'],
                m['body_tpl'], story_text, ar_x, ar_y, actual_duration,
            )
            if not sr:
                return None

            req_id = sr['request_id']
            # status_url и response_url получены от провайдера в ответ на submit и могут
            # отличаться от шаблонного паттерна (platform_url/requests/…), поэтому
            # хранятся явно в batch.data, а не восстанавливаются из шаблона при возобновлении.
            s_url  = sr['status_url']
            r_url  = sr['response_url']
            m_id   = str(m['id'])

            db_set_batch_video_pending(batch_id, {
                'request_id':   req_id,
                'status_url':   s_url,
                'response_url': r_url,
                'model_id':     m_id,
            })
            write_log_entry(log_id, fmt_id_msg("Запрос принят ({}): {}", model_name, req_id))
            write_log_entry(None, fmt_id_msg("[video] Генерация запущена: request_id={}", req_id))

            video_url, poll_err = falai.poll(log_id, s_url, r_url)
            if video_url:
                used_model    = model_name
                used_model_id = m_id
                return video_url

            write_log_entry(log_id, f"Поллинг не дал результата, сброс handshake", level='warn')
            db_set_batch_video_pending(batch_id, {
                'request_id':   None,
                'status_url':   None,
                'response_url': None,
                'model_id':     None,
            })
            return None

        max_passes = 1 if is_probe else 5
        video_url = iterate_models(models, fails_to_next, video_callback, max_passes=max_passes)

        if not video_url:
            if is_probe:
                msg = f'Пробная модель {models[0]["name"]} не дала результата ({fails_to_next} попыток)'
                db_log_update(log_id, msg, 'error')
                write_log_entry(log_id, msg, level='error')
                write_log_entry(None, f"[video] {msg}")
                raise AppException(batch_id, 'video', msg, log_id)
            else:
                msg = f'Все активные видео-модели не дали результата после {max_passes} проходов'
                db_log_update(log_id, msg, 'error')
                write_log_entry(log_id, msg, level='error')
                write_log_entry(None, f"[video] {msg}")
                db_set_batch_story_ready_from_error(batch_id)
                return

        write_log_entry(log_id, f"Скачиваю оригинал: {video_url}")
        write_log_entry(None, f"[video] Скачиваю оригинал от провайдера…")
        try:
            original_data = falai.download_video(log_id, video_url)
            orig_mb = round(len(original_data) / 1024 / 1024, 1)
            db_set_batch_original_video(batch_id, original_data)
            write_log_entry(log_id, f"Оригинал сохранён ({orig_mb} МБ)")
            write_log_entry(None, f"[video] Оригинал сохранён в БД ({orig_mb} МБ)")
        except Exception as e:
            msg = f"Ошибка скачивания оригинала: {e}"
            db_log_update(log_id, msg, 'error')
            write_log_entry(log_id, msg, level='error')
            write_log_entry(None, f"[video] {msg}")
            raise AppException(batch_id, 'video', msg, log_id)

        db_set_batch_video_ready(batch_id, video_url)
        if used_model_id:
            db_set_batch_video_model(batch_id, used_model_id)
        msg = f'Видео сгенерировано ({used_model})' if used_model else 'Видео сгенерировано'
        db_log_update(log_id, msg, 'ok')
        write_log_entry(log_id, f"URL: {video_url}")
        write_log_entry(None, f"[video] Готово: batch → video_ready, url={video_url[:60]}…")

    except AppException:
        raise
    except AspectRatioConflictError as e:
        msg = str(e)
        db_log_update(log_id, msg, 'error')
        write_log_entry(None, f"[video] {msg}")
        raise AppException(batch_id, 'video', msg, log_id)
    except ProviderFatalError as e:
        msg = f"Фатальная ошибка провайдера: {e}"
        db_log_update(log_id, msg, 'error')
        write_log_entry(None, f"[video] {msg}")
        raise AppException(batch_id, 'video', msg, log_id)
