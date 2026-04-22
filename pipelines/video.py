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
from utils.prompt_params import apply_prompt_params
from db import (
    settings_get,
    db_get_batch_by_id,
    db_get_active_targets,
    db_update_batch_current_movie_model_id,
    db_claim_batch_status,
    db_get_story_text,
    db_get_active_video_models,
    db_get_video_model_by_id,
    db_save_video_job_and_set_pending,
    db_set_batch_status,
    db_create_batch_movie,
    db_copy_movie_for_emulation,
    db_get_random_real_original_video,
    db_link_batch_story_only,
    db_transfer_donor_movie,
)
from log import write_log, db_log_update, write_log_entry
from pipelines.base import check_cancelled, iterate_models
from common.exceptions import AppException
from clients import falai, grok, skyreels
from clients.falai import ProviderFatalError
from utils.utils import fmt_id_msg, nearest_allowed_duration


def _video_client(platform_name: str):
    """Возвращает клиент нужной платформы по имени."""
    if platform_name == 'Grok':
        return grok
    if platform_name == 'SkyReels':
        return skyreels
    return falai


class AspectRatioConflictError(Exception):
    """Raised when active targets have different aspect ratios."""


def run(batch_id, log_id):
    snap = environment.snapshot()
    try:
        batch = db_get_batch_by_id(batch_id)
        if not batch:
            return

        status = batch['status']

        # Три допустимых входных статуса:
        # - video_pending:    handshake с провайдером уже существует — это подхват
        #                     прерванного поллинга. Генерацию не перезапускаем.
        # - story_ready:      сюжет готов, видео ещё не стартовало. CAS-переход
        #                     story_ready → video_generating защищает от двойного подхвата:
        #                     если другой воркер успел раньше, возвращаем 0 строк → выходим.
        # - video_generating: пайплайн был прерван после CAS, но до первого submit.
        #                     Подхватываем без повторного CAS (статус уже выставлен).
        if status == 'video_pending':
            resumed = True
        elif status in ('story_ready', 'video_generating'):
            resumed = False
            if status == 'story_ready':
                if not db_claim_batch_status(batch_id, 'story_ready', 'video_generating'):
                    return
        else:
            return

        # is_probe: батч создан вручную для тестирования конкретной модели (movie_probe).
        # Для проба aspect ratio жёстко 9:16 — результат идёт на просмотр, а не в эфир.
        # Для slot/adhoc aspect ratio берётся из конфигурации активного таргета.
        # Конфликт соотношений у нескольких таргетов — фатальная ошибка конфигурации.
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

        # Режим пула (donor): только для slot/adhoc и только при первом запуске (not resumed).
        # Story-пайплайн уже записал donor_batch_id в batches.data; здесь переносим
        # готовое видео из донорского батча, минуя генерацию полностью.
        # Для probe пул не используется — нам важен результат конкретной модели.
        if not resumed and not is_probe:
            batch_data = batch.get('data') or {}
            donor_batch_id = batch_data.get('donor_batch_id') if isinstance(batch_data, dict) else None
            if donor_batch_id:
                if not check_cancelled('video', batch_id, batch, log_id):
                    write_log_entry(log_id, fmt_id_msg("[video] Батч {} — режим пула, переносим видео из пула {}", batch_id, donor_batch_id))
                    db_log_update(log_id, 'Видео получено из пула', 'running')
                    result_donor_id = db_transfer_donor_movie(donor_batch_id, batch_id)
                    if result_donor_id:
                        updated = db_get_batch_by_id(batch_id)
                        new_status = updated['status'] if updated else None
                        detail = fmt_id_msg("Видео подобрано из пула {}", donor_batch_id)
                        db_log_update(log_id, detail, 'ok')
                        write_log_entry(log_id, detail)
                        if new_status == 'transcode_ready':
                            # Обоснованное исключение: транскод-пайплайн не будет запущен,
                            # т.к. статус уже transcode_ready; запись создаётся здесь,
                            # чтобы в мониторе отображался лог для пропущенного шага.
                            tr_log_id = write_log(
                                'transcode', 'Транскодирование пропущено — видео получено из пула',
                                status='ok', batch_id=batch_id,
                            )
                            write_log_entry(tr_log_id, detail)
                        write_log_entry(log_id, fmt_id_msg("[video] Батч {} — видео из пула, новый статус: {}", batch_id, new_status))
                    else:
                        msg = fmt_id_msg("Не удалось получить видео из пула {}", donor_batch_id)
                        db_log_update(log_id, msg, 'error')
                        write_log_entry(log_id, msg, level='error')
                        write_log_entry(log_id, f"[video] {msg}")
                        raise AppException(batch_id, 'video', msg, log_id)
                    return

        story_id        = batch.get('story_id')
        if story_id is None:
            raise RuntimeError('story_id отсутствует у батча в статусе story_ready/video_generating — ошибка логики')
        story_id        = str(story_id)
        batch_data      = batch.get('data') or {}
        pinned_model_id = batch_data.get('movie_model_id') if isinstance(batch_data, dict) else None

        try:
            video_duration = max(1, min(60, int(settings_get('video_duration', '6'))))
        except (ValueError, TypeError):
            video_duration = 6

        # Проверка отмены: только для slot/adhoc.
        # Probe-батч запускается пользователем явно — отменять его нет смысла.
        if not is_probe and check_cancelled('video', batch_id, batch, log_id):
            return

        # Эмуляция: заимствуем оригинальное видео из случайного реального батча в пуле.
        # Используется при разработке, когда дёргать реального провайдера нежелательно.
        if snap.emulation_mode:
            write_log_entry(log_id, fmt_id_msg("[video] Батч {} — эмуляция генерации видео", batch_id))
            db_log_update(log_id, 'Видео [эмуляция]', 'running')
            result = db_get_random_real_original_video()
            if result is None:
                msg = '[эмуляция] Нет видео в пуле — невозможно скопировать оригинал'
                db_log_update(log_id, msg, 'error')
                write_log_entry(log_id, msg, level='error')
                write_log_entry(log_id, f"[video] {msg}")
                raise AppException(batch_id, 'video', msg, log_id)
            donor_movie_id, donor_batch_id, donor_story_id = result
            write_log_entry(log_id, fmt_id_msg("Видео заимствовано из батча: {}", donor_batch_id), level='info')
            db_copy_movie_for_emulation(donor_movie_id, batch_id)
            if donor_story_id:
                db_link_batch_story_only(batch_id, donor_story_id)
                write_log_entry(log_id, fmt_id_msg("story_id подменён: {} → {}", story_id, donor_story_id), level='info')
            db_set_batch_status(batch_id, 'video_ready')
            db_log_update(log_id, 'Видео [эмуляция]', 'ok')
            return

        write_log_entry(log_id, fmt_id_msg("[video] Батч {} ({}) — {} генерации видео",
                                      batch_id, target, 'возобновление' if resumed else 'начало'))

        if not falai.is_configured():
            msg = 'FAL_API_KEY не задан — генерация невозможна'
            db_log_update(log_id, msg, 'error')
            write_log_entry(log_id, f"[video] {msg}")
            raise AppException(batch_id, 'video', msg, log_id)

        try:
            max_attempts_per_model = max(1, int(settings_get('video_fails_to_next', '3')))
        except (ValueError, TypeError):
            max_attempts_per_model = 3

        # Выбор набора моделей:
        # - pinned_model_id (из batches.data.movie_model_id): пользователь жёстко
        #   задал модель → список из одного элемента, один проход (max_passes=1).
        # - нет pinned: перебор всех активных моделей с max_passes проходами.
        if pinned_model_id:
            pinned_m = db_get_video_model_by_id(pinned_model_id)
            if not pinned_m:
                msg = fmt_id_msg('Пробная модель {} не найдена', pinned_model_id)
                db_log_update(log_id, msg, 'error')
                write_log_entry(log_id, f"[video] {msg}")
                raise AppException(batch_id, 'video', msg, log_id)
            models = [pinned_m]
        else:
            models = db_get_active_video_models()
            if not models:
                msg = 'Нет активных video-моделей в ai_models (type=text-to-video)'
                db_log_update(log_id, msg, 'error')
                write_log_entry(log_id, f"[video] {msg}")
                raise AppException(batch_id, 'video', msg, log_id)

        story_text = db_get_story_text(story_id)
        if not story_text:
            msg = fmt_id_msg('Не найден сюжет story_id={}', story_id)
            db_log_update(log_id, msg, 'error')
            write_log_entry(log_id, f"[video] {msg}")
            raise AppException(batch_id, 'video', msg, log_id)

        video_post_prompt = settings_get('video_post_prompt', '').strip()
        if video_post_prompt:
            video_post_prompt = apply_prompt_params(video_post_prompt)
            story_text = story_text + '\n\n' + video_post_prompt

        db_log_update(log_id, f"Генерация видео… {'(возобновление)' if resumed else ''}".strip(), 'running')
        write_log_entry(log_id, f"Соотношение сторон: {ar_x}:{ar_y}")
        if is_probe:
            write_log_entry(log_id, f"Пробная модель: {models[0]['name']}, попыток: {max_attempts_per_model}")
        else:
            write_log_entry(log_id, f"Моделей: {len(models)}, попыток на модель: {max_attempts_per_model}")

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
            (request_id, status_url, response_url, current_movie_model_id) в batch.data,
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
            client = _video_client(m.get('platform_name', ''))

            if saved_request_id:
                write_log_entry(log_id, fmt_id_msg("Возобновление: request_id={}", saved_request_id))
                video_url, poll_err = client.poll(log_id, saved_status_url, saved_response_url)
                if video_url:
                    used_model_id = str(m['id'])
                    return video_url
                write_log_entry(log_id, f"Поллинг не дал результата, сброс handshake", level='warn')
                db_save_video_job_and_set_pending(batch_id, {
                    'request_id':   None,
                    'status_url':   None,
                    'response_url': None,
                })
                return None

            if cnt == 0:
                db_update_batch_current_movie_model_id(batch_id, m['id'])
                write_log_entry(log_id, f"Модель: {model_name}")
                write_log_entry(log_id, f"[video] Запрос к провайдеру: модель={model_name}, ar={ar_x}:{ar_y}")
            else:
                write_log_entry(log_id, f"[{model_name}] попытка {cnt + 1}/{max_attempts_per_model}", level='warn')

            allowed = m.get('allowed_durations') or [0]
            actual_duration = nearest_allowed_duration(video_duration, allowed)
            if actual_duration != video_duration:
                write_log_entry(log_id, f"Длительность скорректирована: {video_duration}с → {actual_duration}с (модель поддерживает: {allowed})", level='warn')

            sr = client.submit(
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

            db_save_video_job_and_set_pending(batch_id, {
                'request_id':   req_id,
                'status_url':   s_url,
                'response_url': r_url,
            })
            write_log_entry(log_id, fmt_id_msg("Запрос принят ({}): {}", model_name, req_id))
            write_log_entry(log_id, fmt_id_msg("[video] Генерация запущена: request_id={}", req_id))

            video_url, poll_err = client.poll(log_id, s_url, r_url)
            if video_url:
                used_model    = model_name
                used_model_id = m_id
                return video_url

            write_log_entry(log_id, f"Поллинг не дал результата, сброс handshake", level='warn')
            db_save_video_job_and_set_pending(batch_id, {
                'request_id':   None,
                'status_url':   None,
                'response_url': None,
            })
            return None

        # Подхват при переборе моделей (slot/adhoc, not pinned):
        # current_movie_model_id в batches.data хранит модель, на которой
        # остановились. При подхвате первый проход начинается с неё, а не с
        # начала списка — чтобы не повторять уже провалившиеся модели.
        # Второй и последующие проходы идут по полному списку с самого начала:
        # передаём полный models в iterate_models, но _orig_video_callback
        # молча пропускает (None) модели до resume_model_id без инкремента
        # attempt_counts, так что счётчики попыток этих моделей остаются чистыми
        # для следующих проходов.
        resume_model_id = (
            (batch_data.get('current_movie_model_id') if isinstance(batch_data, dict) else None)
            if resumed and not pinned_model_id else None
        )
        resume_unlocked = not resume_model_id
        if resume_model_id:
            idx = next((i for i, m in enumerate(models) if str(m['id']) == resume_model_id), None)
            if idx is not None and idx > 0:
                write_log_entry(log_id, fmt_id_msg("[video] Подхват: первый проход начинается с модели {} (позиция {} из {})", resume_model_id, idx + 1, len(models)))
            else:
                resume_unlocked = True

        def _orig_video_callback(m):
            nonlocal resume_unlocked
            if not resume_unlocked:
                if str(m['id']) == resume_model_id:
                    resume_unlocked = True
                else:
                    return None
            return video_callback(m)

        cb = _orig_video_callback if resume_model_id else video_callback
        # pinned: один проход (модель задана жёстко, повторять нет смысла).
        # перебор: max_model_passes проходов по всему списку.
        max_passes = 1 if pinned_model_id else snap.max_model_passes
        video_url = iterate_models(models, max_attempts_per_model, cb, max_passes=max_passes)

        if not video_url:
            if is_probe:
                msg = f'Пробная модель {models[0]["name"]} не дала результата ({max_attempts_per_model} попыток)'
                db_log_update(log_id, msg, 'error')
                write_log_entry(log_id, msg, level='error')
                write_log_entry(log_id, f"[video] {msg}")
                raise AppException(batch_id, 'video', msg, log_id)
            else:
                msg = f'Все активные видео-модели не дали результата после {max_passes} проходов'
                db_log_update(log_id, msg, 'error')
                write_log_entry(log_id, msg, level='error')
                write_log_entry(log_id, f"[video] {msg}")
                db_set_batch_status(batch_id, 'story_ready')
                return

        write_log_entry(log_id, f"Скачиваю оригинал: {video_url}")
        write_log_entry(log_id, f"[video] Скачиваю оригинал от провайдера…")
        try:
            original_data = falai.download_video(log_id, video_url)
            orig_mb = round(len(original_data) / 1024 / 1024, 1)
            db_create_batch_movie(batch_id, original_data, video_url, used_model_id)
            write_log_entry(log_id, f"Оригинал сохранён ({orig_mb} МБ)")
            write_log_entry(log_id, f"[video] Оригинал сохранён в БД ({orig_mb} МБ)")
        except Exception as e:
            msg = f"Ошибка скачивания оригинала: {e}"
            db_log_update(log_id, msg, 'error')
            write_log_entry(log_id, msg, level='error')
            write_log_entry(log_id, f"[video] {msg}")
            raise AppException(batch_id, 'video', msg, log_id)

        db_set_batch_status(batch_id, 'video_ready')
        msg = f'Видео сгенерировано ({used_model})' if used_model else 'Видео сгенерировано'
        db_log_update(log_id, msg, 'ok')
        write_log_entry(log_id, f"URL: {video_url}")
        write_log_entry(log_id, f"[video] Готово: batch → video_ready, url={video_url[:60]}…")

    except AspectRatioConflictError as e:
        msg = str(e)
        db_log_update(log_id, msg, 'error')
        write_log_entry(log_id, f"[video] {msg}")
        raise AppException(batch_id, 'video', msg, log_id)
    except ProviderFatalError as e:
        msg = f"Фатальная ошибка провайдера: {e}"
        db_log_update(log_id, msg, 'error')
        write_log_entry(log_id, f"[video] {msg}")
        raise AppException(batch_id, 'video', msg, log_id)
