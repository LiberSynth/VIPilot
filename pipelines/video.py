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

from utils.notify import notify_failure

from db import (
    db_set_batch_video_model,
    db_get,
    env_get,
    db_get_batch_by_id,
    db_get_active_targets,
    db_set_batch_video_generating_by_id,
    db_get_story_text,
    db_get_active_video_models,
    db_get_video_model_by_id,
    db_set_batch_video_pending,
    db_set_batch_video_ready,
    db_set_batch_video_error,
    db_set_batch_story_ready_from_error,
    db_set_batch_original_video,
    db_get_random_real_original_video,
    db_set_batch_story_id,
    db_get_movie_from_donor,
)
from log import db_log_pipeline, db_log_update
from pipelines.base import check_cancelled, handle_critical_error, pipeline_log
from clients import falai
from clients.falai import ProviderFatalError


class AspectRatioConflictError(Exception):
    """Raised when active targets have different aspect ratios."""


def run(batch_id):
    log_id = None
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
                    pipeline_log(None, f"[video] Батч {batch_id[:8]}… — донор, movie_id уже перенесён (возобновление)")
                    return
                if not check_cancelled('video', batch_id, batch):
                    pipeline_log(None, f"[video] Батч {batch_id[:8]}… — режим донора, переносим видео от {donor_batch_id[:8]}…")
                    log_id = db_log_pipeline(
                        'video', 'Видео получено от донора',
                        status='running', batch_id=batch_id,
                    )
                    result_donor_id = db_get_movie_from_donor(donor_batch_id, batch_id)
                    if result_donor_id:
                        updated = db_get_batch_by_id(batch_id)
                        new_status = updated['status'] if updated else None
                        detail = f"Видео перенесено от донора {donor_batch_id[:8]}…"
                        db_log_update(log_id, detail, 'ok')
                        pipeline_log(log_id, detail)
                        if new_status == 'transcode_ready':
                            tr_log_id = db_log_pipeline(
                                'transcode', 'Транскодирование пропущено — видео получено от донора',
                                status='ok', batch_id=batch_id,
                            )
                            pipeline_log(tr_log_id, detail)
                        pipeline_log(None, f"[video] Батч {batch_id[:8]}… — видео от донора, новый статус: {new_status}")
                    else:
                        msg = f"Не удалось перенести видео от донора {donor_batch_id[:8]}…"
                        db_log_update(log_id, msg, 'error')
                        pipeline_log(log_id, msg, level='error')
                        db_set_batch_video_error(batch_id)
                        pipeline_log(None, f"[video] {msg}")
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

        if not is_probe and check_cancelled('video', batch_id, batch):
            return

        if env_get("emulation_mode", "0") == "1":
            pipeline_log(None, f"[video] Батч {batch_id[:8]}… — эмуляция генерации видео")
            log_id = db_log_pipeline(
                'video', 'Видео [эмуляция]',
                status='running', batch_id=batch_id,
            )
            result = db_get_random_real_original_video()
            if result is None:
                msg = '[эмуляция] Нет видео в пуле — невозможно скопировать оригинал'
                db_log_update(log_id, msg, 'error')
                pipeline_log(log_id, msg, level='error')
                db_set_batch_video_error(batch_id)
                pipeline_log(None, f"[video] {msg}")
                return
            sample, donor_batch_id, donor_story_id = result
            pipeline_log(log_id, f"Видео заимствовано из батча: {donor_batch_id}", level='info')
            db_set_batch_original_video(batch_id, sample)
            if donor_story_id:
                db_set_batch_story_id(batch_id, donor_story_id)
            db_set_batch_video_ready(batch_id, 'emulation://skipped')
            db_log_update(log_id, 'Видео [эмуляция]', 'ok')
            return

        pipeline_log(None, f"[video] Батч {batch_id[:8]}… ({target}) — "
                     f"{'возобновление' if resumed else 'начало'} генерации видео")

        used_model    = None
        used_model_id = None

        if resumed:
            batch_data   = batch['data'] or {}
            request_id   = batch_data['request_id']
            status_url   = batch_data['status_url']
            response_url = batch_data['response_url']
            used_model   = batch_data.get('model_name')
            log_id = db_log_pipeline(
                'video', 'Генерация видео… (возобновление)',
                status='running', batch_id=batch_id,
            )
            pipeline_log(log_id, f"Возобновление: request_id={request_id}")

        else:
            if not falai.is_configured():
                msg = 'FAL_API_KEY не задан — генерация невозможна'
                db_log_pipeline('video', msg, status='error', batch_id=batch_id)
                pipeline_log(None, f"[video] {msg}")
                notify_failure(f"video: {msg}")
                return

            if pinned_model_id:
                pinned_m = db_get_video_model_by_id(pinned_model_id)
                if not pinned_m:
                    msg = f'Пробная модель {str(pinned_model_id)[:8]}… не найдена'
                    db_log_pipeline('video', msg, status='error', batch_id=batch_id)
                    pipeline_log(None, f"[video] {msg}")
                    return
                models = [pinned_m]
                fails_to_next = 1
            else:
                models = db_get_active_video_models()
                if not models:
                    msg = 'Нет активных video-моделей в ai_models (type=text-to-video)'
                    db_log_pipeline('video', msg, status='error', batch_id=batch_id)
                    pipeline_log(None, f"[video] {msg}")
                    notify_failure(f"video: {msg}")
                    return
                try:
                    fails_to_next = max(1, int(db_get('video_fails_to_next', '3')))
                except (ValueError, TypeError):
                    fails_to_next = 3

            story_text = db_get_story_text(story_id)
            if not story_text:
                msg = f'Не найден сюжет story_id={story_id[:8]}…'
                db_log_pipeline('video', msg, status='error', batch_id=batch_id)
                pipeline_log(None, f"[video] {msg}")
                notify_failure(f"video: {msg} (батч {batch_id[:8]})")
                return

            video_post_prompt = db_get('video_post_prompt', '').strip()
            if video_post_prompt:
                video_post_prompt = video_post_prompt.replace('{продолжительность}', str(video_duration))
                story_text = story_text + '\n\n' + video_post_prompt

            log_id = db_log_pipeline(
                'video', 'Генерация видео…',
                status='running', batch_id=batch_id,
            )
            pipeline_log(log_id, f"Соотношение сторон: {ar_x}:{ar_y}")
            if pinned_model_id:
                pipeline_log(log_id, f"Пробная модель: {models[0]['name']}, попыток: {fails_to_next}")
            else:
                pipeline_log(log_id, f"Моделей: {len(models)}, попыток на модель: {fails_to_next}")

            request_id   = None
            status_url   = None
            response_url = None

            max_passes = 5
            for pass_num in range(max_passes):
                for m in models:
                    model_name   = m['name']
                    submit_url   = m['submit_url']
                    platform_url = m['platform_url']
                    body_tpl     = m['body_tpl']

                    pipeline_log(log_id, f"Модель: {model_name}")
                    pipeline_log(None, f"[video] Запрос к провайдеру: модель={model_name}, ar={ar_x}:{ar_y}")

                    submit_result = None
                    for attempt in range(fails_to_next):
                        submit_result = falai.submit(
                            log_id, model_name, submit_url, platform_url,
                            body_tpl, story_text, ar_x, ar_y, video_duration,
                        )
                        if submit_result:
                            break
                        pipeline_log(log_id, f"[{model_name}] неудачная попытка {attempt + 1}/{fails_to_next}", level='warn')

                    if submit_result:
                        request_id   = submit_result['request_id']
                        status_url   = submit_result['status_url']
                        response_url = submit_result['response_url']
                        used_model    = model_name
                        used_model_id = m['id']
                        break

                if request_id:
                    break

                if pinned_model_id:
                    msg = f'Пробная модель {models[0]["name"]} исчерпала все попытки ({fails_to_next})'
                    db_log_update(log_id, msg, 'error')
                    pipeline_log(log_id, msg, level='error')
                    db_set_batch_video_error(batch_id)
                    pipeline_log(None, f"[video] {msg}")
                    return

                if pass_num < max_passes - 1:
                    msg = f'Все активные видео-модели не приняли запрос — повтор с первой модели (проход {pass_num + 1}/{max_passes})'
                    db_log_update(log_id, msg, 'warn')
                    pipeline_log(log_id, msg, level='warn')
                    pipeline_log(None, f"[video] {msg}")
                else:
                    msg = f'Все активные видео-модели не приняли запрос после {max_passes} проходов'
                    db_log_update(log_id, msg, 'error')
                    pipeline_log(log_id, msg, level='error')
                    pipeline_log(None, f"[video] {msg}")
                    db_set_batch_video_error(batch_id)
                    return

            db_set_batch_video_pending(batch_id, {
                'request_id':   request_id,
                'status_url':   status_url,
                'response_url': response_url,
                'model_name':   used_model,
            })
            pipeline_log(log_id, f"Запрос принят ({used_model}): {request_id}")
            pipeline_log(None, f"[video] Генерация запущена: request_id={request_id}")

        video_url, poll_err = falai.poll(log_id, status_url, response_url)
        if not video_url:
            if poll_err:
                db_log_update(log_id, poll_err, 'error')
                notify_failure(f"video: {poll_err} (батч {batch_id[:8]})")
            if is_probe:
                db_set_batch_video_error(batch_id)
            else:
                db_set_batch_story_ready_from_error(batch_id)
            return

        pipeline_log(log_id, f"Скачиваю оригинал: {video_url}")
        pipeline_log(None, f"[video] Скачиваю оригинал от провайдера…")
        try:
            original_data = falai.download_video(log_id, video_url)
            orig_mb = round(len(original_data) / 1024 / 1024, 1)
            db_set_batch_original_video(batch_id, original_data)
            pipeline_log(log_id, f"Оригинал сохранён ({orig_mb} МБ)")
            pipeline_log(None, f"[video] Оригинал сохранён в БД ({orig_mb} МБ)")
        except Exception as e:
            msg = f"Ошибка скачивания оригинала: {e}"
            db_log_update(log_id, msg, 'error')
            pipeline_log(log_id, msg, level='error')
            db_set_batch_video_error(batch_id)
            pipeline_log(None, f"[video] {msg}")
            notify_failure(f"video: {msg} (батч {batch_id[:8]})")
            return

        db_set_batch_video_ready(batch_id, video_url)
        if used_model_id:
            db_set_batch_video_model(batch_id, used_model_id)
        msg = f'Видео сгенерировано ({used_model})' if used_model else 'Видео сгенерировано'
        db_log_update(log_id, msg, 'ok')
        pipeline_log(log_id, f"URL: {video_url}")
        pipeline_log(None, f"[video] Готово: batch → video_ready, url={video_url[:60]}…")

    except AspectRatioConflictError as e:
        msg = str(e)
        db_log_pipeline('video', msg, status='error', batch_id=batch_id)
        if log_id:
            db_log_update(log_id, msg, 'error')
        if batch_id:
            db_set_batch_video_error(batch_id)
        pipeline_log(None, f"[video] {msg}")
        notify_failure(f"video: {msg}")
    except ProviderFatalError as e:
        msg = str(e)
        db_log_pipeline('video', msg, status='error', batch_id=batch_id)
        if log_id:
            db_log_update(log_id, msg, 'error')
        if batch_id:
            db_set_batch_video_error(batch_id)
        pipeline_log(None, f"[video] Фатальная ошибка провайдера: {msg}")
        notify_failure(f"video: фатальная ошибка провайдера — {msg}")
    except Exception as e:
        handle_critical_error('video', batch_id, log_id, e)
