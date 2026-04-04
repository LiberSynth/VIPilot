"""
Pipeline 3 — Генерация видео.

Берёт первый батч story_ready (или video_pending при рестарте),
отправляет промпт в fal.ai, поллит статус и сохраняет video_url в batches.

Статусы батча:
  story_ready → video_pending → video_ready
                             → video_error
"""

import os
import time
import requests

from utils.notify import notify_failure

from db import (
    db_set_batch_video_model,
    db_get,
    env_get,
    db_get_story_ready_batch_atomic,
    db_get_video_pending_batch,
    db_is_batch_scheduled,
    db_set_batch_obsolete,
    db_get_story_text,
    db_get_active_video_models,
    db_get_video_model_by_id,
    db_set_batch_video_pending,
    db_set_batch_video_ready,
    db_set_batch_video_error,
    db_set_batch_story_ready_from_error,
    db_reset_video_generating,
    db_set_batch_original_video,
    db_get_random_real_original_video,
)
from log import db_log_pipeline, db_log_entry, db_log_update, db_log_interrupt_running

_FAL_KEY      = os.environ.get('FAL_API_KEY', '')


class ProviderFatalError(Exception):
    """Raised when a provider returns a permanent, non-retryable billing/account error."""


# ---------------------------------------------------------------------------
# Per-provider fatal-error detectors
# Each function receives (status_code: int, response_body: dict | str) and
# returns True if the response represents a fatal billing/account error.
# Add a new function here to support additional providers.
# ---------------------------------------------------------------------------

def _is_fatal_fal(status_code: int, body) -> bool:
    """Detect fal.ai balance-exhausted / account-locked errors (HTTP 403)."""
    if status_code != 403:
        return False
    text = str(body).lower()
    return 'exhausted balance' in text or 'locked' in text


# Registry: list of detector functions to try in order.
_FATAL_DETECTORS = [
    _is_fatal_fal,
]


def _is_provider_fatal(status_code: int, body) -> bool:
    """Return True if any registered detector considers the error fatal."""
    return any(fn(status_code, body) for fn in _FATAL_DETECTORS)


_POLL_INTERVAL = 30   # секунд между опросами
_POLL_MAX      = 240  # максимум попыток (~2 часа)


def _headers():
    return {
        'Authorization': f'Key {_FAL_KEY}',
        'Content-Type': 'application/json',
    }


def _build_body(body_tpl, prompt, ar_x, ar_y, video_duration):
    """Заполняет шаблон тела запроса значениями из батча/настроек."""
    body = dict(body_tpl)
    if 'prompt' in body:
        body['prompt'] = str(body['prompt']).format(prompt)
    if 'duration' in body:
        if body['duration'] == '{:int}':
            body['duration'] = video_duration
        else:
            body['duration'] = str(body['duration']).format(video_duration)
    if 'aspect_ratio' in body:
        body['aspect_ratio'] = str(body['aspect_ratio']).format(ar_x, ar_y)
    # Поддержка width/height для моделей, принимающих размеры вместо aspect_ratio
    if body.get('width') == '{:width}' or body.get('height') == '{:height}':
        if ar_x <= ar_y:   # портрет или квадрат
            h = 1024
            w = round(1024 * ar_x / ar_y / 32) * 32
        else:              # альбомная
            w = 1024
            h = round(1024 * ar_y / ar_x / 32) * 32
        body['width']  = w
        body['height'] = h
    # num_frames: количество кадров из длительности (25 fps)
    if body.get('num_frames') == '{:frames}':
        body['num_frames'] = video_duration * 25
    return body


def _poll(log_id, status_url, response_url):
    """Поллит fal.ai до завершения.
    Возвращает (video_url, None) при успехе или (None, error_msg) при сбое.
    Не меняет статус батча — это обязанность вызывающего кода."""
    for attempt in range(_POLL_MAX):
        time.sleep(_POLL_INTERVAL)
        try:
            s = requests.get(
                status_url,
                headers={'Authorization': f'Key {_FAL_KEY}'},
                timeout=15,
            ).json()
            status = s.get('status')
            if log_id:
                db_log_entry(log_id, f"Статус [{attempt + 1}]: {status}")

            if status == 'COMPLETED':
                result = requests.get(
                    response_url,
                    headers={'Authorization': f'Key {_FAL_KEY}'},
                    timeout=15,
                ).json()
                detail = result.get('detail')
                if detail:
                    types = [d.get('type', '') for d in detail] if isinstance(detail, list) else []
                    if any('content_policy' in t for t in types):
                        msg = 'fal.ai отклонил промпт: нарушение политики контента'
                        if log_id:
                            db_log_entry(log_id, msg, level='error')
                        return None, msg
                video_url = result.get('video', {}).get('url')
                if not video_url:
                    msg = f'Нет URL видео в ответе fal.ai: {str(result)}'
                    if log_id:
                        db_log_entry(log_id, msg, level='error')
                    return None, msg
                return video_url, None

            elif status == 'FAILED':
                msg = f'fal.ai: генерация провалилась: {str(s)}'
                if log_id:
                    db_log_entry(log_id, msg, level='error')
                return None, msg

        except Exception as e:
            if log_id:
                db_log_entry(log_id, f"Ошибка опроса статуса: {e}", level='warn')

    msg = 'Таймаут генерации видео (2 часа)'
    if log_id:
        db_log_entry(log_id, msg, level='error')
    return None, msg


def run():
    batch_id = None
    log_id = None
    try:
        db_log_interrupt_running('video')

        # Приоритет: возобновить прерванную генерацию (video_pending с request_id)
        batch   = db_get_video_pending_batch()
        resumed = batch is not None
        if not batch:
            # Атомарный захват: batch сразу переводится в video_generating,
            # поэтому следующий поток его не возьмёт.
            batch = db_get_story_ready_batch_atomic()
        if not batch:
            return

        batch_id  = str(batch['id'])
        target    = batch['target_name'] or 'пробный'
        ar_x      = batch['aspect_ratio_x'] or 9
        ar_y      = batch['aspect_ratio_y'] or 16
        story_id  = str(batch['story_id'])
        is_probe  = batch['target_id'] is None
        pinned_model_id = batch.get('video_model_id')

        try:
            video_duration = max(1, min(60, int(db_get('video_duration', '6'))))
        except (ValueError, TypeError):
            video_duration = 6

        if not is_probe and not db_is_batch_scheduled(batch['scheduled_at'], batch['target_id']):
            db_set_batch_obsolete(batch_id)
            db_log_pipeline('video', 'Батч отменён — слот удалён из расписания или таргет отключён',
                            status='прервана', batch_id=batch_id)
            print(f"[video] Батч {batch_id[:8]}… отменён, пропускаю")
            return

        # ── Режим эмуляции ──────────────────────────────────────────────────
        if env_get("emulation_mode", "0") == "1":
            print(f"[video] Батч {batch_id[:8]}… — эмуляция генерации видео")
            log_id = db_log_pipeline(
                'video', 'Видео [эмуляция]',
                status='running', batch_id=batch_id,
            )
            sample = db_get_random_real_original_video()
            if sample is None:
                msg = '[эмуляция] Нет видео в пуле — невозможно скопировать оригинал'
                db_log_update(log_id, msg, 'error')
                if log_id:
                    db_log_entry(log_id, msg, level='error')
                db_set_batch_video_error(batch_id)
                print(f"[video] {msg}")
                return
            db_set_batch_original_video(batch_id, sample)
            if log_id:
                db_log_entry(log_id, '[эмуляция] Оригинал скопирован из случайного донора')
                db_log_entry(log_id, '[эмуляция] Батч переведён в video_ready')
            db_set_batch_video_ready(batch_id, 'emulation://skipped')
            db_log_update(log_id, 'Видео [эмуляция]', 'ok')
            return

        print(f"[video] Батч {batch_id[:8]}… ({target}) — "
              f"{'возобновление' if resumed else 'начало'} генерации видео")

        used_model    = None
        used_model_id = None

        # --- Возобновление ---
        if resumed:
            request_id   = batch['data']['request_id']
            status_url   = batch['data']['status_url']
            response_url = batch['data']['response_url']
            used_model   = batch['data'].get('model_name')
            log_id = db_log_pipeline(
                'video', 'Генерация видео… (возобновление)',
                status='running', batch_id=batch_id,
            )
            if log_id:
                db_log_entry(log_id, f"Возобновление: request_id={request_id}")

        # --- Новая генерация ---
        else:
            if not _FAL_KEY:
                msg = 'FAL_API_KEY не задан — генерация невозможна'
                db_log_pipeline('video', msg, status='error', batch_id=batch_id)
                print(f"[video] {msg}")
                notify_failure(f"video: {msg}")
                return

            # --- Выбор модели(ей) ---
            if pinned_model_id:
                # Probe-режим: используем только зафиксированную модель, одна попытка
                pinned_m = db_get_video_model_by_id(pinned_model_id)
                if not pinned_m:
                    msg = f'Пробная модель {str(pinned_model_id)[:8]}… не найдена'
                    db_log_pipeline('video', msg, status='error', batch_id=batch_id)
                    print(f"[video] {msg}")
                    return
                models = [pinned_m]
                try:
                    fails_to_next = max(1, int(db_get('video_fails_to_next', '3')))
                except (ValueError, TypeError):
                    fails_to_next = 3
            else:
                models = db_get_active_video_models()
                if not models:
                    msg = 'Нет активных video-моделей в ai_models (type=text-to-video)'
                    db_log_pipeline('video', msg, status='error', batch_id=batch_id)
                    print(f"[video] {msg}")
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
                print(f"[video] {msg}")
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
            if log_id:
                db_log_entry(log_id, f"Соотношение сторон: {ar_x}:{ar_y}")
                db_log_entry(log_id, f"Промпт:\n{story_text}")
                if pinned_model_id:
                    db_log_entry(log_id, f"Пробная модель: {models[0]['name']}, попыток: {fails_to_next}")
                else:
                    db_log_entry(log_id, f"Моделей: {len(models)}, попыток на модель: {fails_to_next}")

            request_id   = None
            status_url   = None
            response_url = None

            for m in models:
                model_name   = m['name']
                submit_url   = m['submit_url']
                platform_url = m['platform_url']
                body_tpl     = m['body_tpl']

                if log_id:
                    db_log_entry(log_id, f"Модель: {model_name}")
                print(f"[video] Запрос к fal.ai: модель={model_name}, ar={ar_x}:{ar_y}")

                for attempt in range(fails_to_next):
                    try:
                        body = _build_body(body_tpl, story_text, ar_x, ar_y, video_duration)
                        resp = requests.post(submit_url, headers=_headers(), json=body, timeout=30)
                    except requests.exceptions.Timeout:
                        if log_id:
                            db_log_entry(log_id, f"[{model_name}] таймаут (30 с), попытка {attempt + 1}/{fails_to_next}", level='warn')
                        continue
                    except requests.exceptions.RequestException as e:
                        if log_id:
                            db_log_entry(log_id, f"[{model_name}] ошибка соединения: {e}", level='warn')
                        continue

                    try:
                        data = resp.json()
                    except ValueError:
                        if _is_provider_fatal(resp.status_code, resp.text):
                            msg = f"[{model_name}] Фатальная ошибка провайдера (HTTP {resp.status_code}): {resp.text}"
                            if log_id:
                                db_log_entry(log_id, msg, level='error')
                            raise ProviderFatalError(msg)
                        if log_id:
                            db_log_entry(log_id, f"[{model_name}] не-JSON (HTTP {resp.status_code}): {resp.text}", level='warn')
                        continue

                    if resp.status_code >= 400:
                        err = data.get('error', data)
                        if _is_provider_fatal(resp.status_code, err):
                            msg = f"[{model_name}] Фатальная ошибка провайдера (HTTP {resp.status_code}): {err}"
                            if log_id:
                                db_log_entry(log_id, msg, level='error')
                            raise ProviderFatalError(msg)
                        if log_id:
                            db_log_entry(log_id, f"[{model_name}] HTTP {resp.status_code}: {err}", level='warn')
                        continue

                    if 'request_id' not in data:
                        if log_id:
                            db_log_entry(log_id, f"[{model_name}] нет request_id: {str(data)}", level='warn')
                        continue

                    request_id   = data['request_id']
                    status_url   = data.get('status_url',
                                            f"{platform_url}/requests/{request_id}/status")
                    response_url = data.get('response_url',
                                            f"{platform_url}/requests/{request_id}")
                    used_model    = model_name
                    used_model_id = m['id']
                    break

                if request_id:
                    break

            if not request_id:
                if pinned_model_id:
                    msg = f'Пробная модель {models[0]["name"]} исчерпала все попытки ({fails_to_next})'
                    if log_id:
                        db_log_update(log_id, msg, 'error')
                        db_log_entry(log_id, msg, level='error')
                    db_set_batch_video_error(batch_id)
                    print(f"[video] {msg}")
                    return
                else:
                    msg = 'Все активные видео-модели не приняли запрос — повтор с первой модели'
                    if log_id:
                        db_log_update(log_id, msg, 'warn')
                        db_log_entry(log_id, msg, level='warn')
                    print(f"[video] {msg}")
                    return

            db_set_batch_video_pending(batch_id, {
                'request_id':   request_id,
                'status_url':   status_url,
                'response_url': response_url,
                'model_name':   used_model,
            })
            if log_id:
                db_log_entry(log_id, f"Запрос принят ({used_model}): {request_id}")
            print(f"[video] Генерация запущена: request_id={request_id}")

        # --- Поллинг ---
        video_url, poll_err = _poll(log_id, status_url, response_url)
        if not video_url:
            if poll_err:
                if log_id:
                    db_log_update(log_id, poll_err, 'error')
                notify_failure(f"video: {poll_err} (батч {batch_id[:8]})")
            if is_probe:
                # Проба: фиксируем ошибку, сюжет сохраняется
                db_set_batch_video_error(batch_id)
            else:
                # Обычный батч: откатываем в story_ready, сохраняя story_id
                # (сюжет НЕ регенерируется, видео попробуем снова на следующем цикле)
                db_set_batch_story_ready_from_error(batch_id)
            return

        # --- Скачиваем и сохраняем оригинал ---
        if log_id:
            db_log_entry(log_id, f"Скачиваю оригинал: {video_url}")
        print(f"[video] Скачиваю оригинал с fal.ai…")
        try:
            r = requests.get(video_url, timeout=120, stream=True)
            r.raise_for_status()
            original_data = b''.join(r.iter_content(chunk_size=256 * 1024))
            orig_mb = round(len(original_data) / 1024 / 1024, 1)
            db_set_batch_original_video(batch_id, original_data)
            if log_id:
                db_log_entry(log_id, f"Оригинал сохранён ({orig_mb} МБ)")
            print(f"[video] Оригинал сохранён в БД ({orig_mb} МБ)")
        except Exception as e:
            msg = f"Ошибка скачивания оригинала: {e}"
            db_log_update(log_id, msg, 'error')
            if log_id:
                db_log_entry(log_id, msg, level='error')
            db_set_batch_video_error(batch_id)
            print(f"[video] {msg}")
            notify_failure(f"video: {msg} (батч {batch_id[:8]})")
            return

        db_set_batch_video_ready(batch_id, video_url)
        if used_model_id:
            db_set_batch_video_model(batch_id, used_model_id)
        msg = f'Видео сгенерировано ({used_model})' if used_model else 'Видео сгенерировано'
        db_log_update(log_id, msg, 'ok')
        if log_id:
            db_log_entry(log_id, f"URL: {video_url}")
        print(f"[video] Готово: batch → video_ready, url={video_url[:60]}…")

    except ProviderFatalError as e:
        msg = str(e)
        db_log_pipeline('video', msg, status='error', batch_id=batch_id)
        if log_id:
            db_log_update(log_id, msg, 'error')
        if batch_id:
            db_set_batch_video_error(batch_id)
        print(f"[video] Фатальная ошибка провайдера: {msg}")
        notify_failure(f"video: фатальная ошибка провайдера — {msg}")
    except Exception as e:
        db_log_pipeline('video', f"Сбой пайплайна: {e}", status='error',
                        batch_id=batch_id)
        print(f"[video] Ошибка: {e}")
        notify_failure(f"сбой video-пайплайна: {e}")
    finally:
        # Если батч был захвачен (video_generating) но не переведён в
        # video_pending/video_ready/отменён/pending — возвращаем в story_ready.
        # Безопасно: UPDATE сработает только если статус всё ещё video_generating.
        if batch_id:
            db_reset_video_generating(batch_id)
