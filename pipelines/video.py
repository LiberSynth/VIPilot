"""
Pipeline 3 — Генерация видео.

Принимает batch_id, определяет нужно ли возобновление (video_pending)
или новая генерация (story_ready), отправляет промпт в fal.ai,
поллит статус и сохраняет video_url в batches.

Статусы батча:
  story_ready → video_generating → video_pending → video_ready
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
)
from log import db_log_pipeline, db_log_entry, db_log_update
from pipelines.base import check_cancelled, handle_critical_error

_FAL_KEY = os.environ.get('FAL_API_KEY', '')


class ProviderFatalError(Exception):
    """Raised when a provider returns a permanent, non-retryable billing/account error."""


def _is_fatal_fal(status_code: int, body) -> bool:
    """Detect fal.ai balance-exhausted / account-locked errors (HTTP 403)."""
    if status_code != 403:
        return False
    text = str(body).lower()
    return 'exhausted balance' in text or 'locked' in text


_FATAL_DETECTORS = [_is_fatal_fal]


def _is_provider_fatal(status_code: int, body) -> bool:
    return any(fn(status_code, body) for fn in _FATAL_DETECTORS)


_POLL_INTERVAL = 30
_POLL_MAX      = 240


def _headers():
    return {
        'Authorization': f'Key {_FAL_KEY}',
        'Content-Type': 'application/json',
    }


def _build_body(body_tpl, prompt, ar_x, ar_y, video_duration):
    body = dict(body_tpl)
    if 'prompt' in body:
        body['prompt'] = str(body['prompt']).format(prompt)
    if ar_x <= ar_y:
        calc_h = 1024
        calc_w = round(1024 * ar_x / ar_y / 32) * 32
    else:
        calc_w = 1024
        calc_h = round(1024 * ar_y / ar_x / 32) * 32

    _INT_VALUES = {
        'duration':   video_duration,
        'width':      calc_w,
        'height':     calc_h,
        'num_frames': video_duration * 25,
    }

    for field, val in list(body.items()):
        if val == '{:int}' and field in _INT_VALUES:
            body[field] = _INT_VALUES[field]
        elif isinstance(val, str) and val != '{:int}':
            if field == 'aspect_ratio':
                body[field] = val.format(ar_x, ar_y)
            elif field == 'prompt':
                pass
            else:
                try:
                    body[field] = val.format(video_duration)
                except (IndexError, KeyError):
                    pass

    return body


def _poll(log_id, status_url, response_url):
    """Поллит fal.ai до завершения.
    Возвращает (video_url, None) при успехе или (None, error_msg) при сбое."""
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

        is_probe        = batch['type'] == 'probe'
        if is_probe:
            target = 'пробный'
            ar_x   = 9
            ar_y   = 16
        else:
            active_targets = db_get_active_targets()
            tgt    = active_targets[0] if active_targets else {}
            target = tgt.get('name') or 'adhoc'
            ar_x   = tgt.get('aspect_ratio_x') or 9
            ar_y   = tgt.get('aspect_ratio_y') or 16
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
            print(f"[video] Батч {batch_id[:8]}… — эмуляция генерации видео")
            log_id = db_log_pipeline(
                'video', 'Видео [эмуляция]',
                status='running', batch_id=batch_id,
            )
            result = db_get_random_real_original_video()
            if result is None:
                msg = '[эмуляция] Нет видео в пуле — невозможно скопировать оригинал'
                db_log_update(log_id, msg, 'error')
                if log_id:
                    db_log_entry(log_id, msg, level='error')
                db_set_batch_video_error(batch_id)
                print(f"[video] {msg}")
                return
            sample, donor_batch_id, donor_story_id = result
            if log_id:
                db_log_entry(log_id, f"Видео заимствовано из батча: {donor_batch_id}", level='info')
            db_set_batch_original_video(batch_id, sample)
            if donor_story_id:
                db_set_batch_story_id(batch_id, donor_story_id)
            db_set_batch_video_ready(batch_id, 'emulation://skipped')
            db_log_update(log_id, 'Видео [эмуляция]', 'ok')
            return

        print(f"[video] Батч {batch_id[:8]}… ({target}) — "
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
            if log_id:
                db_log_entry(log_id, f"Возобновление: request_id={request_id}")

        else:
            if not _FAL_KEY:
                msg = 'FAL_API_KEY не задан — генерация невозможна'
                db_log_pipeline('video', msg, status='error', batch_id=batch_id)
                print(f"[video] {msg}")
                notify_failure(f"video: {msg}")
                return

            if pinned_model_id:
                pinned_m = db_get_video_model_by_id(pinned_model_id)
                if not pinned_m:
                    msg = f'Пробная модель {str(pinned_model_id)[:8]}… не найдена'
                    db_log_pipeline('video', msg, status='error', batch_id=batch_id)
                    print(f"[video] {msg}")
                    return
                models = [pinned_m]
                fails_to_next = 1
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

        video_url, poll_err = _poll(log_id, status_url, response_url)
        if not video_url:
            if poll_err:
                if log_id:
                    db_log_update(log_id, poll_err, 'error')
                notify_failure(f"video: {poll_err} (батч {batch_id[:8]})")
            if is_probe:
                db_set_batch_video_error(batch_id)
            else:
                db_set_batch_story_ready_from_error(batch_id)
            return

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
        handle_critical_error('video', batch_id, log_id, e)
