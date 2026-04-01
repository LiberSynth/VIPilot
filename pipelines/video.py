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

from db import (
    db_get,
    db_get_story_ready_batch,
    db_get_video_pending_batch,
    db_is_batch_scheduled,
    db_set_batch_obsolete,
    db_get_story_text,
    db_get_active_video_model,
    db_set_batch_video_pending,
    db_set_batch_video_ready,
    db_set_batch_video_error,
)
from log import db_log_pipeline, db_log_entry, db_log_update, db_log_interrupt_running

_FAL_KEY      = os.environ.get('FAL_API_KEY', '')
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
        if body['duration'] == '{int}':
            body['duration'] = video_duration
        else:
            body['duration'] = str(body['duration']).format(video_duration)
    if 'aspect_ratio' in body:
        body['aspect_ratio'] = str(body['aspect_ratio']).format(ar_x, ar_y)
    return body


def _poll(log_id, batch_id, status_url, response_url):
    """Поллит fal.ai до завершения. Возвращает video_url или None."""
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
                        db_log_update(log_id, msg, 'error')
                        if log_id:
                            db_log_entry(log_id, msg, level='error')
                        db_set_batch_video_error(batch_id)
                        return None
                video_url = result.get('video', {}).get('url')
                if not video_url:
                    msg = f'Нет URL видео в ответе fal.ai: {str(result)[:200]}'
                    db_log_update(log_id, msg, 'error')
                    if log_id:
                        db_log_entry(log_id, msg, level='error')
                    db_set_batch_video_error(batch_id)
                    return None
                return video_url

            elif status == 'FAILED':
                msg = f'fal.ai: генерация провалилась: {str(s)[:200]}'
                db_log_update(log_id, msg, 'error')
                if log_id:
                    db_log_entry(log_id, msg, level='error')
                db_set_batch_video_error(batch_id)
                return None

        except Exception as e:
            if log_id:
                db_log_entry(log_id, f"Ошибка опроса статуса: {e}", level='warn')

    msg = 'Таймаут генерации видео (2 часа)'
    db_log_update(log_id, msg, 'error')
    if log_id:
        db_log_entry(log_id, msg, level='error')
    db_set_batch_video_error(batch_id)
    return None


def run():
    try:
        db_log_interrupt_running('video')

        # Приоритет: возобновить прерванную генерацию (video_pending с request_id)
        batch   = db_get_video_pending_batch()
        resumed = batch is not None
        if not batch:
            batch = db_get_story_ready_batch()
        if not batch:
            return

        batch_id = str(batch['id'])
        target   = batch['target_name']
        ar_x     = batch['aspect_ratio_x']
        ar_y     = batch['aspect_ratio_y']
        story_id = str(batch['story_id'])

        try:
            video_duration = max(1, min(60, int(db_get('video_duration', '6'))))
        except (ValueError, TypeError):
            video_duration = 6

        if not db_is_batch_scheduled(batch['scheduled_at'], batch['target_id']):
            db_set_batch_obsolete(batch_id)
            db_log_pipeline('video', 'Батч устарел — слот удалён из расписания или таргет отключён',
                            status='прервана', batch_id=batch_id)
            print(f"[video] Батч {batch_id[:8]}… устарел, пропускаю")
            return

        print(f"[video] Батч {batch_id[:8]}… ({target}) — "
              f"{'возобновление' if resumed else 'начало'} генерации видео")

        # --- Возобновление ---
        if resumed:
            request_id   = batch['data']['request_id']
            status_url   = batch['data']['status_url']
            response_url = batch['data']['response_url']
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
                return

            submit_url, platform_url, body_tpl, model_name, _ = db_get_active_video_model()
            if not submit_url:
                msg = 'Нет активной video-модели в ai_models (type=text-to-video)'
                db_log_pipeline('video', msg, status='error', batch_id=batch_id)
                print(f"[video] {msg}")
                return

            story_text = db_get_story_text(story_id)
            if not story_text:
                msg = f'Не найден сюжет story_id={story_id[:8]}…'
                db_log_pipeline('video', msg, status='error', batch_id=batch_id)
                print(f"[video] {msg}")
                return

            log_id = db_log_pipeline(
                'video', 'Генерация видео…',
                status='running', batch_id=batch_id,
            )
            if log_id:
                db_log_entry(log_id, f"Модель: {model_name}")
                db_log_entry(log_id, f"Соотношение сторон: {ar_x}:{ar_y}")
                preview = story_text[:120] + ('…' if len(story_text) > 120 else '')
                db_log_entry(log_id, f"Промпт: {preview}")

            body = _build_body(body_tpl, story_text, ar_x, ar_y, video_duration)
            print(f"[video] Запрос к fal.ai: модель={model_name}, ar={ar_x}:{ar_y}")

            try:
                resp = requests.post(submit_url, headers=_headers(), json=body, timeout=30)
            except requests.exceptions.Timeout:
                msg = 'Таймаут отправки запроса к fal.ai (30 сек)'
                db_log_update(log_id, msg, 'error')
                if log_id:
                    db_log_entry(log_id, msg, level='error')
                print(f"[video] {msg}")
                return
            except requests.exceptions.RequestException as e:
                msg = f'Ошибка соединения с fal.ai: {e}'
                db_log_update(log_id, msg, 'error')
                if log_id:
                    db_log_entry(log_id, msg, level='error')
                print(f"[video] {msg}")
                return

            try:
                data = resp.json()
            except ValueError:
                body_preview = ' '.join(resp.text.split())[:200]
                msg = f'fal.ai вернул не-JSON (HTTP {resp.status_code}): {body_preview or "(пустое тело)"}'
                db_log_update(log_id, msg, 'error')
                if log_id:
                    db_log_entry(log_id, msg, level='error')
                print(f"[video] {msg}")
                return

            if resp.status_code >= 400:
                err = data.get('error', data)
                msg = f'fal.ai HTTP {resp.status_code}: {err}'
                db_log_update(log_id, msg, 'error')
                if log_id:
                    db_log_entry(log_id, msg, level='error')
                print(f"[video] {msg}")
                return

            if 'request_id' not in data:
                msg = f'fal.ai не вернул request_id: {str(data)[:300]}'
                db_log_update(log_id, msg, 'error')
                if log_id:
                    db_log_entry(log_id, msg, level='error')
                print(f"[video] {msg}")
                return

            request_id   = data['request_id']
            status_url   = data.get('status_url',
                                    f"{platform_url}/requests/{request_id}/status")
            response_url = data.get('response_url',
                                    f"{platform_url}/requests/{request_id}")

            db_set_batch_video_pending(batch_id, {
                'request_id':   request_id,
                'status_url':   status_url,
                'response_url': response_url,
            })
            if log_id:
                db_log_entry(log_id, f"Запрос принят: {request_id}")
            print(f"[video] Генерация запущена: request_id={request_id}")

        # --- Поллинг ---
        video_url = _poll(log_id, batch_id, status_url, response_url)
        if not video_url:
            return

        db_set_batch_video_ready(batch_id, video_url)
        msg = 'Видео сгенерировано'
        db_log_update(log_id, msg, 'ok')
        if log_id:
            db_log_entry(log_id, f"URL: {video_url[:80]}…")
        print(f"[video] Готово: batch → video_ready, url={video_url[:60]}…")

    except Exception as e:
        db_log_pipeline('video', f"Сбой пайплайна: {e}", status='error')
        print(f"[video] Ошибка: {e}")
