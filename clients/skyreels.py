"""
SkyReels API client.
Отвечает за HTTP-взаимодействие с SkyReels: submit-запрос, poll-цикл и скачивание видео.
API ключ передаётся в теле запроса (поле api_key), а не в заголовках.
"""

import os
import time
import json
import requests

from clients.falai import build_body, ProviderFatalError
from log import write_log_entry

_SKYREELS_KEY = os.environ.get('SKYREELS_API_KEY', '')

_POLL_INTERVAL = 15
_POLL_MAX      = 480  # 480 × 15 с = 2 часа

def _headers():
    return {'Content-Type': 'application/json'}

def _compact_json(value, limit: int = 1200) -> str:
    """Возвращает компактный JSON/строку для диагностики с ограничением размера."""
    try:
        text = json.dumps(value, ensure_ascii=False, separators=(',', ':'))
    except Exception:
        text = str(value)
    if len(text) <= limit:
        return text
    return text[:limit] + '...'

def submit(batch_id, category, model_name: str, submit_url: str, platform_url: str,
           body_tpl: dict, prompt: str, ar_x: int, ar_y: int,
           video_duration: int):
    """
    Отправляет задание на генерацию видео в SkyReels.

    Возвращает словарь {'request_id', 'status_url', 'response_url'} при успехе или None.
    Бросает ProviderFatalError при фатальной ошибке провайдера.
    """
    body = build_body(body_tpl, prompt, ar_x, ar_y, video_duration, batch_id, category)
    body['api_key'] = _SKYREELS_KEY

    try:
        resp = requests.post(submit_url, headers=_headers(), json=body, timeout=30)
    except requests.exceptions.Timeout:
        write_log_entry(batch_id, category, f"[{model_name}] таймаут (30 с)", level='warn')
        return None
    except requests.exceptions.RequestException as e:
        write_log_entry(batch_id, category, f"[{model_name}] ошибка соединения: {e}", level='warn')
        return None

    try:
        data = resp.json()
    except ValueError:
        write_log_entry(batch_id, category, f"[{model_name}] не-JSON (HTTP {resp.status_code}): {resp.text}", level='warn')
        return None

    if resp.status_code >= 400:
        if resp.status_code in (401, 403):
            msg = f"[{model_name}] Фатальная ошибка провайдера (HTTP {resp.status_code}): {data}"
            write_log_entry(batch_id, category, msg, level='error')
            raise ProviderFatalError(msg)
        write_log_entry(batch_id, category, f"[{model_name}] HTTP {resp.status_code}: {data}", level='warn')
        return None

    task_id = data.get('task_id') or data.get('request_id') or data.get('id')
    if not task_id:
        write_log_entry(batch_id, category, f"[{model_name}] нет task_id в ответе: {str(data)}", level='warn')
        return None

    base = platform_url.rstrip('/')
    status_url   = f"{base}/api/v1/video/task/{task_id}"
    response_url = f"{base}/api/v1/video/task/{task_id}"

    return {
        'request_id':   str(task_id),
        'status_url':   status_url,
        'response_url': response_url,
    }

def poll(batch_id, category, status_url: str, response_url: str):
    """
    Поллит SkyReels до завершения генерации.
    Возвращает (video_url, None) при успехе или (None, error_msg) при сбоке.
    """
    params = {'api_key': _SKYREELS_KEY}

    for attempt in range(_POLL_MAX):
        time.sleep(_POLL_INTERVAL)
        try:
            resp = requests.get(status_url, params=params, timeout=15)
            try:
                data = resp.json()
            except ValueError:
                write_log_entry(batch_id, category, f"SkyReels poll [{attempt + 1}] не-JSON (HTTP {resp.status_code})", level='warn')
                write_log_entry(
                    batch_id, category,
                    f"SkyReels poll [{attempt + 1}] http={resp.status_code}, body={_compact_json(resp.text)}",
                    level='silent',
                )
                write_log_entry(batch_id, category, f"Статус [{attempt + 1}]: None")
                continue

            status = (data.get('status') or '').upper()
            write_log_entry(batch_id, category, f"Статус [{attempt + 1}]: {status}")
            write_log_entry(
                batch_id, category,
                f"SkyReels poll [{attempt + 1}] http={resp.status_code}, status={status}, keys={list(data.keys()) if isinstance(data, dict) else []}",
                level='silent',
            )

            if status in ('SUCCEEDED', 'COMPLETED', 'SUCCESS'):
                video_url = (
                    data.get('video_url')
                    or data.get('output', {}).get('video_url')
                    or data.get('result', {}).get('video_url')
                )
                if not video_url:
                    msg = f"SkyReels: нет video_url в ответе: {str(data)}"
                    write_log_entry(batch_id, category, msg, level='error')
                    return None, msg
                return video_url, None

            if status in ('FAILED', 'ERROR'):
                msg = f"SkyReels: генерация провалилась: {str(data)}"
                write_log_entry(batch_id, category, msg, level='error')
                return None, msg

        except Exception as e:
            write_log_entry(batch_id, category, f"Ошибка опроса статуса: {e}", level='warn')
            write_log_entry(batch_id, category, f"SkyReels poll [{attempt + 1}] exception={type(e).__name__}: {e}", level='silent')

    msg = 'Таймаут генерации видео SkyReels (2 часа)'
    write_log_entry(batch_id, category, msg, level='error')
    return None, msg

def download_video(batch_id, category, video_url: str) -> bytes:
    """Скачивает видео по URL."""
    r = requests.get(video_url, timeout=120, stream=True)
    r.raise_for_status()
    return b''.join(r.iter_content(chunk_size=256 * 1024))
