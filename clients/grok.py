"""
xAI Grok API client.
Отвечает за HTTP-взаимодействие с xAI API: submit-запрос и poll-цикл видео.
Интерфейс совместим с clients.falai (submit / poll / download_video).
"""

import os
import time
import requests

from log import write_log_entry
from clients.falai import ProviderFatalError, build_body


_XAI_KEY = os.environ.get('XAI_API_KEY', '')

_POLL_INTERVAL = 10
_POLL_MAX      = 240  # 40 минут максимум


def is_configured() -> bool:
    """Возвращает True если API-ключ задан."""
    return bool(_XAI_KEY)


def _headers():
    return {
        'Authorization': f'Bearer {_XAI_KEY}',
        'Content-Type': 'application/json',
    }


def submit(log_id, model_name: str, submit_url: str, platform_url: str,
           body_tpl: dict, prompt: str, ar_x: int, ar_y: int,
           video_duration: int):
    """
    Отправляет задание на генерацию видео в xAI Grok (одна попытка).

    Возвращает словарь {'request_id', 'status_url', 'response_url'} при успехе или None.
    Бросает ProviderFatalError при фатальной ошибке провайдера (401/403).
    """
    try:
        body = build_body(body_tpl, prompt, ar_x, ar_y, video_duration, log_id)
        resp = requests.post(submit_url, headers=_headers(), json=body, timeout=30)
    except requests.exceptions.Timeout:
        write_log_entry(log_id, f"[{model_name}] таймаут (30 с)", level='warn')
        return None
    except requests.exceptions.RequestException as e:
        write_log_entry(log_id, f"[{model_name}] ошибка соединения: {e}", level='warn')
        return None

    try:
        data = resp.json()
    except ValueError:
        write_log_entry(log_id, f"[{model_name}] не-JSON (HTTP {resp.status_code}): {resp.text}", level='warn')
        return None

    if resp.status_code >= 400:
        err = data.get('error', data)
        if resp.status_code in (401, 403):
            msg = f"[{model_name}] Фатальная ошибка провайдера (HTTP {resp.status_code}): {err}"
            write_log_entry(log_id, msg, level='error')
            raise ProviderFatalError(msg)
        write_log_entry(log_id, f"[{model_name}] HTTP {resp.status_code}: {err}", level='warn')
        return None

    request_id = data.get('request_id') or data.get('id')
    if not request_id:
        write_log_entry(log_id, f"[{model_name}] нет request_id в ответе: {str(data)}", level='warn')
        return None

    status_url   = f"{platform_url}/videos/{request_id}"
    response_url = status_url
    return {
        'request_id':   request_id,
        'status_url':   status_url,
        'response_url': response_url,
    }


def poll(log_id, status_url: str, response_url: str):
    """
    Поллит xAI API до завершения генерации.
    Возвращает (video_url, None) при успехе или (None, error_msg) при сбое.
    """
    for attempt in range(_POLL_MAX):
        time.sleep(_POLL_INTERVAL)
        try:
            s = requests.get(status_url, headers=_headers(), timeout=15).json()
            status = s.get('status')
            write_log_entry(log_id, f"Статус [{attempt + 1}]: {status}")

            if status in ('completed', 'done'):
                video_url = (s.get('video') or {}).get('url')
                if not video_url:
                    msg = f'Нет URL видео в ответе xAI: {str(s)}'
                    write_log_entry(log_id, msg, level='error')
                    return None, msg
                return video_url, None

            elif status in ('failed', 'error'):
                msg = f'xAI Grok: генерация провалилась: {str(s)}'
                write_log_entry(log_id, msg, level='error')
                return None, msg

        except Exception as e:
            write_log_entry(log_id, f"Ошибка опроса статуса: {e}", level='warn')

    msg = 'Таймаут генерации видео (40 минут)'
    write_log_entry(log_id, msg, level='error')
    return None, msg
