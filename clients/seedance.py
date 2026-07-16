"""
Seedance 2.0 API client (https://api.seedance2.ai).
Отвечает за HTTP-взаимодействие: submit-запрос, poll-цикл и скачивание видео.
Интерфейс совместим с clients.falai (submit / poll).
"""

import copy
import os
import time
import json
import requests

from log import write_log_entry
from clients.falai import ProviderFatalError

_SEEDANCE_KEY = os.environ.get('SEEDANCE_API_KEY', '')

_POLL_INTERVAL = 10
_POLL_MAX      = 720  # 720 × 10 с = 2 часа

_FATAL_HTTP = (401, 402, 403)

def _headers():
    return {
        'Authorization': f'Bearer {_SEEDANCE_KEY}',
        'Content-Type': 'application/json',
    }

def _compact_json(value, limit: int = 1200) -> str:
    try:
        text = json.dumps(value, ensure_ascii=False, separators=(',', ':'))
    except Exception:
        text = str(value)
    if len(text) <= limit:
        return text
    return text[:limit] + '...'

def _is_fatal(status_code: int) -> bool:
    return status_code in _FATAL_HTTP

def _format_error(data) -> str:
    if isinstance(data, dict):
        err = data.get('error')
        if isinstance(err, dict):
            code = err.get('code', '')
            message = err.get('message', '')
            if code or message:
                return f'{code}: {message}'.strip(': ')
        return str(data)
    return str(data)

def build_body(body_tpl, prompt, ar_x, ar_y, video_duration, batch_id=None, category=None):
    """Строит тело запроса Seedance из шаблона модели и параметров пайплайна."""
    body = copy.deepcopy(body_tpl)
    inp = body.setdefault('input', {})
    if not isinstance(inp, dict):
        inp = {}
        body['input'] = inp
    inp['prompt'] = prompt
    inp['duration'] = video_duration
    inp['aspect_ratio'] = f'{ar_x}:{ar_y}'
    return body

def submit(batch_id, category, model_name: str, submit_url: str, platform_url: str,
           body_tpl: dict, prompt: str, ar_x: int, ar_y: int,
           video_duration: int):
    """
    Отправляет задание на генерацию видео в Seedance 2.0.

    Возвращает словарь {'request_id', 'status_url', 'response_url'} при успехе или None.
    Бросает ProviderFatalError при фатальной ошибке провайдера.
    """
    body = build_body(body_tpl, prompt, ar_x, ar_y, video_duration, batch_id, category)

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
        if _is_fatal(resp.status_code):
            msg = f"[{model_name}] Фатальная ошибка провайдера (HTTP {resp.status_code}): {resp.text}"
            write_log_entry(batch_id, category, msg, level='error')
            raise ProviderFatalError(msg)
        write_log_entry(batch_id, category, f"[{model_name}] не-JSON (HTTP {resp.status_code}): {resp.text}", level='warn')
        return None

    if resp.status_code >= 400:
        err_text = _format_error(data)
        if _is_fatal(resp.status_code):
            msg = f"[{model_name}] Фатальная ошибка провайдера (HTTP {resp.status_code}): {err_text}"
            write_log_entry(batch_id, category, msg, level='error')
            raise ProviderFatalError(msg)
        write_log_entry(batch_id, category, f"[{model_name}] HTTP {resp.status_code}: {err_text}", level='warn')
        return None

    task_id = data.get('taskId') or data.get('task_id') or data.get('id')
    if not task_id:
        write_log_entry(batch_id, category, f"[{model_name}] нет taskId в ответе: {str(data)}", level='warn')
        return None

    base = platform_url.rstrip('/')
    status_url = f"{base}/v1/tasks/{task_id}"
    return {
        'request_id':   str(task_id),
        'status_url':   status_url,
        'response_url': status_url,
    }

def poll(batch_id, category, status_url: str, response_url: str):
    """
    Поллит Seedance до завершения генерации.
    Возвращает (video_url, None) при успехе или (None, error_msg) при сбое.
    """
    for attempt in range(_POLL_MAX):
        time.sleep(_POLL_INTERVAL)
        try:
            resp = requests.get(status_url, headers=_headers(), timeout=15)
            try:
                data = resp.json()
            except ValueError:
                write_log_entry(
                    batch_id, category,
                    f"Seedance poll [{attempt + 1}] не-JSON (HTTP {resp.status_code})",
                    level='warn',
                )
                write_log_entry(
                    batch_id, category,
                    f"Seedance poll [{attempt + 1}] http={resp.status_code}, body={_compact_json(resp.text)}",
                    level='silent',
                )
                write_log_entry(batch_id, category, f"Статус [{attempt + 1}]: None")
                continue

            status = (data.get('status') or '').lower()
            write_log_entry(batch_id, category, f"Статус [{attempt + 1}]: {status}")
            write_log_entry(
                batch_id, category,
                f"Seedance poll [{attempt + 1}] http={resp.status_code}, status={status}, keys={list(data.keys()) if isinstance(data, dict) else []}",
                level='silent',
            )

            if resp.status_code >= 400:
                err_text = _format_error(data)
                err_lc = err_text.lower()
                if 'moderation' in err_lc or 'policy' in err_lc:
                    msg = f'Seedance: генерация отклонена модерацией контента: {err_text}'
                    write_log_entry(batch_id, category, msg, level='error')
                    return None, msg
                if _is_fatal(resp.status_code):
                    msg = f'Seedance: фатальная ошибка провайдера (HTTP {resp.status_code}): {err_text}'
                    write_log_entry(batch_id, category, msg, level='error')
                    return None, msg
                write_log_entry(batch_id, category, f"Seedance poll HTTP {resp.status_code}: {err_text}", level='warn')
                continue

            if status == 'completed':
                payload = data.get('data') if isinstance(data.get('data'), dict) else {}
                results = payload.get('results') or []
                video_url = results[0] if results else None
                if not video_url:
                    msg = f"Seedance: нет video URL в ответе: {str(data)}"
                    write_log_entry(batch_id, category, msg, level='error')
                    return None, msg
                return video_url, None

            if status == 'failed':
                reason = data.get('failed_reason') or (data.get('data') or {}).get('failed_reason') or str(data)
                msg = f'Seedance: генерация провалилась: {reason}'
                write_log_entry(batch_id, category, msg, level='error')
                return None, msg

        except Exception as e:
            write_log_entry(batch_id, category, f"Ошибка опроса статуса: {e}", level='warn')
            write_log_entry(
                batch_id, category,
                f"Seedance poll [{attempt + 1}] exception={type(e).__name__}: {e}",
                level='silent',
            )

    msg = 'Таймаут генерации видео Seedance (2 часа)'
    write_log_entry(batch_id, category, msg, level='error')
    return None, msg
