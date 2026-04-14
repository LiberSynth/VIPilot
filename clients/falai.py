"""
fal.ai API client.
Отвечает за HTTP-взаимодействие с fal.ai: submit-запрос, poll-цикл и скачивание видео.
Пайплайн импортирует только этот клиент, не зная о fal.ai напрямую.
"""

import os
import time
import requests

from log import write_log_entry


_FAL_KEY = os.environ.get('FAL_API_KEY', '')

_POLL_INTERVAL = 30
_POLL_MAX      = 240


class ProviderFatalError(Exception):
    """Raised when a provider returns a permanent, non-retryable billing/account error."""


def is_configured() -> bool:
    """Возвращает True если API-ключ задан."""
    return bool(_FAL_KEY)


def _headers():
    return {
        'Authorization': f'Key {_FAL_KEY}',
        'Content-Type': 'application/json',
    }


def _is_fatal_fal(status_code: int, body) -> bool:
    """Detect fal.ai balance-exhausted / account-locked errors (HTTP 403)."""
    if status_code != 403:
        return False
    text = str(body).lower()
    return 'exhausted balance' in text or 'locked' in text


_FATAL_DETECTORS = [_is_fatal_fal]


def _is_provider_fatal(status_code: int, body) -> bool:
    return any(fn(status_code, body) for fn in _FATAL_DETECTORS)


def build_body(body_tpl, prompt, ar_x, ar_y, video_duration, log_id=None):
    """
    Строит тело запроса к fal.ai из шаблона модели.

    :param log_id: идентификатор записи лога (опционально)
    """
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
                except (IndexError, KeyError) as e:
                    if log_id:
                        write_log_entry(
                            log_id,
                            f"Поле «{field}» шаблона не поддерживает подстановку video_duration — оставляем как есть ({e})",
                            level='warn',
                        )

    return body


def submit(log_id, model_name: str, submit_url: str, platform_url: str,
           body_tpl: dict, prompt: str, ar_x: int, ar_y: int,
           video_duration: int):
    """
    Отправляет задание на генерацию видео в fal.ai (одна попытка).

    Возвращает словарь {'request_id', 'status_url', 'response_url'} при успехе или None.
    Бросает ProviderFatalError при фатальной ошибке провайдера.

    :param log_id: идентификатор записи лога
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
        if _is_provider_fatal(resp.status_code, resp.text):
            msg = f"[{model_name}] Фатальная ошибка провайдера (HTTP {resp.status_code}): {resp.text}"
            write_log_entry(log_id, msg, level='error')
            raise ProviderFatalError(msg)
        write_log_entry(log_id, f"[{model_name}] не-JSON (HTTP {resp.status_code}): {resp.text}", level='warn')
        return None

    if resp.status_code >= 400:
        err = data.get('error', data)
        if _is_provider_fatal(resp.status_code, err):
            msg = f"[{model_name}] Фатальная ошибка провайдера (HTTP {resp.status_code}): {err}"
            write_log_entry(log_id, msg, level='error')
            raise ProviderFatalError(msg)
        write_log_entry(log_id, f"[{model_name}] HTTP {resp.status_code}: {err}", level='warn')
        return None

    if 'request_id' not in data:
        write_log_entry(log_id, f"[{model_name}] нет request_id: {str(data)}", level='warn')
        return None

    request_id   = data['request_id']
    status_url   = data.get('status_url', f"{platform_url}/requests/{request_id}/status")
    response_url = data.get('response_url', f"{platform_url}/requests/{request_id}")
    return {
        'request_id':   request_id,
        'status_url':   status_url,
        'response_url': response_url,
    }


def poll(log_id, status_url: str, response_url: str):
    """
    Поллит fal.ai до завершения генерации.
    Возвращает (video_url, None) при успехе или (None, error_msg) при сбое.

    :param log_id: идентификатор записи лога
    """
    for attempt in range(_POLL_MAX):
        time.sleep(_POLL_INTERVAL)
        try:
            s = requests.get(
                status_url,
                headers=_headers(),
                timeout=15,
            ).json()
            status = s.get('status')
            write_log_entry(log_id, f"Статус [{attempt + 1}]: {status}")

            if status == 'COMPLETED':
                result = requests.get(
                    response_url,
                    headers=_headers(),
                    timeout=15,
                ).json()
                detail = result.get('detail')
                if detail:
                    types = [d.get('type', '') for d in detail] if isinstance(detail, list) else []
                    if any('content_policy' in t for t in types):
                        msg = 'fal.ai отклонил промпт: нарушение политики контента'
                        write_log_entry(log_id, msg, level='error')
                        return None, msg
                video_url = result.get('video', {}).get('url')
                if not video_url:
                    msg = f'Нет URL видео в ответе fal.ai: {str(result)}'
                    write_log_entry(log_id, msg, level='error')
                    return None, msg
                return video_url, None

            elif status == 'FAILED':
                msg = f'fal.ai: генерация провалилась: {str(s)}'
                write_log_entry(log_id, msg, level='error')
                return None, msg

        except Exception as e:
            write_log_entry(log_id, f"Ошибка опроса статуса: {e}", level='warn')

    msg = 'Таймаут генерации видео (2 часа)'
    write_log_entry(log_id, msg, level='error')
    return None, msg


def download_video(log_id, video_url: str) -> bytes:
    """
    Скачивает видео по URL.
    Возвращает байты видео или бросает исключение при ошибке.

    :param log_id: идентификатор записи лога
    """
    r = requests.get(video_url, timeout=120, stream=True)
    r.raise_for_status()
    return b''.join(r.iter_content(chunk_size=256 * 1024))
