"""
fal.ai API client.
Отвечает за HTTP-взаимодействие с fal.ai: submit-запрос, poll-цикл и скачивание видео.
Пайплайн импортирует только этот клиент, не зная о fal.ai напрямую.
"""

import os
import time
import json
import requests

from log import write_log_entry


_FAL_KEY = os.environ.get('FAL_API_KEY', '')

_POLL_INTERVAL = 30
_POLL_MAX      = 240

_DOWNLOAD_MAX_ATTEMPTS = 3
_DOWNLOAD_RETRY_DELAY  = 5


class ProviderFatalError(Exception):
    """Raised when a provider returns a permanent, non-retryable billing/account error."""


def _headers():
    return {
        'Authorization': f'Key {_FAL_KEY}',
        'Content-Type': 'application/json',
    }


def _compact_json(value, limit: int = 1200) -> str:
    """Возвращает компактный JSON/строку для диагностики с ограничением размера."""
    try:
        text = json.dumps(value, ensure_ascii=False, separators=(',', ':'))
    except Exception:
        text = str(value)
    if len(text) <= limit:
        return text
    return text[:limit] + '...'


def _is_fatal_fal(status_code: int, body) -> bool:
    """Detect fal.ai balance-exhausted / account-locked errors (HTTP 403)."""
    if status_code != 403:
        return False
    text = str(body).lower()
    return 'exhausted balance' in text or 'locked' in text


_FATAL_DETECTORS = [_is_fatal_fal]


def _is_provider_fatal(status_code: int, body) -> bool:
    return any(fn(status_code, body) for fn in _FATAL_DETECTORS)


def build_body(body_tpl, prompt, ar_x, ar_y, video_duration, batch_id=None, category=None):
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
                    write_log_entry(
                        batch_id, category,
                        f"Поле «{field}» шаблона не поддерживает подстановку video_duration — оставляем как есть ({e})",
                        level='warn',
                    )

    return body


def submit(batch_id, category, model_name: str, submit_url: str, platform_url: str,
           body_tpl: dict, prompt: str, ar_x: int, ar_y: int,
           video_duration: int):
    """
    Отправляет задание на генерацию видео в fal.ai (одна попытка).

    Возвращает словарь {'request_id', 'status_url', 'response_url'} при успехе или None.
    Бросает ProviderFatalError при фатальной ошибке провайдера.

    :param log_id: идентификатор записи лога
    """
    try:
        body = build_body(body_tpl, prompt, ar_x, ar_y, video_duration, batch_id, category)
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
        if _is_provider_fatal(resp.status_code, resp.text):
            msg = f"[{model_name}] Фатальная ошибка провайдера (HTTP {resp.status_code}): {resp.text}"
            write_log_entry(batch_id, category, msg, level='error')
            raise ProviderFatalError(msg)
        write_log_entry(batch_id, category, f"[{model_name}] не-JSON (HTTP {resp.status_code}): {resp.text}", level='warn')
        return None

    if resp.status_code >= 400:
        err = data.get('error', data)
        if _is_provider_fatal(resp.status_code, err):
            msg = f"[{model_name}] Фатальная ошибка провайдера (HTTP {resp.status_code}): {err}"
            write_log_entry(batch_id, category, msg, level='error')
            raise ProviderFatalError(msg)
        write_log_entry(batch_id, category, f"[{model_name}] HTTP {resp.status_code}: {err}", level='warn')
        return None

    if 'request_id' not in data:
        write_log_entry(batch_id, category, f"[{model_name}] нет request_id: {str(data)}", level='warn')
        return None

    request_id   = data['request_id']
    status_url   = data.get('status_url', f"{platform_url}/requests/{request_id}/status")
    response_url = data.get('response_url', f"{platform_url}/requests/{request_id}")
    return {
        'request_id':   request_id,
        'status_url':   status_url,
        'response_url': response_url,
    }


_POLL_MAX_ERRORS = 10


def poll(batch_id, category, status_url: str, response_url: str):
    """
    Поллит fal.ai до завершения генерации.
    Возвращает (video_url, None) при успехе или (None, error_msg) при сбое.

    Выходит досрочно, если подряд получено _POLL_MAX_ERRORS ошибок (None-ответов):
    это означает недоступность провайдера, а не ожидание выполнения задания.

    :param log_id: идентификатор записи лога
    """
    consecutive_errors = 0
    for attempt in range(_POLL_MAX):
        time.sleep(_POLL_INTERVAL)
        try:
            resp = requests.get(
                status_url,
                headers=_headers(),
                timeout=15,
            )
            try:
                s = resp.json()
            except ValueError:
                consecutive_errors += 1
                write_log_entry(batch_id, category, f"fal.ai poll [{attempt + 1}] не-JSON (HTTP {resp.status_code})", level='warn')
                write_log_entry(
                    batch_id, category,
                    f"[video] fal.ai poll [{attempt + 1}] http={resp.status_code}, body={_compact_json(resp.text)}",
                    level='silent',
                )
                if consecutive_errors >= _POLL_MAX_ERRORS:
                    msg = f'fal.ai недоступен: {_POLL_MAX_ERRORS} ошибок подряд — прерываем поллинг'
                    write_log_entry(batch_id, category, msg, level='error')
                    return None, msg
                continue
            status = s.get('status')
            write_log_entry(batch_id, category, f"Статус [{attempt + 1}]: {status}")
            write_log_entry(
                batch_id, category,
                f"[video] fal.ai poll [{attempt + 1}] http={resp.status_code}, status={status}, keys={list(s.keys()) if isinstance(s, dict) else []}",
                level='silent',
            )
            consecutive_errors = 0

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
                        write_log_entry(batch_id, category, msg, level='error')
                        return None, msg
                video_url = result.get('video', {}).get('url')
                if not video_url:
                    msg = f'Нет URL видео в ответе fal.ai: {str(result)}'
                    write_log_entry(batch_id, category, msg, level='error')
                    return None, msg
                return video_url, None

            elif status == 'FAILED':
                msg = f'fal.ai: генерация провалилась: {str(s)}'
                write_log_entry(batch_id, category, msg, level='error')
                return None, msg

        except Exception as e:
            consecutive_errors += 1
            write_log_entry(batch_id, category, f"Ошибка опроса статуса: {e}", level='warn')
            write_log_entry(batch_id, category, f"[video] fal.ai poll [{attempt + 1}] exception={type(e).__name__}: {e}", level='silent')
            if consecutive_errors >= _POLL_MAX_ERRORS:
                msg = f'fal.ai недоступен: {_POLL_MAX_ERRORS} ошибок подряд — прерываем поллинг'
                write_log_entry(batch_id, category, msg, level='error')
                return None, msg

    msg = 'Таймаут генерации видео (2 часа)'
    write_log_entry(batch_id, category, msg, level='error')
    return None, msg


def download_video(batch_id, category, video_url: str) -> bytes:
    """
    Скачивает видео по URL.
    Возвращает байты видео или бросает исключение при ошибке.

    :param log_id: идентификатор записи лога
    """
    last_err = None
    for attempt in range(1, _DOWNLOAD_MAX_ATTEMPTS + 1):
        try:
            r = requests.get(video_url, timeout=120, stream=True)
            r.raise_for_status()
            return b''.join(r.iter_content(chunk_size=256 * 1024))
        except (requests.exceptions.RequestException, OSError) as e:
            last_err = e
            if attempt < _DOWNLOAD_MAX_ATTEMPTS:
                write_log_entry(
                    batch_id, category,
                    f"Ошибка скачивания (попытка {attempt}/{_DOWNLOAD_MAX_ATTEMPTS}): {e}",
                    level='warn',
                )
                time.sleep(_DOWNLOAD_RETRY_DELAY)
            else:
                write_log_entry(
                    batch_id, category,
                    f"Ошибка скачивания после {_DOWNLOAD_MAX_ATTEMPTS} попыток: {e}",
                    level='error',
                )
    raise last_err
