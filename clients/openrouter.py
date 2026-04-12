"""
OpenRouter API client.
Отвечает за HTTP-взаимодействие с OpenRouter — отправку промптов и получение текста.
Пайплайн импортирует только этот клиент, не зная об OpenRouter напрямую.
"""

import os
import requests

from log import log_entry

_API_KEY = os.environ.get('OPENROUTER_API_KEY', '')


def is_configured() -> bool:
    """Возвращает True если API-ключ задан."""
    return bool(_API_KEY)


def _headers():
    return {
        'Authorization': f'Bearer {_API_KEY}',
        'Content-Type': 'application/json',
    }


def _build_body(body_tpl, model_url, system_prompt, user_prompt):
    body = dict(body_tpl)
    if 'messages' in body:
        has_system = any(m.get('role') == 'system' for m in body['messages'])
        messages = []
        for msg in body['messages']:
            m = dict(msg)
            if m.get('role') == 'system':
                m['content'] = str(m['content']).format(system_prompt)
            elif m.get('role') == 'user':
                if has_system:
                    m['content'] = str(m['content']).format(user_prompt)
                else:
                    merged = (system_prompt.strip() + '\n\n' + user_prompt.strip()).strip() \
                             if system_prompt else user_prompt
                    m['content'] = str(m['content']).format(merged)
            messages.append(m)
        body['messages'] = messages
    body['model'] = model_url
    return body


def generate(log_id, model_name: str, model: dict, system_prompt: str, user_prompt: str):
    """
    Выполняет один запрос к OpenRouter.
    Возвращает строку с текстом или None при любой ошибке.

    :param log_id: идентификатор записи лога
    :param model_name: отображаемое имя модели для логов
    :param model: словарь с ключами body_tpl, model_url, platform_url
    :param system_prompt: системный промпт
    :param user_prompt: пользовательский промпт
    """
    try:
        body = _build_body(model['body_tpl'], model['model_url'], system_prompt, user_prompt)
        resp = requests.post(model['platform_url'], headers=_headers(), json=body, timeout=60)
    except requests.exceptions.Timeout:
        log_entry(log_id, f"[{model_name}] таймаут (60 с)", level='warn')
        return None
    except requests.exceptions.RequestException as e:
        log_entry(log_id, f"[{model_name}] ошибка соединения: {e}", level='warn')
        return None

    try:
        data = resp.json()
    except ValueError:
        log_entry(log_id, f"[{model_name}] не-JSON (HTTP {resp.status_code}): {resp.text}", level='warn')
        return None

    if resp.status_code >= 400:
        err = data.get('error', {})
        if isinstance(err, dict):
            err = err.get('message', data)
        log_entry(log_id, f"[{model_name}] HTTP {resp.status_code}: {err}", level='warn')
        return None

    choices = data.get('choices')
    if not choices:
        log_entry(log_id, f"[{model_name}] нет поля choices в ответе", level='warn')
        return None

    result = ((choices[0].get('message') or {}).get('content') or '').strip()
    if not result:
        log_entry(log_id, f"[{model_name}] пустой текст", level='warn')
        return None

    return result
