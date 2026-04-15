"""
OpenRouter API client.
Отвечает за HTTP-взаимодействие с OpenRouter/DeepSeek и совместимыми API —
отправку промптов и получение текста.
Пайплайн импортирует только этот клиент, не зная об OpenRouter напрямую.
"""

import os
import requests

from log import write_log_entry


def is_configured() -> bool:
    """Возвращает True если хотя бы один известный API-ключ задан."""
    return bool(
        os.environ.get('OPENROUTER_API_KEY') or os.environ.get('DEEPSEEK_API_KEY')
    )


def _headers(key_env: str | None):
    api_key = os.environ.get(key_env or '', '') if key_env else ''
    return {
        'Authorization': f'Bearer {api_key}',
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
    Выполняет один запрос к платформе (OpenRouter, DeepSeek или совместимой).
    Возвращает строку с текстом или None при любой ошибке.

    :param log_id: идентификатор записи лога
    :param model_name: отображаемое имя модели для логов
    :param model: словарь с ключами body_tpl, model_url, platform_url, key_env
    :param system_prompt: системный промпт
    :param user_prompt: пользовательский промпт
    """
    try:
        body = _build_body(model['body_tpl'], model['model_url'], system_prompt, user_prompt)
        resp = requests.post(
            model['platform_url'],
            headers=_headers(model.get('key_env')),
            json=body,
            timeout=60,
        )
    except requests.exceptions.Timeout:
        write_log_entry(log_id, f"[{model_name}] таймаут (60 с)", level='warn')
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
        err = data.get('error', {})
        if isinstance(err, dict):
            err = err.get('message', data)
        write_log_entry(log_id, f"[{model_name}] HTTP {resp.status_code}: {err}", level='warn')
        return None

    choices = data.get('choices')
    if not choices:
        write_log_entry(log_id, f"[{model_name}] нет поля choices в ответе", level='warn')
        return None

    result = ((choices[0].get('message') or {}).get('content') or '').strip()
    if not result:
        write_log_entry(log_id, f"[{model_name}] пустой текст", level='warn')
        return None

    return result
