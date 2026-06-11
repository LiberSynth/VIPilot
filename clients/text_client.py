"""
Клиент для текстовых платформ (OpenAI-совместимый Chat API).
Поддерживает OpenRouter, DeepSeek и любой совместимый провайдер —
координаты берутся из записи модели (platform_url, env_key_name).
"""

import os
import json
import requests

from log import write_log_entry

def _headers(env_key_name: str | None):
    api_key = os.environ.get(env_key_name or '', '') if env_key_name else ''
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

def _compact_json(value, limit: int = 1200) -> str:
    """Возвращает компактный JSON/строку для диагностических логов с ограничением размера."""
    try:
        text = json.dumps(value, ensure_ascii=False, separators=(',', ':'))
    except Exception:
        text = str(value)
    if len(text) <= limit:
        return text
    return text[:limit] + '...'

def generate(batch_id, category, model_name: str, model: dict, system_prompt: str, user_prompt: str):
    """
    Выполняет один запрос к текстовой платформе.
    Возвращает строку с текстом или None при любой ошибке.

    :param log_id: идентификатор записи лога
    :param model_name: отображаемое имя модели для логов
    :param model: словарь с ключами body_tpl, model_url, platform_url, env_key_name
    :param system_prompt: системный промпт
    :param user_prompt: пользовательский промпт
    """
    try:
        body = _build_body(model['body_tpl'], model['model_url'], system_prompt, user_prompt)
        write_log_entry(
            batch_id, category,
            (
                f"text request: model={model_name}, "
                f"platform_url={model.get('platform_url')}, model_url={model.get('model_url')}, "
                f"env_key_name={model.get('env_key_name')}"
            ),
            level='silent',
        )
        resp = requests.post(
            model['platform_url'],
            headers=_headers(model.get('env_key_name')),
            json=body,
            timeout=60,
        )
    except requests.exceptions.Timeout:
        write_log_entry(batch_id, category, f"[{model_name}] таймаут (60 с)", level='warn')
        return None
    except requests.exceptions.RequestException as e:
        write_log_entry(batch_id, category, f"[{model_name}] ошибка соединения: {e}", level='warn')
        return None

    try:
        data = resp.json()
    except ValueError:
        write_log_entry(batch_id, category, f"[{model_name}] не-JSON (HTTP {resp.status_code}): {resp.text}", level='warn')
        write_log_entry(
            batch_id, category,
            f"text response non-json: model={model_name}, http={resp.status_code}, body={_compact_json(resp.text)}",
            level='silent',
        )
        return None

    write_log_entry(
        batch_id, category,
        f"text response: model={model_name}, http={resp.status_code}, keys={list(data.keys()) if isinstance(data, dict) else []}",
        level='silent',
    )

    if resp.status_code >= 400:
        err = data.get('error', {})
        if isinstance(err, dict):
            err = err.get('message', data)
        write_log_entry(batch_id, category, f"[{model_name}] HTTP {resp.status_code}: {err}", level='warn')
        write_log_entry(
            batch_id, category,
            f"text response error: model={model_name}, http={resp.status_code}, error={_compact_json(err)}",
            level='silent',
        )
        return None

    choices = data.get('choices')
    if not choices:
        write_log_entry(batch_id, category, f"[{model_name}] нет поля choices в ответе", level='warn')
        write_log_entry(
            batch_id, category,
            f"text response invalid: model={model_name}, reason=no_choices, body={_compact_json(data)}",
            level='silent',
        )
        return None

    result = ((choices[0].get('message') or {}).get('content') or '').strip()
    if not result:
        write_log_entry(batch_id, category, f"[{model_name}] пустой текст", level='warn')
        write_log_entry(
            batch_id, category,
            f"text response invalid: model={model_name}, reason=empty_content, body={_compact_json(data)}",
            level='silent',
        )
        return None

    write_log_entry(
        batch_id, category,
        f"text response ok: model={model_name}, chars={len(result)}",
        level='silent',
    )

    return result
