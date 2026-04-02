"""
Pipeline 2 — Генерация сюжета.
Берёт первый pending-батч, атомарно захватывает его (story_generating),
перебирает активные text-модели по порядку (с retry на каждую),
генерирует текст через OpenRouter и сохраняет результат.
Если все модели не ответили — батч возвращается в pending для следующего цикла.
"""

import os
import requests

from db import (
    db_get,
    db_get_pending_batch,
    db_get_active_text_models,
    db_create_story,
    db_set_batch_story,
    db_set_batch_pending,
    db_is_batch_scheduled,
    db_set_batch_obsolete,
)
from log import db_log_pipeline, db_log_entry, db_log_update, db_log_interrupt_running

_API_KEY = os.environ.get('OPENROUTER_API_KEY', '')


def _headers():
    return {
        'Authorization': f'Bearer {_API_KEY}',
        'Content-Type': 'application/json',
    }


def _build_body(body_tpl, model_url, system_prompt, user_prompt):
    body = dict(body_tpl)
    if 'messages' in body:
        messages = []
        for msg in body['messages']:
            m = dict(msg)
            if m.get('role') == 'system':
                m['content'] = str(m['content']).format(system_prompt)
            elif m.get('role') == 'user':
                m['content'] = str(m['content']).format(user_prompt)
            messages.append(m)
        body['messages'] = messages
    body['model'] = model_url
    return body


def _try_model(log_id, m, system_prompt, user_prompt):
    """Один запрос к одной модели. Возвращает текст сюжета или None."""
    model_name = m['name']
    try:
        body = _build_body(m['body_tpl'], m['model_url'], system_prompt, user_prompt)
        resp = requests.post(m['platform_url'], headers=_headers(), json=body, timeout=60)
    except requests.exceptions.Timeout:
        if log_id:
            db_log_entry(log_id, f"[{model_name}] таймаут (60 с)", level='warn')
        return None
    except requests.exceptions.RequestException as e:
        if log_id:
            db_log_entry(log_id, f"[{model_name}] ошибка соединения: {e}", level='warn')
        return None

    try:
        data = resp.json()
    except ValueError:
        preview = ' '.join(resp.text.split())[:200]
        if log_id:
            db_log_entry(log_id, f"[{model_name}] не-JSON (HTTP {resp.status_code}): {preview}", level='warn')
        return None

    if resp.status_code >= 400:
        err = data.get('error', {})
        if isinstance(err, dict):
            err = err.get('message', data)
        if log_id:
            db_log_entry(log_id, f"[{model_name}] HTTP {resp.status_code}: {err}", level='warn')
        return None

    choices = data.get('choices')
    if not choices:
        if log_id:
            db_log_entry(log_id, f"[{model_name}] нет поля choices в ответе", level='warn')
        return None

    result = ((choices[0].get('message') or {}).get('content') or '').strip()
    if not result:
        if log_id:
            db_log_entry(log_id, f"[{model_name}] пустой текст", level='warn')
        return None

    return result


def run():
    batch_id   = None
    batch_done = False
    log_id     = None

    try:
        db_log_interrupt_running('story')

        batch = db_get_pending_batch()
        if not batch:
            batch_done = True
            return

        batch_id = str(batch['id'])
        target   = batch['target_name']

        if not db_is_batch_scheduled(batch['scheduled_at'], batch['target_id']):
            db_set_batch_obsolete(batch_id)
            db_log_pipeline('story', 'Батч отменён — слот удалён из расписания или таргет отключён',
                            status='прервана', batch_id=batch_id)
            print(f"[story] Батч {batch_id[:8]}… отменён, пропускаю")
            batch_done = True
            return

        print(f"[story] Батч {batch_id[:8]}… ({target}) — начало генерации сюжета")

        log_id = db_log_pipeline(
            'story', 'Генерация сюжета…',
            status='running', batch_id=batch_id,
        )

        if not _API_KEY:
            msg = 'OPENROUTER_API_KEY не задан — генерация невозможна'
            db_log_update(log_id, msg, 'error')
            if log_id:
                db_log_entry(log_id, msg, level='error')
            print(f"[story] {msg}")
            return

        models = db_get_active_text_models()
        if not models:
            msg = 'Нет активных text-моделей в ai_models'
            db_log_update(log_id, msg, 'error')
            if log_id:
                db_log_entry(log_id, msg, level='error')
            print(f"[story] {msg}")
            return

        try:
            fails_to_next = max(1, int(db_get('story_fails_to_next', '3')))
        except (ValueError, TypeError):
            fails_to_next = 3

        system_prompt = db_get('system_prompt', '')
        user_prompt   = db_get('metaprompt', '')

        if log_id:
            preview = user_prompt[:120] + ('…' if len(user_prompt) > 120 else '')
            db_log_entry(log_id, f"Промпт: {preview}")
            db_log_entry(log_id, f"Моделей: {len(models)}, попыток на модель: {fails_to_next}")

        story_id        = None
        used_model_name = None
        used_model_id   = None

        for m in models:
            model_name = m['name']
            if log_id:
                db_log_entry(log_id, f"Модель: {model_name}")
            print(f"[story] Запрос к OpenRouter: модель={model_name}")

            for attempt in range(fails_to_next):
                result = _try_model(log_id, m, system_prompt, user_prompt)
                if result:
                    story_id = db_create_story(m['id'], result)
                    if story_id:
                        used_model_name = model_name
                        used_model_id   = m['id']
                        break
                    if log_id:
                        db_log_entry(log_id, f"[{model_name}] не удалось сохранить сюжет", level='warn')
                else:
                    if log_id:
                        db_log_entry(log_id, f"[{model_name}] попытка {attempt + 1}/{fails_to_next} не удалась", level='warn')

            if story_id:
                break

        if not story_id:
            msg = 'Все активные модели не дали результата — батч возвращён в очередь'
            db_log_update(log_id, msg, 'warn')
            if log_id:
                db_log_entry(log_id, msg, level='warn')
            print(f"[story] {msg}")
            return

        preview = result[:200] + ('…' if len(result) > 200 else '')
        print(f"[story] Сюжет получен: {result[:100]}{'…' if len(result) > 100 else ''}")
        if log_id:
            db_log_entry(log_id, f"Сюжет: {preview}")

        db_set_batch_story(batch_id, story_id)
        batch_done = True

        msg = f'Сюжет сгенерирован ({used_model_name})'
        db_log_update(log_id, msg, 'ok')
        if log_id:
            db_log_entry(log_id, f"Сохранён как story {story_id[:8]}…, батч → story_ready")

        print(f"[story] Готово: story_id={story_id[:8]}…, batch → story_ready")

    except Exception as e:
        msg = f"Сбой пайплайна: {e}"
        if log_id:
            db_log_update(log_id, msg, 'error')
            db_log_entry(log_id, msg, level='error')
        else:
            new_log_id = db_log_pipeline('story', msg, status='error', batch_id=batch_id)
            if new_log_id:
                db_log_entry(new_log_id, msg, level='error')
        print(f"[story] Ошибка: {e}")

    finally:
        if batch_id and not batch_done:
            db_set_batch_pending(batch_id)
            print(f"[story] Батч {batch_id[:8]}… сброшен в pending (повторная попытка)")
