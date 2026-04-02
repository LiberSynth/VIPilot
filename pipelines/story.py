"""
Pipeline 2 — Генерация сюжета.
Берёт первый pending-батч, атомарно захватывает его (story_generating),
генерирует текст через активную text-модель (OpenRouter) и сохраняет результат.
При любом сбое finally-блок возвращает батч в pending для повторной попытки.
"""

import os
import requests

from db import (
    db_get,
    db_get_pending_batch,
    db_get_active_text_model,
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


def run():
    batch_id   = None
    batch_done = False   # True когда батч переведён в финальный статус (любой, кроме story_generating)
    log_id     = None

    try:
        db_log_interrupt_running('story')

        # Атомарный захват: batch сразу переводится в story_generating,
        # поэтому следующий поток его не возьмёт.
        batch = db_get_pending_batch()
        if not batch:
            batch_done = True   # нечего делать — сброс не нужен
            return

        batch_id = str(batch['id'])
        target   = batch['target_name']

        if not db_is_batch_scheduled(batch['scheduled_at'], batch['target_id']):
            db_set_batch_obsolete(batch_id)
            db_log_pipeline('story', 'Батч устарел — слот удалён из расписания или таргет отключён',
                            status='прервана', batch_id=batch_id)
            print(f"[story] Батч {batch_id[:8]}… устарел, пропускаю")
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
            return   # finally сбросит в pending

        platform_url, model_url, body_tpl, model_name, model_id = db_get_active_text_model()
        if not platform_url:
            msg = 'Нет активной text-модели в ai_models'
            db_log_update(log_id, msg, 'error')
            if log_id:
                db_log_entry(log_id, msg, level='error')
            print(f"[story] {msg}")
            return   # finally сбросит в pending

        if log_id:
            db_log_entry(log_id, f"Модель: {model_name}")

        system_prompt = db_get('system_prompt', '')
        user_prompt   = db_get('metaprompt', '')

        if log_id:
            preview = user_prompt[:120] + ('…' if len(user_prompt) > 120 else '')
            db_log_entry(log_id, f"Промпт: {preview}")

        body = _build_body(body_tpl, model_url, system_prompt, user_prompt)
        print(f"[story] Запрос к OpenRouter: модель={model_name}")

        try:
            resp = requests.post(platform_url, headers=_headers(), json=body, timeout=60)
        except requests.exceptions.Timeout:
            msg = 'Таймаут запроса к OpenRouter (60 сек)'
            db_log_update(log_id, msg, 'error')
            if log_id:
                db_log_entry(log_id, msg, level='error')
            print(f"[story] {msg}")
            return   # finally сбросит в pending
        except requests.exceptions.RequestException as e:
            msg = f'Ошибка соединения с OpenRouter: {e}'
            db_log_update(log_id, msg, 'error')
            if log_id:
                db_log_entry(log_id, msg, level='error')
            print(f"[story] {msg}")
            return   # finally сбросит в pending

        try:
            data = resp.json()
        except ValueError:
            body_preview = ' '.join(resp.text.split())[:200]
            msg = f'OpenRouter вернул не-JSON (HTTP {resp.status_code}): {body_preview or "(пустое тело)"}'
            db_log_update(log_id, msg, 'error')
            if log_id:
                db_log_entry(log_id, msg, level='error')
            print(f"[story] {msg}")
            return   # finally сбросит в pending

        if resp.status_code >= 400:
            err = data.get('error', {})
            if isinstance(err, dict):
                err = err.get('message', data)
            msg = f'OpenRouter HTTP {resp.status_code}: {err}'
            db_log_update(log_id, msg, 'error')
            if log_id:
                db_log_entry(log_id, msg, level='error')
            print(f"[story] {msg}")
            return   # finally сбросит в pending

        choices = data.get('choices')
        if not choices:
            msg = f'OpenRouter: нет поля choices в ответе: {str(data)[:300]}'
            db_log_update(log_id, msg, 'error')
            if log_id:
                db_log_entry(log_id, msg, level='error')
            print(f"[story] {msg}")
            return   # finally сбросит в pending

        result = (choices[0].get('message') or {}).get('content', '').strip()
        if not result:
            msg = 'OpenRouter вернул пустой текст'
            db_log_update(log_id, msg, 'error')
            if log_id:
                db_log_entry(log_id, msg, level='error')
            print(f"[story] {msg}")
            return   # finally сбросит в pending

        preview = result[:200] + ('…' if len(result) > 200 else '')
        print(f"[story] Сюжет получен: {result[:100]}{'…' if len(result) > 100 else ''}")
        if log_id:
            db_log_entry(log_id, f"Сюжет: {preview}")

        story_id = db_create_story(model_id, result)
        if not story_id:
            msg = 'Не удалось сохранить сюжет в БД'
            db_log_update(log_id, msg, 'error')
            if log_id:
                db_log_entry(log_id, msg, level='error')
            print(f"[story] {msg}")
            return   # finally сбросит в pending

        db_set_batch_story(batch_id, story_id)
        batch_done = True   # статус теперь story_ready — сброс не нужен

        msg = f'Сюжет сгенерирован ({model_name})'
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
        # Если батч был захвачен (story_generating) но не завершён нормально —
        # возвращаем в pending, чтобы следующий цикл повторил попытку.
        if batch_id and not batch_done:
            db_set_batch_pending(batch_id)
            print(f"[story] Батч {batch_id[:8]}… сброшен в pending (повторная попытка)")
