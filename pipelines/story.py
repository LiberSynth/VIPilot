"""
Pipeline 2 — Генерация сюжета.
Принимает batch_id, атомарно переводит батч в story_generating,
перебирает активные text-модели по порядку (с retry на каждую),
генерирует текст через OpenRouter и сохраняет результат.
"""

import os
import requests

from utils.notify import notify_failure
from db import (
    db_set_batch_text_model,
    db_get,
    db_get_batch_by_id,
    db_set_batch_story_generating_by_id,
    db_get_active_text_models,
    db_get_text_model_by_id,
    db_create_story,
    db_set_batch_story,
    db_set_batch_story_probe,
    db_set_batch_story_error,
    db_set_batch_pending,
    db_is_batch_scheduled,
    db_set_batch_obsolete,
    db_steal_video_from_cancelled,
    env_get,
)
from log import db_log_pipeline, db_log_entry, db_log_update

_API_KEY = os.environ.get('OPENROUTER_API_KEY', '')


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
        if log_id:
            db_log_entry(log_id, f"[{model_name}] не-JSON (HTTP {resp.status_code}): {resp.text}", level='warn')
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


def run(batch_id):
    batch_done = False
    log_id     = None

    try:
        batch = db_get_batch_by_id(batch_id)
        if not batch:
            return

        if batch['status'] != 'pending':
            return

        if not db_set_batch_story_generating_by_id(batch_id):
            return

        target   = batch['target_name'] or 'пробный'
        is_probe = batch['target_id'] is None

        if not is_probe and not db_is_batch_scheduled(batch['scheduled_at'], batch['target_id']):
            db_set_batch_obsolete(batch_id)
            db_log_pipeline('story', 'Батч отменён — слот удалён из расписания или таргет отключён',
                            status='прервана', batch_id=batch_id)
            print(f"[story] Батч {batch_id[:8]}… отменён, пропускаю")
            batch_done = True
            return

        if not is_probe and env_get('emulation_mode', '0') != '1' and env_get('use_donor', '1') == '1':
            donor_id = db_steal_video_from_cancelled(batch_id)
            if donor_id:
                msg = f"Видео взято из отменённого батча {donor_id[:8]}… — генерация пропущена"
                log_id = db_log_pipeline(
                    'story', msg,
                    status='ok', batch_id=batch_id,
                )
                if log_id:
                    db_log_entry(log_id, f"Батч-донор: {donor_id}")
                    db_log_entry(log_id, "Видео перенесено — генерация и транскодирование пропущены или сокращены")
                print(f"[story] Батч {batch_id[:8]}… — {msg}")
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

        batch_data     = batch.get('data') or {}
        is_story_probe = batch_data.get('story_probe', False) if isinstance(batch_data, dict) else False
        probe_model_id = str(batch['text_model_id']) if batch.get('text_model_id') else None

        if is_story_probe and probe_model_id:
            probe_model = db_get_text_model_by_id(probe_model_id)
            models = [probe_model] if probe_model else []
        else:
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
            db_log_entry(log_id, f"Промпт:\n{user_prompt}")
            db_log_entry(log_id, f"Моделей: {len(models)}, попыток на модель: {fails_to_next}")

        story_id        = None
        used_model_name = None
        used_model_id   = None

        while not story_id:
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
                if is_story_probe:
                    msg = f'Модель не ответила после {fails_to_next} попыток — пробный сюжет не получен'
                    db_log_update(log_id, msg, 'error')
                    if log_id:
                        db_log_entry(log_id, msg, level='error')
                    print(f"[story] {msg}")
                    db_set_batch_story_error(batch_id)
                    batch_done = True
                    return
                msg = 'Все активные модели не дали результата — повтор с первой модели'
                db_log_update(log_id, msg, 'warn')
                if log_id:
                    db_log_entry(log_id, msg, level='warn')
                print(f"[story] {msg}")

        print(f"[story] Сюжет получен: {result[:100]}{'…' if len(result) > 100 else ''}")
        if log_id:
            db_log_entry(log_id, f"Сюжет:\n{result}")

        if is_story_probe:
            db_set_batch_story_probe(batch_id, story_id)
            db_set_batch_text_model(batch_id, used_model_id)
            batch_done = True
            msg = f'Сюжет сгенерирован ({used_model_name})'
            db_log_update(log_id, msg, 'ok')
            if log_id:
                db_log_entry(log_id, f"Сохранён как story {story_id[:8]}…, батч → story_probe")
            print(f"[story] Пробный сюжет: story_id={story_id[:8]}…, batch → story_probe")
        else:
            db_set_batch_story(batch_id, story_id)
            db_set_batch_text_model(batch_id, used_model_id)
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
        notify_failure(f"сбой story-пайплайна: {e}")

    finally:
        if not batch_done:
            db_set_batch_pending(batch_id)
            print(f"[story] Батч {batch_id[:8]}… сброшен в pending (повторная попытка)")
