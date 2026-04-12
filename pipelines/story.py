"""
Pipeline 2 — Генерация сюжета.
Принимает batch_id, атомарно переводит батч в story_generating,
перебирает активные text-модели по порядку (с retry на каждую),
генерирует текст через OpenRouter и сохраняет результат.
"""

import os
import requests

from db import (
    db_set_story_model,
    db_get,
    db_get_batch_by_id,
    db_get_active_targets,
    db_set_batch_story_generating_by_id,
    db_get_active_text_models,
    db_get_text_model_by_id,
    db_create_story,
    db_set_batch_story,
    db_set_batch_story_probe,
    db_set_batch_status,
    db_claim_donor_batch,
    db_set_batch_story_ready_from_donor,
    db_claim_unused_story_for_batch,
    db_get_story_title,
    env_get,
)
from log import db_log_pipeline, db_log_entry, db_log_update
from pipelines.base import check_cancelled, handle_critical_error

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

        if batch['status'] not in ('pending', 'story_generating'):
            return

        if batch['status'] == 'pending':
            if not db_set_batch_story_generating_by_id(batch_id):
                return

        is_probe = batch['type'] in ('story_probe', 'movie_probe')
        if is_probe:
            target = 'пробный'
        else:
            active_targets = db_get_active_targets()
            tgt    = active_targets[0] if active_targets else {}
            target = tgt.get('name') or 'adhoc'

        if batch.get('story_id') is not None and batch['type'] == 'movie_probe':
            preset_story_id = str(batch['story_id'])
            log_id = db_log_pipeline(
                'story', 'Сюжет задан вручную — пропуск поиска и генерации',
                status='ok', batch_id=batch_id,
            )
            if log_id:
                db_log_entry(log_id, f"Используется заданный сюжет id={preset_story_id[:8]}…")
            print(f"[story] Батч {batch_id[:8]}… — story_id задан вручную ({preset_story_id[:8]}…), батч → story_ready")
            if not db_set_batch_story(batch_id, preset_story_id):
                db_set_batch_status(batch_id, 'error')
            batch_done = True
            return

        if not is_probe and check_cancelled('story', batch_id, batch):
            batch_done = True
            return

        if not is_probe and env_get('emulation_mode', '0') != '1' and env_get('use_donor', '1') == '1' \
                and batch.get('story_id') is None:
            batch_data = batch.get('data') or {}
            donor_batch_id = batch_data.get('donor_batch_id') if isinstance(batch_data, dict) else None

            if donor_batch_id is None:
                db_claim_donor_batch(batch_id)
                batch = db_get_batch_by_id(batch_id)
                if not batch:
                    return
                batch_data = batch.get('data') or {}
                donor_batch_id = batch_data.get('donor_batch_id') if isinstance(batch_data, dict) else None

            if donor_batch_id:
                donor_batch = db_get_batch_by_id(donor_batch_id)
                donor_story_id = str(donor_batch['story_id']) if donor_batch and donor_batch.get('story_id') else None
                if not db_set_batch_story_ready_from_donor(batch_id, donor_batch_id, donor_story_id):
                    msg = f'Не удалось записать donor_batch_id для батча {batch_id[:8]}… — донор {donor_batch_id[:8]}…'
                    log_id = db_log_pipeline(
                        'story', msg,
                        status='error', batch_id=batch_id,
                    )
                    db_set_batch_status(batch_id, 'error')
                    print(f"[story] {msg}")
                    batch_done = True
                    return
                donor_title = db_get_story_title(donor_story_id) if donor_story_id else None
                if donor_title:
                    detail = f"Включен режим «Использовать донора». Контент будет заимствован от донора. Сюжет: «{donor_title}»"
                else:
                    detail = "Включен режим «Использовать донора». Контент будет заимствован от донора."
                log_id = db_log_pipeline(
                    'story', 'Найден донор, генерация сюжета не требуется',
                    status='ok', batch_id=batch_id,
                )
                if log_id:
                    db_log_entry(log_id, detail)
                print(f"[story] Батч {batch_id[:8]}… — найден донор {donor_batch_id}, батч → story_ready")
                batch_done = True
                return

        if batch['type'] != 'story_probe':
            approve_stories = db_get('approve_stories', '0') == '1'
            grade_required  = approve_stories
            condition_label = 'grade = good' if grade_required else 'любой grade (включая NULL)'
            pool_log_id = db_log_pipeline(
                'story', 'Поиск сюжета в пуле…',
                status='running', batch_id=batch_id,
            )
            db_log_entry(
                pool_log_id,
                f"Настройка «Утверждать сюжеты»: {'включена' if approve_stories else 'выключена'}. "
                f"Условие выборки: {condition_label}.",
            )
            pool_story = db_claim_unused_story_for_batch(batch_id, grade_required)
            if pool_story:
                pool_story_id    = pool_story['id']
                pool_story_title = pool_story['title']
                db_log_entry(
                    pool_log_id,
                    f"Найден сюжет из пула: id={pool_story_id[:8]}…, название=«{pool_story_title}». "
                    f"AI-генерация не запускается.",
                )
                db_log_update(pool_log_id, f'Сюжет из пула: «{pool_story_title}»', 'ok')
                print(f"[story] Батч {batch_id[:8]}… — сюжет из пула {pool_story_id[:8]}…, батч → story_ready")
                batch_done = True
                return
            else:
                if approve_stories:
                    msg = 'Пул сюжетов пуст (grade = good) — AI-генерация запрещена (approve_stories включён)'
                    db_log_entry(pool_log_id, msg, level='error')
                    db_log_update(pool_log_id, msg, 'error')
                    db_set_batch_status(batch_id, 'error')
                    print(f"[story] Батч {batch_id[:8]}… — {msg}")
                    batch_done = True
                    return
                reason = (
                    f"Подходящий сюжет в пуле не найден (условие: {condition_label}). "
                    f"Переход к AI-генерации."
                )
                db_log_entry(pool_log_id, reason)
                db_log_update(pool_log_id, 'Сюжет в пуле не найден — запускается AI-генерация', 'ok')
                print(f"[story] Батч {batch_id[:8]}… — {reason}")

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
        is_story_probe = batch['type'] == 'story_probe'
        probe_model_id = batch_data.get('probe_model_id') if isinstance(batch_data, dict) else None

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
                        first_line = result.split('\n')[0]
                        if '.' not in first_line:
                            title = ' '.join(first_line.split()[:4]).rstrip('.')
                            result = result[len(first_line):].strip()
                        else:
                            title = ' '.join(result.split()[:4]).rstrip('.')
                        story_id = db_create_story(m['id'], title, result)
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
                    db_set_batch_status(batch_id, 'error')
                    batch_done = True
                    return
                msg = 'Все активные модели не дали результата — повтор с первой модели'
                db_log_update(log_id, msg, 'warn')
                if log_id:
                    db_log_entry(log_id, msg, level='warn')
                print(f"[story] {msg}")

        print(f"[story] Сюжет получен: {result[:100]}{'…' if len(result) > 100 else ''}")
        if log_id:
            db_log_entry(log_id, f"Название: {title}")
            db_log_entry(log_id, f"Сюжет:\n{result}")

        if is_story_probe:
            db_set_batch_story_probe(batch_id, story_id)
            db_set_story_model(story_id, used_model_id)
            batch_done = True
            msg = f'Сюжет сгенерирован ({used_model_name})'
            db_log_update(log_id, msg, 'ok')
            if log_id:
                db_log_entry(log_id, f"Сохранён как story {story_id}, батч → story_probe")
            print(f"[story] Пробный сюжет: story_id={story_id[:8]}…, batch → story_probe")
        else:
            if not db_set_batch_story(batch_id, story_id):
                msg = 'Ошибка сохранения статуса батча (db_set_batch_story вернул False)'
                db_log_update(log_id, msg, 'error')
                if log_id:
                    db_log_entry(log_id, msg, level='error')
                print(f"[story] {msg}")
                return
            db_set_story_model(story_id, used_model_id)
            batch_done = True
            msg = f'Сюжет сгенерирован ({used_model_name})'
            db_log_update(log_id, msg, 'ok')
            if log_id:
                db_log_entry(log_id, f"Сохранён как story {story_id}, батч → story_ready")
            print(f"[story] Готово: story_id={story_id[:8]}…, batch → story_ready")

    except Exception as e:
        handle_critical_error('story', batch_id, log_id, e)

