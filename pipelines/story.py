"""
Pipeline 2 — Генерация сюжета.
Принимает batch_id, атомарно переводит батч в story_generating,
перебирает активные text-модели по порядку (с retry на каждую),
генерирует текст через OpenRouter и сохраняет результат.
"""

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
from log import db_log_update
from pipelines.base import check_cancelled, pipeline_log, iterate_models
from exceptions import AppException
from clients import openrouter
from utils.utils import fmt_id_msg


def run(batch_id, log_id):
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
            db_log_update(log_id, 'Сюжет назначен пользователем — пропуск поиска и генерации', 'ok')
            _story_title = db_get_story_title(preset_story_id) or preset_story_id
            pipeline_log(log_id, fmt_id_msg('Используется заданный сюжет "{}"', _story_title))
            pipeline_log(None, fmt_id_msg("[story] Батч {} — story_id назначен пользователем ({}), батч → story_ready", batch_id, preset_story_id))
            if not db_set_batch_story(batch_id, preset_story_id):
                msg = fmt_id_msg('Ошибка записи story_id={} в БД', preset_story_id)
                pipeline_log(log_id, msg, level='error')
                raise AppException(batch_id, 'story', msg, log_id)
            return

        if not is_probe and check_cancelled('story', batch_id, batch, log_id):
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
                    msg = fmt_id_msg('Не удалось записать donor_batch_id для батча {} — донор {}', batch_id, donor_batch_id)
                    db_log_update(log_id, msg, 'error')
                    pipeline_log(None, f"[story] {msg}")
                    raise AppException(batch_id, 'story', msg, log_id)
                donor_title = db_get_story_title(donor_story_id) if donor_story_id else None
                if donor_title:
                    detail = f"Включен режим «Использовать донора». Контент будет заимствован от донора. Сюжет: «{donor_title}»"
                else:
                    detail = "Включен режим «Использовать донора». Контент будет заимствован от донора."
                db_log_update(log_id, 'Найден донор, генерация сюжета не требуется', 'ok')
                pipeline_log(log_id, detail)
                pipeline_log(None, fmt_id_msg("[story] Батч {} — найден донор {}, батч → story_ready", batch_id, donor_batch_id))
                return

        if batch['type'] != 'story_probe':
            approve_stories = db_get('approve_stories', '0') == '1'
            grade_required  = approve_stories
            condition_label = 'grade = good' if grade_required else 'любой grade (включая NULL)'
            db_log_update(log_id, 'Поиск сюжета в пуле…', 'running')
            pipeline_log(
                log_id,
                f"Настройка «Утверждать сюжеты»: {'включена' if approve_stories else 'выключена'}. "
                f"Условие выборки: {condition_label}.",
            )
            pool_story = db_claim_unused_story_for_batch(batch_id, grade_required)
            if pool_story:
                pool_story_id    = pool_story['id']
                pool_story_title = pool_story['title']
                pipeline_log(
                    log_id,
                    fmt_id_msg("Найден сюжет из пула: id={}, название=«{}». AI-генерация не запускается.", pool_story_id, pool_story_title),
                )
                db_log_update(log_id, f'Сюжет из пула: «{pool_story_title}»', 'ok')
                pipeline_log(None, fmt_id_msg("[story] Батч {} — сюжет из пула {}, батч → story_ready", batch_id, pool_story_id))
                return
            else:
                if approve_stories:
                    msg = 'Пул сюжетов пуст (grade = good) — AI-генерация запрещена (approve_stories включён)'
                    pipeline_log(log_id, msg, level='error')
                    db_log_update(log_id, msg, 'error')
                    pipeline_log(None, fmt_id_msg("[story] Батч {} — {}", batch_id, msg))
                    raise AppException(batch_id, 'story', msg, log_id)
                reason = (
                    f"Подходящий сюжет в пуле не найден (условие: {condition_label}). "
                    f"Переход к AI-генерации."
                )
                pipeline_log(log_id, reason)
                db_log_update(log_id, 'Сюжет в пуле не найден — запускается AI-генерация', 'running')
                pipeline_log(None, fmt_id_msg("[story] Батч {} — {}", batch_id, reason))

        pipeline_log(None, fmt_id_msg("[story] Батч {} ({}) — начало генерации сюжета", batch_id, target))

        db_log_update(log_id, 'Генерация сюжета…', 'running')

        if not openrouter.is_configured():
            msg = 'OPENROUTER_API_KEY не задан — генерация невозможна'
            db_log_update(log_id, msg, 'error')
            pipeline_log(log_id, msg, level='error')
            pipeline_log(None, f"[story] {msg}")
            raise AppException(batch_id, 'story', msg, log_id)

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
            pipeline_log(log_id, msg, level='error')
            pipeline_log(None, f"[story] {msg}")
            raise AppException(batch_id, 'story', msg, log_id)

        try:
            fails_to_next = max(1, int(db_get('story_fails_to_next', '3')))
        except (ValueError, TypeError):
            fails_to_next = 3

        system_prompt = db_get('system_prompt', '')
        user_prompt   = db_get('metaprompt', '')

        pipeline_log(log_id, f"Моделей: {len(models)}, попыток на модель: {fails_to_next}")

        story_id        = None
        used_model_name = None
        used_model_id   = None

        attempt_counters = {}

        def story_callback(m):
            nonlocal story_id, used_model_name, used_model_id
            model_name = m['name']
            cnt = attempt_counters.get(model_name, 0)
            attempt_counters[model_name] = cnt + 1
            if cnt == 0:
                pipeline_log(log_id, f"Модель: {model_name}")
                pipeline_log(None, f"[story] Запрос к OpenRouter: модель={model_name}")
            raw = openrouter.generate(log_id, model_name, m, system_prompt, user_prompt)
            if raw:
                first_line = raw.split('\n')[0]
                if '.' not in first_line:
                    title  = ' '.join(first_line.split()[:4]).rstrip('.')
                    text   = raw[len(first_line):].strip()
                else:
                    title  = ' '.join(raw.split()[:4]).rstrip('.')
                    text   = raw
                sid = db_create_story(m['id'], title, text)
                if sid:
                    story_id        = sid
                    used_model_name = model_name
                    used_model_id   = m['id']
                    return (sid, title, text)
                pipeline_log(log_id, f"[{model_name}] не удалось сохранить сюжет", level='warn')
            else:
                pipeline_log(log_id, f"[{model_name}] попытка {attempt_counters[model_name]}/{fails_to_next} не удалась", level='warn')
            return None

        max_passes = 1 if is_story_probe else 5
        iterate_result = iterate_models(models, fails_to_next, story_callback, max_passes=max_passes)

        if not iterate_result:
            if is_story_probe:
                msg = f'Модель не ответила после {fails_to_next} попыток — пробный сюжет не получен'
            else:
                msg = f'Все активные модели не дали результата после {max_passes} проходов'
            db_log_update(log_id, msg, 'error')
            pipeline_log(log_id, msg, level='error')
            pipeline_log(None, f"[story] {msg}")
            raise AppException(batch_id, 'story', msg, log_id)

        story_id, title, result = iterate_result

        pipeline_log(None, f"[story] Сюжет получен: {result[:100]}{'…' if len(result) > 100 else ''}")
        pipeline_log(log_id, f"Название: {title}")
        pipeline_log(log_id, f"Сюжет:\n{result}")

        if is_story_probe:
            db_set_batch_story_probe(batch_id, story_id)
            db_set_story_model(story_id, used_model_id)
            msg = f'Сюжет сгенерирован ({used_model_name})'
            db_log_update(log_id, msg, 'ok')
            pipeline_log(log_id, fmt_id_msg("Сохранён как story {}, батч → story_probe", story_id))
            pipeline_log(None, fmt_id_msg("[story] Пробный сюжет: story_id={}, batch → story_probe", story_id))
        else:
            if not db_set_batch_story(batch_id, story_id):
                msg = 'Ошибка сохранения статуса батча (db_set_batch_story вернул False)'
                db_log_update(log_id, msg, 'error')
                pipeline_log(log_id, msg, level='error')
                pipeline_log(None, f"[story] {msg}")
                raise AppException(batch_id, 'story', msg, log_id)
            db_set_story_model(story_id, used_model_id)
            msg = f'Сюжет сгенерирован ({used_model_name})'
            db_log_update(log_id, msg, 'ok')
            pipeline_log(log_id, fmt_id_msg("Сохранён как story {}, батч → story_ready", story_id))
            pipeline_log(None, fmt_id_msg("[story] Готово: story_id={}, batch → story_ready", story_id))

    except AppException:
        raise
