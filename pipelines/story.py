"""
Pipeline story — генерация сюжета.
Принимает batch_id в статусе generating (CAS при dispatch в main_loop),
перебирает активные text-модели по порядку (с retry на каждую),
генерирует текст через OpenRouter и сохраняет результат.
"""

import common.environment as environment
from utils.prompt_params import apply_prompt_params
from db import (
    db_set_story_model,
    settings_get,
    cycle_config_get,
    db_get_batch_by_id,
    db_get_active_text_models,
    db_get_text_model_by_id,
    db_create_story,
    db_set_batch_story,
)
from log import write_log_entry
from pipelines.base import iterate_models
from common.exceptions import AppException
from clients import text_client
from routes.api import client_is_configured
from utils.utils import fmt_id_msg


def run(batch_id, category):
    snap = environment.snapshot()
    batch = db_get_batch_by_id(batch_id)
    if not batch:
        return
    write_log_entry(
        batch_id, category,
        fmt_id_msg(
            "[story] Батч {} — phase=run_start, status={}, type={}",
            batch_id, batch.get("status"), batch.get("type")
        ),
        level='silent',
    )

    if batch["status"] != "generating":
        return

    if batch.get("type") != "story":
        msg = f"Неподдерживаемый тип батча для story-пайплайна: {batch.get('type')}"
        write_log_entry(batch_id, category, msg, level="error")
        raise AppException(batch_id, "story", msg)

    write_log_entry(
        batch_id, category,
        fmt_id_msg("[story] Батч {} — phase=target_resolved, type=story", batch_id),
        level='silent',
    )

    if not client_is_configured('text'):
        msg = "API-ключ текстовой платформы не задан — генерация невозможна"
        write_log_entry(batch_id, category, msg, level="error")
        write_log_entry(batch_id, category, f"[story] {msg}", level='silent')
        raise AppException(batch_id, "story", msg)

    batch_data = batch.get("data") or {}
    pinned_model_id = (
        batch_data.get("story_model_id") if isinstance(batch_data, dict) else None
    )

    # Выбор набора моделей:
    # - pinned_model_id: список из одной модели, один проход.
    # - иначе: перебор всех активных текстовых моделей.
    if pinned_model_id:
        manual_model = db_get_text_model_by_id(pinned_model_id)
        models = [manual_model] if manual_model else []
    else:
        models = db_get_active_text_models()

    if not models:
        msg = "Нет активных text-моделей в ai_models"
        write_log_entry(batch_id, category, msg, level="error")
        write_log_entry(batch_id, category, f"[story] {msg}", level='silent')
        raise AppException(batch_id, "story", msg)

    try:
        max_attempts_per_model = max(1, int(settings_get("story_fails_to_next", "3")))
    except (ValueError, TypeError):
        max_attempts_per_model = 3

    format_prompt = cycle_config_get("format_prompt")
    user_prompt = cycle_config_get("text_prompt")

    user_prompt = apply_prompt_params(user_prompt)
    format_prompt = apply_prompt_params(format_prompt)

    write_log_entry(
        batch_id, category, f"Моделей: {len(models)}, попыток на модель: {max_attempts_per_model}", level='silent'
    )
    write_log_entry(
        batch_id, category,
        fmt_id_msg(
            "[story] Батч {} — phase=models_selected, count={}, pinned={}, max_attempts={}",
            batch_id, len(models), bool(pinned_model_id), max_attempts_per_model,
        ),
        level='silent',
    )

    story_id = None
    used_model_name = None
    used_model_id = None
    attempt_counters = {}

    def story_callback(m):
        nonlocal story_id, used_model_name, used_model_id
        model_name = m["name"]
        cnt = attempt_counters.get(model_name, 0)
        attempt_counters[model_name] = cnt + 1
        write_log_entry(
            batch_id, category,
            fmt_id_msg(
                "[story] Батч {} — phase=model_callback_enter, model={}, attempt={}",
                batch_id, model_name, cnt + 1,
            ),
            level='silent',
        )
        if cnt == 0:
            write_log_entry(batch_id, category, f"Модель: {model_name}")
            write_log_entry(
                batch_id, category, f"[story] Запрос к текстовой платформе: модель={model_name}", level='silent'
            )
        raw = text_client.generate(batch_id, category, model_name, m, format_prompt, user_prompt)
        if raw:
            first_line = raw.split("\n")[0]
            if "." not in first_line:
                title = " ".join(first_line.split()[:4]).rstrip(".")
                text = raw[len(first_line):].strip()
            else:
                title = " ".join(raw.split()[:4]).rstrip(".")
                text = raw
            sid = db_create_story(m["id"], title, text)
            if sid:
                story_id = sid
                used_model_name = model_name
                used_model_id = m["id"]
                write_log_entry(
                    batch_id, category,
                    fmt_id_msg(
                        "[story] Батч {} — phase=model_callback_success, model={}, story_id={}",
                        batch_id, model_name, sid,
                    ),
                    level='silent',
                )
                return sid, title, text
            write_log_entry(batch_id, category, f"[{model_name}] не удалось сохранить сюжет", level="warn")
            write_log_entry(
                batch_id, category,
                fmt_id_msg(
                    "[story] Батч {} — phase=story_save_failed, model={}, attempt={}",
                    batch_id, model_name, cnt + 1,
                ),
                level='silent',
            )
        else:
            write_log_entry(
                batch_id, category,
                f"[{model_name}] попытка {attempt_counters[model_name]}/{max_attempts_per_model} не удалась",
                level="warn",
            )
            write_log_entry(
                batch_id, category,
                fmt_id_msg(
                    "[story] Батч {} — phase=model_callback_no_result, model={}, attempt={}",
                    batch_id, model_name, attempt_counters[model_name],
                ),
                level='silent',
            )
        return None

    max_passes = 1 if pinned_model_id else snap.max_model_passes
    write_log_entry(
        batch_id, category,
        fmt_id_msg("[story] Батч {} — phase=iterate_start, max_passes={}", batch_id, max_passes),
        level='silent',
    )
    iterate_result = iterate_models(
        models, max_attempts_per_model, story_callback, max_passes=max_passes
    )

    if not iterate_result:
        msg = f"Все активные модели не дали результата после {max_passes} проходов"
        write_log_entry(batch_id, category, msg, level="error")
        write_log_entry(batch_id, category, f"[story] {msg}", level='silent')
        write_log_entry(
            batch_id, category,
            fmt_id_msg("[story] Батч {} — phase=iterate_failed, max_passes={}", batch_id, max_passes),
            level='silent',
        )
        raise AppException(batch_id, "story", msg)

    story_id, title, result = iterate_result

    write_log_entry(batch_id, category, f"Название: {title}", level='silent')
    write_log_entry(batch_id, category, f"Сюжет:\n{result}", level='silent')
    write_log_entry(
        batch_id, category,
        f"[story] Сюжет получен: {result[:100]}{'.' if len(result) > 100 else ''}",
        level='silent',
    )

    if not db_set_batch_story(batch_id, story_id):
        msg = "Ошибка сохранения статуса батча (db_set_batch_story вернул False)"
        write_log_entry(batch_id, category, msg, level="error")
        write_log_entry(batch_id, category, f"[story] {msg}", level='silent')
        raise AppException(batch_id, "story", msg)

    db_set_story_model(story_id, used_model_id)
    write_log_entry(batch_id, category, f"Сюжет «{title}» успешно сохранён.")
    write_log_entry(
        batch_id, category,
        fmt_id_msg(
            "[story] Готово: story_id={}, batch → ready", story_id
        ),
        level='silent',
    )
    write_log_entry(
        batch_id, category,
        fmt_id_msg("[story] Батч {} — phase=run_done, status=ready, story_id={}", batch_id, story_id),
        level='silent',
    )
