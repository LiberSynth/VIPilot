"""
Pipeline prompt — преобразование сюжета в T2V-промпт.
Принимает batch_id в статусе processing (CAS при dispatch),
перебирает активные text-модели, сохраняет результат в stories.prompt.
"""

import common.environment as environment
from utils.prompt_params import apply_prompt_params
from db import (
    settings_get,
    cycle_config_get,
    db_get_batch_by_id,
    db_get_active_text_models,
    db_get_story_text,
    db_update_story_prompt,
    db_set_batch_status,
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
            "[prompt] Батч {} — phase=run_start, status={}, type={}",
            batch_id, batch.get("status"), batch.get("type"),
        ),
        level='silent',
    )

    if batch["status"] != "processing":
        return

    if batch.get("type") != "prompt":
        msg = f"Неподдерживаемый тип батча для prompt-пайплайна: {batch.get('type')}"
        write_log_entry(batch_id, category, msg, level="error")
        raise AppException(batch_id, "prompt", msg)

    story_id = batch.get("story_id")
    if not story_id:
        msg = "У батча prompt не задан story_id"
        write_log_entry(batch_id, category, msg, level="error")
        raise AppException(batch_id, "prompt", msg)

    story_content = db_get_story_text(story_id)
    if not story_content or not str(story_content).strip():
        msg = fmt_id_msg("Пустой сюжет story_id={}", story_id)
        write_log_entry(batch_id, category, msg, level="error")
        raise AppException(batch_id, "prompt", msg)

    if not client_is_configured('text'):
        msg = "API-ключ текстовой платформы не задан — генерация невозможна"
        write_log_entry(batch_id, category, msg, level="error")
        raise AppException(batch_id, "prompt", msg)

    models = db_get_active_text_models()
    if not models:
        msg = "Нет активных text-моделей в ai_models"
        write_log_entry(batch_id, category, msg, level="error")
        raise AppException(batch_id, "prompt", msg)

    try:
        max_attempts_per_model = int(settings_get("story_fails_to_next", "3"))
    except (ValueError, TypeError):
        max_attempts_per_model = 3

    user_prompt = cycle_config_get("t2v_conversion_prompt") or ""
    user_prompt = apply_prompt_params(user_prompt, story_content=story_content)

    write_log_entry(
        batch_id, category,
        f"Моделей: {len(models)}, попыток на модель: {max_attempts_per_model}",
        level='silent',
    )

    attempt_counters = {}

    def prompt_callback(m):
        model_name = m["name"]
        cnt = attempt_counters.get(model_name, 0)
        attempt_counters[model_name] = cnt + 1
        if cnt == 0:
            write_log_entry(batch_id, category, f"Модель: {model_name}")
        raw = text_client.generate(batch_id, category, model_name, m, "", user_prompt)
        if raw and str(raw).strip():
            return str(raw).strip()
        write_log_entry(
            batch_id, category,
            f"[{model_name}] попытка {attempt_counters[model_name]}/{max_attempts_per_model} не удалась",
            level="warn",
        )
        return None

    iterate_result = iterate_models(
        models, max_attempts_per_model, prompt_callback, max_passes=snap.max_model_passes,
    )

    if not iterate_result:
        msg = f"Все активные модели не дали результата после {snap.max_model_passes} проходов"
        write_log_entry(batch_id, category, msg, level="error")
        raise AppException(batch_id, "prompt", msg)

    if not db_update_story_prompt(story_id, iterate_result):
        msg = fmt_id_msg("Не удалось сохранить промпт story_id={}", story_id)
        write_log_entry(batch_id, category, msg, level="error")
        raise AppException(batch_id, "prompt", msg)

    db_set_batch_status(batch_id, "ready")
    write_log_entry(batch_id, category, "T2V-промпт успешно сохранён.")
    write_log_entry(
        batch_id, category,
        fmt_id_msg("[prompt] Батч {} — phase=run_done, status=ready, story_id={}", batch_id, story_id),
        level='silent',
    )
