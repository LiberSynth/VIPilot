"""
Pipeline prompt — преобразование сюжета в T2V-промпт.
Принимает batch_id в статусе processing (CAS при dispatch),
вызывает text-модель из cycle_config (t2v_conversion_model_id),
сохраняет результат в stories.prompt.
"""

import common.environment as environment
from utils.prompt_params import apply_prompt_params
from db import (
    cycle_config_get,
    db_get_batch_by_id,
    db_get_text_model_by_id,
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
        raise AppException(batch_id, "prompt", msg)

    story_id = batch.get("story_id")
    if not story_id:
        msg = "У батча prompt не задан story_id"
        raise AppException(batch_id, "prompt", msg)

    story_content = db_get_story_text(story_id)
    if not story_content or not str(story_content).strip():
        msg = fmt_id_msg("Пустой сюжет story_id={}", story_id)
        raise AppException(batch_id, "prompt", msg)

    if not client_is_configured('text'):
        msg = "API-ключ текстовой платформы не задан — генерация невозможна"
        raise AppException(batch_id, "prompt", msg)

    model_id = (cycle_config_get("t2v_conversion_model_id") or "").strip()
    if not model_id:
        msg = "Не выбрана модель для генерации T2V-промпта (Генерация сюжета → Метапромпт T2V)"
        raise AppException(batch_id, "prompt", msg)

    model = db_get_text_model_by_id(model_id)
    if not model:
        msg = fmt_id_msg("Text-модель t2v_conversion_model_id={} не найдена", model_id)
        raise AppException(batch_id, "prompt", msg)

    user_prompt = cycle_config_get("t2v_conversion_prompt") or ""
    user_prompt = apply_prompt_params(user_prompt, story_content=story_content)

    model_name = model["name"]
    write_log_entry(
        batch_id, category,
        f"Модель: {model_name}, попыток: {snap.max_model_passes}",
        level='silent',
    )
    write_log_entry(batch_id, category, f"Модель: {model_name}")

    attempt_no = 0

    def prompt_callback(m):
        nonlocal attempt_no
        attempt_no += 1
        raw = text_client.generate(batch_id, category, model_name, m, "", user_prompt)
        if raw and str(raw).strip():
            return str(raw).strip()
        write_log_entry(
            batch_id, category,
            f"[{model_name}] попытка {attempt_no}/{snap.max_model_passes} не удалась",
            level="warn",
        )
        return None

    iterate_result = iterate_models(
        [model], 1, prompt_callback, max_passes=snap.max_model_passes,
    )

    if not iterate_result:
        msg = f"Модель {model_name} не дала результата после {snap.max_model_passes} попыток"
        raise AppException(batch_id, "prompt", msg)

    if not db_update_story_prompt(story_id, iterate_result):
        msg = fmt_id_msg("Не удалось сохранить промпт story_id={}", story_id)
        raise AppException(batch_id, "prompt", msg)

    db_set_batch_status(batch_id, "ready")
    write_log_entry(batch_id, category, "T2V-промпт успешно сохранён.")
    write_log_entry(
        batch_id, category,
        fmt_id_msg("[prompt] Батч {} — phase=run_done, status=ready, story_id={}", batch_id, story_id),
        level='silent',
    )
