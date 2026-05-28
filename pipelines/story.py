"""
Pipeline 2 — Генерация сюжета.
Принимает batch_id, атомарно переводит батч в story_generating,
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
    db_get_active_targets,
    db_claim_batch_status,
    db_get_active_text_models,
    db_get_text_model_by_id,
    db_create_story,
    db_set_batch_story,
    db_finalize_story_manual,
    db_claim_donor_batch,
    db_set_batch_story_ready_from_donor,
    db_claim_unused_story_for_batch,
    db_get_story_title,
)
from log import db_log_update, write_log_entry
from pipelines.base import check_cancelled, iterate_models
from common.exceptions import AppException
from clients import text_client
from routes.api import client_is_configured
from utils.utils import fmt_id_msg


def run(batch_id, log_id):
    snap = environment.snapshot()
    batch = db_get_batch_by_id(batch_id)
    if not batch:
        db_log_update(log_id, "Батч не найден", "error")
        return
    write_log_entry(
        log_id,
        fmt_id_msg("[story] Батч {} — phase=run_start, status={}, type={}", batch_id, batch.get("status"), batch.get("type")),
        level='silent',
    )

    # Два допустимых входных статуса:
    # - pending:          батч только что создан. CAS-переход pending → story_generating
    #                     защищает от двойного подхвата: если другой воркер успел,
    #                     возвращаем 0 строк → выходим.
    # - story_generating: пайплайн был прерван после CAS. Подхватываем без повторного CAS.
    if batch["status"] not in ("pending", "story_generating"):
        db_log_update(log_id, "Пайплайн уже выполнен — пропуск", "ok")
        return

    if batch["status"] == "pending":
        if not db_claim_batch_status(batch_id, 'pending', 'story_generating'):
            db_log_update(log_id, "Захват батча не удался — пропуск", "cancelled")
            return

    # is_manual: батч создан вручную (story_manual — ручная генерация сюжета,
    # movie_manual — ручная генерация видео, которому нужен сюжет).
    # Для manual-режима: пул, донор и отмена не применяются — нужен живой результат.
    is_manual = batch["type"] in ("story_manual", "movie_manual")
    if is_manual:
        target = "ручной"
    else:
        active_targets = db_get_active_targets()
        tgt = active_targets[0] if active_targets else {}
        target = tgt.get("name") or "adhoc"
    write_log_entry(
        log_id,
        fmt_id_msg("[story] Батч {} — phase=target_resolved, manual={}, target={}", batch_id, is_manual, target),
        level='silent',
    )

    # movie_manual всегда должен запускаться с выбранным story_id из панели «Режиссёр».
    # Генерация/поиск сюжета для него больше не допускаются.
    if batch["type"] == "movie_manual":
        if batch.get("story_id") is None:
            msg = "Ручная генерация видео требует выбранный сюжет (story_id не задан)"
            db_log_update(log_id, msg, "error")
            write_log_entry(log_id, msg, level='error')
            write_log_entry(log_id, fmt_id_msg("[story] Батч {} — {}", batch_id, msg), level='silent')
            raise AppException(batch_id, "story", msg, log_id)
        preset_story_id = str(batch["story_id"])
        db_log_update(
            log_id,
            "Сюжет назначен пользователем — пропуск поиска и генерации",
            "ok",
        )
        _story_title = db_get_story_title(preset_story_id) or preset_story_id
        write_log_entry(log_id, 'Используется заданный сюжет.')
        write_log_entry(
            log_id,
            fmt_id_msg(
                "[story] Батч {} — phase=user_story_assigned, story_id={}, title=«{}», batch → story_ready",
                batch_id,
                preset_story_id,
                _story_title,
            ),
            level='silent',
        )
        db_set_batch_story(batch_id, preset_story_id)
        return

    # Проверка отмены: только для slot/adhoc.
    # Manual-батч запускается пользователем явно — отменять его нет смысла.
    if not is_manual and check_cancelled("story", batch_id, batch, log_id):
        return

    # Режим пула (donor): только для slot/adhoc, когда use_donor включён
    # и story_id ещё не задан.
    # Находим батч-донор с готовым видео → записываем donor_batch_id в data
    # и переводим в story_ready. Видео-пайплайн потом перенесёт готовое видео,
    # минуя генерацию. Ищем только доноров с grade = good.
    # Если пул пуст — ошибка (AI-генерация запрещена).
    if (
        not is_manual
        and snap.use_donor
        and batch.get("story_id") is None
    ):
        batch_data = batch.get("data") or {}
        donor_batch_id = (
            batch_data.get("donor_batch_id")
            if isinstance(batch_data, dict)
            else None
        )

        if donor_batch_id is None:
            db_claim_donor_batch(batch_id, good_only=True)
            batch = db_get_batch_by_id(batch_id)
            if not batch:
                return
            batch_data = batch.get("data") or {}
            donor_batch_id = (
                batch_data.get("donor_batch_id")
                if isinstance(batch_data, dict)
                else None
            )

        if donor_batch_id:
            donor_batch = db_get_batch_by_id(donor_batch_id)
            donor_story_id = (
                str(donor_batch["story_id"])
                if donor_batch and donor_batch.get("story_id")
                else None
            )
            db_set_batch_story_ready_from_donor(
                batch_id, donor_batch_id, donor_story_id
            )
            donor_title = (
                db_get_story_title(donor_story_id) if donor_story_id else None
            )
            db_log_update(
                log_id, "Подобрано видео из пула, генерация сюжета не требуется", "ok"
            )
            write_log_entry(log_id, "Видео подобрано из пула, генерация сюжета не требуется.")
            write_log_entry(
                log_id,
                fmt_id_msg(
                    "[story] Батч {} — phase=donor_selected, donor_batch_id={}{}",
                    batch_id,
                    donor_batch_id,
                    (f", title=«{donor_title}», batch → story_ready" if donor_title else ", batch → story_ready"),
                ),
                level='silent',
            )
            return
        msg = "Пул видео пуст (grade = good) — AI-генерация запрещена"
        write_log_entry(log_id, msg, level="error")
        db_log_update(log_id, msg, "error")
        write_log_entry(
            log_id, fmt_id_msg("[story] Батч {} — {}", batch_id, msg), level='silent'
        )
        raise AppException(batch_id, "story", msg, log_id)

    # Поиск готового сюжета в пуле: только для slot/adhoc (не story_manual).
    # Разрешены только сюжеты с оценкой «good».
    # Нашли → story_ready без AI. Не нашли → ошибка (AI-генерация запрещена).
    if batch["type"] != "story_manual":
        db_log_update(log_id, "Поиск сюжета в пуле.", "running")
        write_log_entry(
            log_id,
            "Используются только сюжеты с оценкой «good».",
        )
        pool_story = db_claim_unused_story_for_batch(batch_id)
        if pool_story:
            pool_story_id = pool_story["id"]
            pool_story_title = pool_story["title"]
            write_log_entry(
                log_id,
                fmt_id_msg(
                    "Найден сюжет из пула: id={}, название=«{}». AI-генерация не запускается.",
                    pool_story_id,
                    pool_story_title,
                ),
            )
            db_log_update(log_id, f"Сюжет из пула: «{pool_story_title}»", "ok")
            write_log_entry(
                log_id,
                fmt_id_msg(
                    "[story] Батч {} — сюжет из пула {}, батч → story_ready",
                    batch_id,
                    pool_story_id,
                ),
                level='silent',
            )
            return
        msg = "Пул сюжетов пуст (grade = good) — AI-генерация запрещена"
        write_log_entry(log_id, msg, level="error")
        db_log_update(log_id, msg, "error")
        write_log_entry(
            log_id, fmt_id_msg("[story] Батч {} — {}", batch_id, msg), level='silent'
        )
        raise AppException(batch_id, "story", msg, log_id)

    write_log_entry(log_id, 'Начало генерации сюжета.')
    write_log_entry(
        log_id,
        fmt_id_msg(
            "[story] Батч {} ({}) — начало генерации сюжета", batch_id, target
        ),
        level='silent',
    )

    db_log_update(log_id, "Генерация сюжета.", "running")

    if not client_is_configured('text'):
        msg = "API-ключ текстовой платформы не задан — генерация невозможна"
        db_log_update(log_id, msg, "error")
        write_log_entry(log_id, msg, level="error")
        write_log_entry(log_id, f"[story] {msg}", level='silent')
        raise AppException(batch_id, "story", msg, log_id)

    batch_data = batch.get("data") or {}
    is_story_manual = batch["type"] == "story_manual"
    # pinned_model_id (из batches.data.story_model_id): пользователь задал
    # конкретную текстовую модель для story_manual.
    # Только для story_manual — для slot/adhoc story_model_id не пишется,
    # поэтому на практике pinned_model_id бывает только при is_story_manual.
    pinned_model_id = (
        batch_data.get("story_model_id") if isinstance(batch_data, dict) else None
    )

    # Выбор набора моделей:
    # - story_manual + pinned: список из одной модели, один проход.
    # - всё остальное: перебор всех активных текстовых моделей.
    if is_story_manual and pinned_model_id:
        manual_model = db_get_text_model_by_id(pinned_model_id)
        models = [manual_model] if manual_model else []
    else:
        models = db_get_active_text_models()

    if not models:
        msg = "Нет активных text-моделей в ai_models"
        db_log_update(log_id, msg, "error")
        write_log_entry(log_id, msg, level="error")
        write_log_entry(log_id, f"[story] {msg}", level='silent')
        raise AppException(batch_id, "story", msg, log_id)

    try:
        max_attempts_per_model = max(1, int(settings_get("story_fails_to_next", "3")))
    except (ValueError, TypeError):
        max_attempts_per_model = 3

    format_prompt = cycle_config_get("format_prompt")
    user_prompt = cycle_config_get("text_prompt")

    user_prompt = apply_prompt_params(user_prompt)
    format_prompt = apply_prompt_params(format_prompt)

    write_log_entry(
        log_id, f"Моделей: {len(models)}, попыток на модель: {max_attempts_per_model}", level='silent'
    )
    write_log_entry(
        log_id,
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
            log_id,
            fmt_id_msg(
                "[story] Батч {} — phase=model_callback_enter, model={}, attempt={}",
                batch_id, model_name, cnt + 1,
            ),
            level='silent',
        )
        if cnt == 0:
            write_log_entry(log_id, f"Модель: {model_name}")
            write_log_entry(
                log_id, f"[story] Запрос к текстовой платформе: модель={model_name}", level='silent'
            )
        raw = text_client.generate(log_id, model_name, m, format_prompt, user_prompt)
        if raw:
            first_line = raw.split("\n")[0]
            if "." not in first_line:
                title = " ".join(first_line.split()[:4]).rstrip(".")
                text = raw[len(first_line) :].strip()
            else:
                title = " ".join(raw.split()[:4]).rstrip(".")
                text = raw
            sid = db_create_story(m["id"], title, text)
            if sid:
                story_id = sid
                used_model_name = model_name
                used_model_id = m["id"]
                write_log_entry(
                    log_id,
                    fmt_id_msg(
                        "[story] Батч {} — phase=model_callback_success, model={}, story_id={}",
                        batch_id, model_name, sid,
                    ),
                    level='silent',
                )
                return (sid, title, text)
            write_log_entry(
                log_id, f"[{model_name}] не удалось сохранить сюжет", level="warn"
            )
            write_log_entry(
                log_id,
                fmt_id_msg(
                    "[story] Батч {} — phase=story_save_failed, model={}, attempt={}",
                    batch_id, model_name, cnt + 1,
                ),
                level='silent',
            )
        else:
            write_log_entry(
                log_id,
                f"[{model_name}] попытка {attempt_counters[model_name]}/{max_attempts_per_model} не удалась",
                level="warn",
            )
            write_log_entry(
                log_id,
                fmt_id_msg(
                    "[story] Батч {} — phase=model_callback_no_result, model={}, attempt={}",
                    batch_id, model_name, attempt_counters[model_name],
                ),
                level='silent',
            )
        return None

    max_passes = 1 if pinned_model_id else snap.max_model_passes
    write_log_entry(
        log_id,
        fmt_id_msg("[story] Батч {} — phase=iterate_start, max_passes={}", batch_id, max_passes),
        level='silent',
    )
    iterate_result = iterate_models(
        models, max_attempts_per_model, story_callback, max_passes=max_passes
    )

    if not iterate_result:
        if is_story_manual:
            msg = f"Модель не ответила после {max_attempts_per_model} попыток — ручной сюжет не получен"
            db_log_update(log_id, msg, "error")
            write_log_entry(log_id, msg, level='error')
            write_log_entry(log_id, f"[story] {msg}", level='silent')
            raise AppException(batch_id, "story", msg, log_id)
        msg = f"Все активные модели не дали результата после {max_passes} проходов"
        db_log_update(log_id, msg, "error")
        write_log_entry(log_id, msg, level="error")
        write_log_entry(log_id, f"[story] {msg}", level='silent')
        write_log_entry(
            log_id,
            fmt_id_msg("[story] Батч {} — phase=iterate_failed, max_passes={}", batch_id, max_passes),
            level='silent',
        )
        raise AppException(batch_id, "story", msg, log_id)

    story_id, title, result = iterate_result

    write_log_entry(log_id, f"Название: {title}", level='silent')
    write_log_entry(log_id, f"Сюжет:\n{result}", level='silent')
    write_log_entry(
        log_id,
        f"[story] Сюжет получен: {result[:100]}{'.' if len(result) > 100 else ''}",
        level='silent',
    )

    if is_story_manual:
        db_finalize_story_manual(batch_id, story_id)
        db_set_story_model(story_id, used_model_id)
        msg = f"Сюжет сгенерирован ({used_model_name})"
        db_log_update(log_id, msg, "ok")
        write_log_entry(
            log_id,
            fmt_id_msg("Сохранён как story {}, батч → story_manual", story_id),
        )
        write_log_entry(
            log_id,
            fmt_id_msg(
                "[story] Ручной сюжет: story_id={}, batch → story_manual", story_id
            ),
            level='silent',
        )
        write_log_entry(
            log_id,
            fmt_id_msg("[story] Батч {} — phase=run_done, status=story_manual, story_id={}", batch_id, story_id),
            level='silent',
        )
    else:
        if not db_set_batch_story(batch_id, story_id):
            msg = (
                "Ошибка сохранения статуса батча (db_set_batch_story вернул False)"
            )
            db_log_update(log_id, msg, "error")
            write_log_entry(log_id, msg, level="error")
            write_log_entry(log_id, f"[story] {msg}", level='silent')
            raise AppException(batch_id, "story", msg, log_id)
        db_set_story_model(story_id, used_model_id)
        msg = f"Сюжет сгенерирован ({used_model_name})"
        db_log_update(log_id, msg, "ok")
        write_log_entry(
            log_id,
            fmt_id_msg("Сохранён как story {}, батч → story_ready", story_id),
        )
        write_log_entry(
            log_id,
            fmt_id_msg(
                "[story] Готово: story_id={}, batch → story_ready", story_id
            ),
            level='silent',
        )
        write_log_entry(
            log_id,
            fmt_id_msg("[story] Батч {} — phase=run_done, status=story_ready, story_id={}", batch_id, story_id),
            level='silent',
        )
