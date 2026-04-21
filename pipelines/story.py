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
    db_get,
    db_get_batch_by_id,
    db_get_active_targets,
    db_claim_batch_status,
    db_get_active_text_models,
    db_get_text_model_by_id,
    db_create_story,
    db_set_batch_story,
    db_set_batch_story_probe,
    db_claim_donor_batch,
    db_set_batch_story_ready_from_donor,
    db_claim_unused_story_for_batch,
    db_get_story_title,
)
from log import db_log_update, write_log_entry
from pipelines.base import check_cancelled, iterate_models
from common.exceptions import AppException
from clients import text_client
from utils.utils import fmt_id_msg


def run(batch_id, log_id):
    snap = environment.snapshot()
    batch = db_get_batch_by_id(batch_id)
    if not batch:
        return

    # Два допустимых входных статуса:
    # - pending:          батч только что создан. CAS-переход pending → story_generating
    #                     защищает от двойного подхвата: если другой воркер успел,
    #                     возвращаем 0 строк → выходим.
    # - story_generating: пайплайн был прерван после CAS. Подхватываем без повторного CAS.
    if batch["status"] not in ("pending", "story_generating"):
        return

    if batch["status"] == "pending":
        if not db_claim_batch_status(batch_id, 'pending', 'story_generating'):
            return

    # is_probe: батч создан вручную (story_probe — тест текстовой модели,
    # movie_probe — тест видеомодели, которому нужен сюжет).
    # Для probe: пул, донор и отмена не применяются — нужен живой результат.
    is_probe = batch["type"] in ("story_probe", "movie_probe")
    if is_probe:
        target = "пробный"
    else:
        active_targets = db_get_active_targets()
        tgt = active_targets[0] if active_targets else {}
        target = tgt.get("name") or "adhoc"

    # movie_probe с уже заданным story_id: пользователь прикрепил конкретный сюжет
    # к пробному запуску видеомодели. Генерация сюжета не нужна — сразу story_ready.
    if batch.get("story_id") is not None and batch["type"] == "movie_probe":
        preset_story_id = str(batch["story_id"])
        db_log_update(
            log_id,
            "Сюжет назначен пользователем — пропуск поиска и генерации",
            "ok",
        )
        _story_title = db_get_story_title(preset_story_id) or preset_story_id
        write_log_entry(
            log_id, fmt_id_msg('Используется заданный сюжет "{}"', _story_title)
        )
        write_log_entry(
            log_id,
            fmt_id_msg(
                "[story] Батч {} — story_id назначен пользователем ({}), батч → story_ready",
                batch_id,
                preset_story_id,
            ),
        )
        db_set_batch_story(batch_id, preset_story_id)
        return

    # Проверка отмены: только для slot/adhoc.
    # Probe запускается пользователем явно — отменять его нет смысла.
    if not is_probe and check_cancelled("story", batch_id, batch, log_id):
        return

    # Режим пула (donor): только для slot/adhoc, только если эмуляция выключена,
    # use_donor включён и story_id ещё не задан.
    # Находим батч-донор с готовым видео → записываем donor_batch_id в data
    # и переводим в story_ready. Видео-пайплайн потом перенесёт готовое видео,
    # минуя генерацию. Если подходящего донора нет — падаем в AI-генерацию.
    # approve_movies: если включено, ищем только доноров с grade = good;
    # если пул пуст — ошибка (AI-генерация запрещена).
    approve_movies = db_get("approve_movies", "0") == "1"
    if (
        not is_probe
        and not snap.emulation_mode
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
            db_claim_donor_batch(batch_id, good_only=approve_movies)
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
            if donor_title:
                detail = f"Включен режим «Подбирать видео из пула». Контент будет подобран из пула. Сюжет: «{donor_title}»"
            else:
                detail = "Включен режим «Подбирать видео из пула». Контент будет подобран из пула."
            db_log_update(
                log_id, "Подобрано видео из пула, генерация сюжета не требуется", "ok"
            )
            write_log_entry(log_id, detail)
            write_log_entry(
                log_id,
                fmt_id_msg(
                    "[story] Батч {} — подобрано видео из пула {}, батч → story_ready",
                    batch_id,
                    donor_batch_id,
                ),
            )
            return
        else:
            if approve_movies:
                msg = "Пул видео пуст (grade = good) — AI-генерация запрещена (approve_movies включён)"
                write_log_entry(log_id, msg, level="error")
                db_log_update(log_id, msg, "error")
                write_log_entry(
                    log_id, fmt_id_msg("[story] Батч {} — {}", batch_id, msg)
                )
                raise AppException(batch_id, "story", msg, log_id)

    # Поиск готового сюжета в пуле: только для slot/adhoc (не story_probe).
    # Если approve_stories включён — берём только grade=good (одобренные вручную).
    # Нашли → story_ready без AI. Не нашли → идём в AI-генерацию.
    # Если approve_stories включён и пул пуст — ошибка (AI-генерация запрещена).
    if batch["type"] != "story_probe":
        approve_stories = db_get("approve_stories", "0") == "1"
        grade_required = approve_stories
        condition_label = (
            "grade = good" if grade_required else "любой grade (включая NULL)"
        )
        db_log_update(log_id, "Поиск сюжета в пуле…", "running")
        write_log_entry(
            log_id,
            f"Настройка «Утверждать сюжеты»: {'включена' if approve_stories else 'выключена'}. "
            f"Условие выборки: {condition_label}.",
        )
        pool_story = db_claim_unused_story_for_batch(batch_id, grade_required)
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
            )
            return
        else:
            if approve_stories:
                msg = "Пул сюжетов пуст (grade = good) — AI-генерация запрещена (approve_stories включён)"
                write_log_entry(log_id, msg, level="error")
                db_log_update(log_id, msg, "error")
                write_log_entry(
                    log_id, fmt_id_msg("[story] Батч {} — {}", batch_id, msg)
                )
                raise AppException(batch_id, "story", msg, log_id)
            reason = (
                f"Подходящий сюжет в пуле не найден (условие: {condition_label}). "
                f"Переход к AI-генерации."
            )
            write_log_entry(log_id, reason)
            db_log_update(
                log_id,
                "Сюжет в пуле не найден — запускается AI-генерация",
                "running",
            )
            write_log_entry(
                log_id, fmt_id_msg("[story] Батч {} — {}", batch_id, reason)
            )

    write_log_entry(
        log_id,
        fmt_id_msg(
            "[story] Батч {} ({}) — начало генерации сюжета", batch_id, target
        ),
    )

    db_log_update(log_id, "Генерация сюжета…", "running")

    if not text_client.is_configured():
        msg = "API-ключ текстовой платформы не задан — генерация невозможна"
        db_log_update(log_id, msg, "error")
        write_log_entry(log_id, msg, level="error")
        write_log_entry(log_id, f"[story] {msg}")
        raise AppException(batch_id, "story", msg, log_id)

    batch_data = batch.get("data") or {}
    is_story_probe = batch["type"] == "story_probe"
    # pinned_model_id (из batches.data.story_model_id): пользователь задал
    # конкретную текстовую модель для story_probe.
    # Только для story_probe — для slot/adhoc story_model_id не пишется,
    # поэтому на практике pinned_model_id бывает только при is_story_probe.
    pinned_model_id = (
        batch_data.get("story_model_id") if isinstance(batch_data, dict) else None
    )

    # Выбор набора моделей:
    # - story_probe + pinned: список из одной модели, один проход.
    # - всё остальное: перебор всех активных текстовых моделей.
    if is_story_probe and pinned_model_id:
        probe_model = db_get_text_model_by_id(pinned_model_id)
        models = [probe_model] if probe_model else []
    else:
        models = db_get_active_text_models()

    if not models:
        msg = "Нет активных text-моделей в ai_models"
        db_log_update(log_id, msg, "error")
        write_log_entry(log_id, msg, level="error")
        write_log_entry(log_id, f"[story] {msg}")
        raise AppException(batch_id, "story", msg, log_id)

    try:
        fails_to_next = max(1, int(db_get("story_fails_to_next", "3")))
    except (ValueError, TypeError):
        fails_to_next = 3

    format_prompt = db_get("format_prompt", "")
    user_prompt = db_get("text_prompt", "")

    user_prompt = apply_prompt_params(user_prompt)
    format_prompt = apply_prompt_params(format_prompt)

    write_log_entry(
        log_id, f"Моделей: {len(models)}, попыток на модель: {fails_to_next}"
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
        if cnt == 0:
            write_log_entry(log_id, f"Модель: {model_name}")
            write_log_entry(
                log_id, f"[story] Запрос к текстовой платформе: модель={model_name}"
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
                return (sid, title, text)
            write_log_entry(
                log_id, f"[{model_name}] не удалось сохранить сюжет", level="warn"
            )
        else:
            write_log_entry(
                log_id,
                f"[{model_name}] попытка {attempt_counters[model_name]}/{fails_to_next} не удалась",
                level="warn",
            )
        return None

    max_passes = 1 if pinned_model_id else snap.max_model_passes
    iterate_result = iterate_models(
        models, fails_to_next, story_callback, max_passes=max_passes
    )

    if not iterate_result:
        if is_story_probe:
            msg = f"Модель не ответила после {fails_to_next} попыток — пробный сюжет не получен"
            db_log_update(log_id, msg, "error")
            write_log_entry(log_id, msg, level='error')
            write_log_entry(log_id, f"[story] {msg}")
            raise AppException(batch_id, "story", msg, log_id)
        msg = f"Все активные модели не дали результата после {max_passes} проходов"
        db_log_update(log_id, msg, "error")
        write_log_entry(log_id, msg, level="error")
        write_log_entry(log_id, f"[story] {msg}")
        raise AppException(batch_id, "story", msg, log_id)

    story_id, title, result = iterate_result

    write_log_entry(
        log_id,
        f"[story] Сюжет получен: {result[:100]}{'…' if len(result) > 100 else ''}",
    )
    write_log_entry(log_id, f"Название: {title}")
    write_log_entry(log_id, f"Сюжет:\n{result}")

    if is_story_probe:
        db_set_batch_story_probe(batch_id, story_id)
        db_set_story_model(story_id, used_model_id)
        msg = f"Сюжет сгенерирован ({used_model_name})"
        db_log_update(log_id, msg, "ok")
        write_log_entry(
            log_id,
            fmt_id_msg("Сохранён как story {}, батч → story_probe", story_id),
        )
        write_log_entry(
            log_id,
            fmt_id_msg(
                "[story] Пробный сюжет: story_id={}, batch → story_probe", story_id
            ),
        )
    else:
        if not db_set_batch_story(batch_id, story_id):
            msg = (
                "Ошибка сохранения статуса батча (db_set_batch_story вернул False)"
            )
            db_log_update(log_id, msg, "error")
            write_log_entry(log_id, msg, level="error")
            write_log_entry(log_id, f"[story] {msg}")
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
        )
