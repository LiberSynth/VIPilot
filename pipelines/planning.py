from datetime import datetime, timezone, timedelta

import common.environment as environment
from common.exceptions import AppException
from db import (
    settings_get,
    db_cancel_orphaned_planning_batches,
    db_get_schedule, db_get_active_targets, db_create_planning_batch, db_get_last_pipeline_run,
    db_get_batch_by_id, db_claim_unused_movie_for_batch,
)
from log import db_log_update, write_log, log_batch_planned, write_log_entry
from utils.consts import MSK
from utils.utils import parse_hhmm, fmt_id_msg


def run(batch_id, log_id):
    """Stage planning: захватывает видео из пула (pending -> ready)."""
    batch = db_get_batch_by_id(batch_id)
    if not batch:
        db_log_update(log_id, "Батч не найден", "error")
        return

    status = batch.get("status")
    write_log_entry(
        log_id,
        fmt_id_msg(
            "[planning] Батч {} — phase=run_start, status={}, type={}",
            batch_id, status, batch.get("type"),
        ),
        level='silent',
    )

    if batch.get("type") != "planning":
        msg = f"Неподдерживаемый тип батча для planning-пайплайна: {batch.get('type')}"
        db_log_update(log_id, msg, "error")
        write_log_entry(log_id, msg, level="error")
        raise AppException(batch_id, "planning", msg, log_id)

    if status != "pending":
        db_log_update(log_id, "Пайплайн уже выполнен — пропуск", "ok")
        return

    claimed = db_claim_unused_movie_for_batch(batch_id)
    if not claimed:
        msg = "Пустой пул"
        db_log_update(log_id, msg, "error")
        write_log_entry(log_id, msg, level="error")
        write_log_entry(
            log_id,
            fmt_id_msg("[planning] Батч {} — phase=claim_failed_empty_pool", batch_id),
            level='silent',
        )
        raise AppException(batch_id, "planning", msg, log_id)

    source_batch_id = claimed.get("batch_id_source")
    source_label = source_batch_id or "NULL"
    db_log_update(log_id, "Видео захвачено из пула", "ok")
    write_log_entry(log_id, fmt_id_msg("Захвачено видео {} из пула.", claimed["movie_id"]))
    write_log_entry(log_id, fmt_id_msg("Передающий батч: {}", source_label))
    write_log_entry(
        log_id,
        fmt_id_msg(
            "[planning] Батч {} — phase=run_done, status=ready, movie_id={}, source_batch_id={}",
            batch_id, claimed["movie_id"], source_label,
        ),
        level='silent',
    )


def tick():
    """Планирование расписания: создаёт planning-батчи в горизонте."""
    log_id = None
    try:
        write_log_entry(None, "[planning] phase=run_start", level='silent')
        cancelled = db_cancel_orphaned_planning_batches()
        write_log_entry(None, f"[planning] phase=orphan_cleanup, cancelled_count={len(cancelled)}", level='silent')
        for bid in cancelled:
            log_id = write_log(
                'publish',
                'Батч отменён — слот удалён из расписания',
                status='cancelled',
                batch_id=bid,
            )
            write_log_entry(log_id, fmt_id_msg("[planning] Батч {} отменён (слот расписания исчез)", bid), level='silent')

        schedule = db_get_schedule()
        targets  = db_get_active_targets()
        write_log_entry(None, f"[planning] phase=data_loaded, schedule_count={len(schedule)}, targets_count={len(targets)}", level='silent')

        if not schedule or not targets:
            write_log_entry(None, "[planning] phase=run_done, reason=no_schedule_or_targets", level='silent')
            return

        try:
            buffer_hours = int(settings_get('buffer_hours', '24'))
        except (ValueError, TypeError):
            buffer_hours = 24

        now           = datetime.now(timezone.utc)
        effective_now = now + timedelta(seconds=environment.loop_interval)
        window_end    = effective_now + timedelta(hours=buffer_hours)
        A             = db_get_last_pipeline_run('planning', scheduled_only=True)
        write_log_entry(
            None,
            f"[planning] phase=window_built, now={now.isoformat()}, effective_now={effective_now.isoformat()}, window_end={window_end.isoformat()}, last_run={(A.isoformat() if A else None)}, buffer_hours={buffer_hours}",
            level='silent',
        )

        for day_offset in range(-1, 2):
            day = (now + timedelta(days=day_offset)).date()
            for slot in schedule:
                h, m = parse_hhmm(slot['time_utc'])
                dt = datetime(day.year, day.month, day.day, h, m, tzinfo=timezone.utc)

                in_future_window = effective_now <= dt < window_end

                is_catchup = False
                if dt < now:
                    if A is None:
                        is_catchup = False
                    elif dt < A:
                        is_catchup = False
                    elif dt < A + timedelta(hours=buffer_hours):
                        is_catchup = True
                    else:
                        created_at = slot.get('created_at')
                        is_catchup = (created_at is not None and created_at <= dt)

                if in_future_window or is_catchup:
                    batch_id = db_create_planning_batch(dt)
                    write_log_entry(
                        None,
                        f"[planning] phase=slot_evaluated, dt={dt.isoformat()}, in_future_window={in_future_window}, is_catchup={is_catchup}, batch_created={bool(batch_id)}",
                        level='silent',
                    )
                    if batch_id:
                        dt_msk = dt.astimezone(MSK)
                        target_names = ', '.join(t['name'] for t in targets)
                        log_id = log_batch_planned(
                            batch_id,
                            'Батч запланирован',
                            f"Запланирована публикация: {dt_msk.strftime('%d.%m.%Y %H:%M')} МСК",
                            f"Таргеты: {target_names}",
                            f"Горизонт планирования: {buffer_hours} ч",
                        )
                        write_log_entry(
                            log_id,
                            fmt_id_msg("[planning] Создан planning-батч: {} UTC", dt.strftime('%d.%m %H:%M')),
                            level='silent',
                        )
        write_log_entry(None, "[planning] phase=run_done, result=ok", level='silent')

    except Exception as e:
        write_log_entry(log_id, f"[planning] Необработанная ошибка: {e}", level='silent')
        raise
