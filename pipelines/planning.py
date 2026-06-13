from datetime import datetime, timezone, timedelta

import common.environment as environment
from common.exceptions import AppException
from db import (
    settings_get,
    db_get_schedule, db_get_active_targets, db_create_planning_batch, db_get_last_pipeline_run,
    db_get_batch_by_id, db_claim_unused_movie_for_batch,
)
from log import write_log_entry
from utils.consts import MSK
from utils.utils import parse_hhmm, fmt_id_msg

def run(batch_id, category):
    """Stage planning: захватывает видео из пула (pending -> ready)."""
    batch = db_get_batch_by_id(batch_id)
    if not batch:
        return

    status = batch.get("status")
    write_log_entry(
        batch_id, category,
        fmt_id_msg(
            "[planning] Батч {} — phase=run_start, status={}, type={}",
            batch_id, status, batch.get("type"),
        ),
        level='silent',
    )

    if batch.get("type") != "planning":
        msg = f"Неподдерживаемый тип батча для planning-пайплайна: {batch.get('type')}"
        write_log_entry(batch_id, category, msg, level="error")
        raise AppException(batch_id, "planning", msg)

    if status != "pending":
        return

    claimed = db_claim_unused_movie_for_batch(batch_id)
    if not claimed:
        msg = "Пустой пул"
        write_log_entry(batch_id, category, msg, level="error")
        write_log_entry(
            batch_id, category,
            fmt_id_msg("[planning] Батч {} — phase=claim_failed_empty_pool", batch_id),
            level='silent',
        )
        raise AppException(batch_id, "planning", msg)

    source_batch_id = claimed.get("batch_id_source")
    source_label = source_batch_id or "NULL"
    write_log_entry(batch_id, category, fmt_id_msg("Захвачено видео {} из пула.", claimed["movie_id"]))
    write_log_entry(batch_id, category, fmt_id_msg("Передающий батч: {}", source_label))
    write_log_entry(
        batch_id, category,
        fmt_id_msg(
            "[planning] Батч {} — phase=run_done, status=ready, movie_id={}, source_batch_id={}",
            batch_id, claimed["movie_id"], source_label,
        ),
        level='silent',
    )

def tick():
    """Планирование расписания: создаёт planning-батчи в горизонте."""
    write_log_entry(None, 'planning', 'phase=run_start', level='silent')

    schedule = db_get_schedule()
    targets  = db_get_active_targets()
    write_log_entry(None, 'planning', 'phase=data_loaded, ' + f'schedule_count={len(schedule)}, targets_count={len(targets)}', level='silent')

    if not schedule or not targets:
        write_log_entry(None, 'planning', 'phase=run_done, reason=no_schedule_or_targets', level='silent')
        return

    try:
        buffer_minutes = int(settings_get('buffer_minutes', '60'))
    except (ValueError, TypeError):
        buffer_minutes = 60

    now           = datetime.now(timezone.utc)
    effective_now = now + timedelta(seconds=environment.loop_interval)
    window_end    = effective_now + timedelta(minutes=buffer_minutes)
    A             = db_get_last_pipeline_run('planning', scheduled_only=True)
    write_log_entry(None, 'planning', 'phase=window_built, ' + f'now={now.isoformat()}, effective_now={effective_now.isoformat()}, window_end={window_end.isoformat()}, last_run={(A.isoformat() if A else None)}, buffer_minutes={buffer_minutes}', level='silent')

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
                elif dt < A + timedelta(minutes=buffer_minutes):
                    is_catchup = True
                else:
                    created_at = slot.get('created_at')
                    is_catchup = (created_at is not None and created_at <= dt)

            if in_future_window or is_catchup:
                batch_id = db_create_planning_batch(dt)
                write_log_entry(None, 'planning', 'phase=slot_evaluated, ' + f'dt={dt.isoformat()}, in_future_window={in_future_window}, is_catchup={is_catchup}, batch_created={bool(batch_id)}', level='silent')
                if batch_id:
                    dt_msk = dt.astimezone(MSK)
                    target_names = ', '.join(t['name'] for t in targets)
                    write_log_entry(batch_id, 'planning', 'Батч запланирован')
                    write_log_entry(
                        batch_id, 'planning',
                        f"Запланирована публикация: {dt_msk.strftime('%d.%m.%Y %H:%M')} МСК",
                    )
                    write_log_entry(batch_id, 'planning', f"Таргеты: {target_names}")
                    write_log_entry(
                        batch_id, 'planning',
                        f"Горизонт планирования: {buffer_minutes} мин",
                    )
                    write_log_entry(
                        batch_id, 'planning',
                        fmt_id_msg("[planning] Создан planning-батч: {} UTC", dt.strftime('%d.%m %H:%M')),
                        level='silent',
                    )
    write_log_entry(None, 'planning', 'phase=run_done, result=ok', level='silent')
