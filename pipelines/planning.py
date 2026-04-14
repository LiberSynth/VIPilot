from datetime import datetime, timezone, timedelta

import common.environment as environment
from db import (
    db_get,
    db_cancel_waiting_batches,
    db_get_schedule, db_get_active_targets, db_ensure_batch, db_get_last_pipeline_run,
)
from log import write_log, log_batch_planned, write_log_entry
from utils.consts import MSK
from utils.utils import parse_hhmm, fmt_id_msg


def run():
    """Планирование: проверяет расписание и создаёт недостающие батчи."""
    try:
        cancelled = db_cancel_waiting_batches()
        for bid in cancelled:
            write_log(
                'publish',
                'Батч отменён — слот удалён из расписания',
                status='cancelled',
                batch_id=bid,
            )
            write_log_entry(None, fmt_id_msg("[planning] Батч {} отменён (слот расписания исчез)", bid), level='silent')

        schedule = db_get_schedule()
        targets  = db_get_active_targets()

        if not schedule or not targets:
            return

        try:
            buffer_hours = int(db_get('buffer_hours', '24'))
        except (ValueError, TypeError):
            buffer_hours = 24

        now           = datetime.now(timezone.utc)
        effective_now = now + timedelta(seconds=environment.loop_interval)
        window_end    = effective_now + timedelta(hours=buffer_hours)
        A             = db_get_last_pipeline_run('planning')

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
                    batch_id = db_ensure_batch(dt)
                    if batch_id:
                        dt_msk = dt.astimezone(MSK)
                        target_names = ', '.join(t['name'] for t in targets)
                        log_batch_planned(
                            batch_id,
                            'Батч запланирован',
                            f"Запланирована публикация: {dt_msk.strftime('%d.%m.%Y %H:%M')} МСК",
                            f"Таргеты: {target_names}",
                            f"Горизонт планирования: {buffer_hours} ч",
                        )
                        write_log_entry(None, f"[planning] Создан батч: {dt.strftime('%d.%m %H:%M')} UTC")

    except Exception as e:
        write_log_entry(None, f"[planning] Необработанная ошибка: {e}")
        raise
