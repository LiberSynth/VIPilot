from datetime import datetime, timezone, timedelta

from db import (
    db_get,
    db_get_schedule, db_get_active_targets, db_ensure_batch, db_get_last_pipeline_run,
)
from log import db_log_pipeline
from utils.consts import MSK
from utils.utils import parse_hhmm
from pipelines.base import pipeline_log, handle_critical_error


def run(loop_interval: int):
    """Планирование: проверяет расписание и создаёт недостающие батчи."""
    try:
        schedule = db_get_schedule()
        targets  = db_get_active_targets()

        if not schedule or not targets:
            return

        try:
            buffer_hours = int(db_get('buffer_hours', '24'))
        except (ValueError, TypeError):
            buffer_hours = 24

        now           = datetime.now(timezone.utc)
        effective_now = now + timedelta(seconds=loop_interval)
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
                        log_id = db_log_pipeline(
                            'planning',
                            'Батч запланирован',
                            status='ok',
                            batch_id=batch_id,
                        )
                        if log_id:
                            dt_msk = dt.astimezone(MSK)
                            pipeline_log(log_id, f"Запланирована публикация: {dt_msk.strftime('%d.%m.%Y %H:%M')} МСК")
                            target_names = ', '.join(t['name'] for t in targets)
                            pipeline_log(log_id, f"Таргеты: {target_names}")
                            pipeline_log(log_id, f"Горизонт планирования: {buffer_hours} ч")
                        pipeline_log(None, f"[planning] Создан батч: {dt.strftime('%d.%m %H:%M')} UTC")

    except Exception as e:
        handle_critical_error('planning', None, None, e)
