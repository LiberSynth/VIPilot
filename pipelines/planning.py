"""
Pipeline 1 — Планирование.
Анализирует расписание и активные таргеты,
создаёт недостающие батчи на горизонт buffer_hours вперёд.
"""

from datetime import datetime, timezone, timedelta

from db import db_get, db_get_schedule, db_get_active_targets, db_ensure_batch
from log import db_log_pipeline, db_log_entry
from utils.utils import parse_hhmm
from utils.consts import MSK


def run():
    try:
        schedule = db_get_schedule()
        targets  = db_get_active_targets()

        if not schedule or not targets:
            return

        buffer_hours      = int(db_get('buffer_hours', '24'))
        now               = datetime.now(timezone.utc)
        window_end        = now + timedelta(hours=buffer_hours)
        catchup_threshold = now - timedelta(hours=buffer_hours)
        days_to_check     = int(buffer_hours / 24) + 2

        created = 0
        for day_offset in range(days_to_check):
            day = (now + timedelta(days=day_offset)).date()
            for slot in schedule:
                h, m = parse_hhmm(slot['time_utc'])
                dt = datetime(day.year, day.month, day.day, h, m, tzinfo=timezone.utc)
                in_future_window = now <= dt < window_end
                is_catchup = (
                    dt < now
                    and slot.get('created_at') is not None
                    and slot['created_at'] < catchup_threshold
                )
                if in_future_window or is_catchup:
                    for target in targets:
                        batch_id = db_ensure_batch(dt, target['id'])
                        if batch_id:
                            created += 1
                            log_id = db_log_pipeline(
                                'planning',
                                'Батч запланирован',
                                status='ok',
                                batch_id=batch_id,
                            )
                            if log_id:
                                dt_msk = dt.astimezone(MSK)
                                db_log_entry(log_id, f"Запланирована публикация: {dt_msk.strftime('%d.%m.%Y %H:%M')} МСК")
                                db_log_entry(log_id, f"Таргет: {target['name']}  ({target['aspect_ratio_x']}:{target['aspect_ratio_y']})")
                                db_log_entry(log_id, f"Горизонт планирования: {buffer_hours} ч")
                            print(
                                f"[planning] Создан батч: {dt.strftime('%d.%m %H:%M')} UTC"
                                f" / {target['name']}"
                            )

        if created:
            print(f"[planning] Итого создано батчей: {created}")

    except Exception as e:
        db_log_pipeline('planning', f"Сбой пайплайна: {e}", status='error')
        print(f"[planning] Ошибка: {e}")
