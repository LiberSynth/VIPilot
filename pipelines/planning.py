"""
Pipeline 1 — Планирование.
Анализирует расписание и активные таргеты,
создаёт недостающие батчи на горизонт buffer_hours вперёд.
"""

from datetime import datetime, timezone, timedelta

from db import db_get, db_get_schedule, db_get_active_targets, db_ensure_batch


def _parse_hhmm(s):
    h, m = map(int, s.split(':'))
    return h, m


def run():
    schedule = db_get_schedule()
    targets  = db_get_active_targets()

    if not schedule or not targets:
        return

    buffer_hours = int(db_get('buffer_hours', '24'))
    now          = datetime.now(timezone.utc)
    window_end   = now + timedelta(hours=buffer_hours)
    days_to_check = int(buffer_hours / 24) + 2

    created = 0
    for day_offset in range(days_to_check):
        day = (now + timedelta(days=day_offset)).date()
        for slot in schedule:
            h, m = _parse_hhmm(slot['time_utc'])
            dt = datetime(day.year, day.month, day.day, h, m, tzinfo=timezone.utc)
            if now <= dt < window_end:
                for target in targets:
                    if db_ensure_batch(dt, target['id']):
                        created += 1
                        print(
                            f"[planning] Создан батч: {dt.strftime('%d.%m %H:%M')} UTC "
                            f"/ {target['name']}"
                        )

    if created:
        print(f"[planning] Итого создано батчей: {created}")
