"""
Pipeline 6 — Сборщик мусора.
Удаляет устаревшие данные согласно 3 настройкам времени жизни:
  entries_lifetime  — подробные записи log_entries
  log_lifetime      — краткие записи log (заголовки)
  batch_lifetime    — батчи (истории-задел не удаляются)

Если любой из параметров равен 0, соответствующая очистка полностью пропускается.
Запускается каждые loop_interval секунд; делает всё за один проход.
"""

import threading

from db import (
    settings_get,
    db_cleanup_log_entries,
    db_cleanup_logs,
    db_cleanup_batches,
)
from log import write_log_entry
from utils.utils import parse_long_lifetime, parse_batch_lifetime

_thread = None

def tick():
    """Запускает поток очистки, если предыдущий уже завершился."""
    global _thread
    if _thread is None or not _thread.is_alive():
        _thread = threading.Thread(target=run, daemon=True)
        _thread.start()

def run():
    try:
        entries_lifetime    = parse_long_lifetime(settings_get("entries_lifetime", "30"), default=30)
        log_lifetime        = parse_long_lifetime(settings_get("log_lifetime", "365"))
        batch_lifetime      = parse_batch_lifetime(settings_get("batch_lifetime", "7"))
        write_log_entry(None, 'cleanup', 'phase=run_start, ' + f'entries_lifetime={entries_lifetime}, log_lifetime={log_lifetime}, batch_lifetime={batch_lifetime}', level='silent')

        summary = []

        if entries_lifetime > 0:
            n = db_cleanup_log_entries(entries_lifetime)
            write_log_entry(None, 'cleanup', 'phase=entries_cleanup_done, ' + f'affected={n}', level='silent')
            if n:
                summary.append(f"log_entries: -{n}")
                write_log_entry(None, 'cleanup', f'Удалено log_entries: {n}', level='silent')

        if log_lifetime > 0:
            n = db_cleanup_logs(log_lifetime)
            write_log_entry(None, 'cleanup', 'phase=logs_cleanup_done, ' + f'affected={n}', level='silent')
            if n:
                summary.append(f"log: -{n}")
                write_log_entry(None, 'cleanup', f'Удалено log-записей: {n}', level='silent')

        if batch_lifetime > 0:
            n = db_cleanup_batches(batch_lifetime)
            write_log_entry(None, 'cleanup', 'phase=batches_cleanup_done, ' + f'affected={n}', level='silent')
            if n:
                summary.append(f"batches: -{n}")
                write_log_entry(None, 'cleanup', f'Удалено батчей: {n}', level='silent')

        if summary:
            write_log_entry(None, 'cleanup', 'Очистка: ' + ', '.join(summary), level='silent')
        write_log_entry(None, 'cleanup', 'phase=run_done, ' + f'had_changes={bool(summary)}', level='silent')

    except Exception as e:
        write_log_entry(None, 'cleanup', f'Необработанная ошибка: {e}', level='error')
        raise
