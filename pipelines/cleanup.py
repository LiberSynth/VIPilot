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
from log import app_log
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
        app_log(
            "cleanup",
            f"entries_lifetime={entries_lifetime}, log_lifetime={log_lifetime}, batch_lifetime={batch_lifetime}",
            phase="run_start",
        )

        summary = []

        if entries_lifetime > 0:
            n = db_cleanup_log_entries(entries_lifetime)
            app_log("cleanup", f"affected={n}", phase="entries_cleanup_done")
            if n:
                summary.append(f"log_entries: -{n}")
                app_log("cleanup", f"Удалено log_entries: {n}")

        if log_lifetime > 0:
            n = db_cleanup_logs(log_lifetime)
            app_log("cleanup", f"affected={n}", phase="logs_cleanup_done")
            if n:
                summary.append(f"log: -{n}")
                app_log("cleanup", f"Удалено log-записей: {n}")

        if batch_lifetime > 0:
            n = db_cleanup_batches(batch_lifetime)
            app_log("cleanup", f"affected={n}", phase="batches_cleanup_done")
            if n:
                summary.append(f"batches: -{n}")
                app_log("cleanup", f"Удалено батчей: {n}")

        if summary:
            app_log("cleanup", "Очистка: " + ", ".join(summary))
        app_log("cleanup", f"had_changes={bool(summary)}", phase="run_done")

    except Exception as e:
        app_log("cleanup", f"Необработанная ошибка: {e}", level="error")
        raise
