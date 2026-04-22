"""
Pipeline 6 — Сборщик мусора.
Удаляет устаревшие данные согласно 4 настройкам времени жизни:
  entries_lifetime  — подробные записи log_entries
  log_lifetime      — краткие записи log (заголовки)
  batch_lifetime    — батчи (истории-задел не удаляются)
  file_lifetime     — обнуляет raw_data/transcoded_data в movies для опубликованных батчей

Если любой из параметров равен 0, соответствующая очистка полностью пропускается.
Запускается каждые loop_interval секунд; делает всё за один проход.
"""

import threading

from db import (
    settings_get,
    db_cleanup_log_entries,
    db_cleanup_logs,
    db_cleanup_batches,
    db_cleanup_video_data,
)
from log import write_log_entry
from utils.utils import parse_long_lifetime, parse_batch_lifetime, parse_file_lifetime

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
        file_lifetime       = parse_file_lifetime(settings_get("file_lifetime", "7"))

        summary = []

        if entries_lifetime > 0:
            n = db_cleanup_log_entries(entries_lifetime)
            if n:
                summary.append(f"log_entries: -{n}")
                write_log_entry(None, f"[cleanup] Удалено log_entries: {n}")

        if log_lifetime > 0:
            n = db_cleanup_logs(log_lifetime)
            if n:
                summary.append(f"log: -{n}")
                write_log_entry(None, f"[cleanup] Удалено log-записей: {n}")

        if batch_lifetime > 0:
            n = db_cleanup_batches(batch_lifetime)
            if n:
                summary.append(f"batches: -{n}")
                write_log_entry(None, f"[cleanup] Удалено батчей: {n}")

        if file_lifetime > 0:
            n = db_cleanup_video_data(file_lifetime)
            if n:
                summary.append(f"movies.video_data: -{n}")
                write_log_entry(None, f"[cleanup] Обнулено movies.raw_data/transcoded_data: {n}")

        if summary:
            write_log_entry(None, "[cleanup] Очистка: " + ", ".join(summary))

    except Exception as e:
        write_log_entry(None, f"[cleanup] Необработанная ошибка: {e}")
        raise
