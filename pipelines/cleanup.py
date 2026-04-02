"""
Pipeline 6 — Сборщик мусора.
Удаляет устаревшие данные согласно 4 настройкам времени жизни:
  entries_lifetime  — подробные записи log_entries
  log_lifetime      — краткие записи log (заголовки)
  batch_lifetime    — батчи + осиротевшие stories
  file_lifetime     — обнуляет video_data у опубликованных батчей в БД

Запускается каждые loop_interval секунд; делает всё за один проход.
"""

import time

from db import (
    db_get,
    db_cleanup_log_entries,
    db_cleanup_logs,
    db_cleanup_batches,
    db_cleanup_video_data,
)
from log import db_log_pipeline
from utils.utils import parse_entries_lifetime, parse_log_lifetime, parse_batch_lifetime, parse_file_lifetime

_last_idle_log: float = 0
_IDLE_LOG_INTERVAL = 3600


def run():
    try:
        entries_lifetime    = parse_entries_lifetime(db_get("entries_lifetime", "30"))
        log_lifetime        = parse_log_lifetime(db_get("log_lifetime", "365"))
        batch_lifetime      = parse_batch_lifetime(db_get("batch_lifetime", "7"))
        file_lifetime       = parse_file_lifetime(db_get("file_lifetime", "7"))

        summary = []

        n = db_cleanup_log_entries(entries_lifetime)
        if n:
            summary.append(f"log_entries: -{n}")
            print(f"[cleanup] Удалено log_entries: {n}")

        n = db_cleanup_logs(log_lifetime)
        if n:
            summary.append(f"log: -{n}")
            print(f"[cleanup] Удалено log-записей: {n}")

        n = db_cleanup_batches(batch_lifetime)
        if n:
            summary.append(f"batches: -{n}")
            print(f"[cleanup] Удалено батчей: {n}")

        n = db_cleanup_video_data(file_lifetime)
        if n:
            summary.append(f"video_data: -{n}")
            print(f"[cleanup] Обнулено video_data: {n}")

        global _last_idle_log
        if summary:
            db_log_pipeline("cleanup", "Очистка: " + ", ".join(summary), status="ok")
            _last_idle_log = time.time()
        elif time.time() - _last_idle_log >= _IDLE_LOG_INTERVAL:
            db_log_pipeline("cleanup", "Данные в норме", status="ok")
            _last_idle_log = time.time()

    except Exception as e:
        db_log_pipeline("cleanup", f"Сбой пайплайна: {e}", status="error")
        print(f"[cleanup] Ошибка: {e}")
