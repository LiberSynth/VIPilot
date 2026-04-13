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

from db import (
    db_get,
    db_cleanup_log_entries,
    db_cleanup_logs,
    db_cleanup_batches,
    db_cleanup_video_data,
)
from log import db_log_pipeline
from utils.utils import parse_entries_lifetime, parse_log_lifetime, parse_batch_lifetime, parse_file_lifetime
from pipelines.base import pipeline_log


def run():
    try:
        entries_lifetime    = parse_entries_lifetime(db_get("entries_lifetime", "30"))
        log_lifetime        = parse_log_lifetime(db_get("log_lifetime", "365"))
        batch_lifetime      = parse_batch_lifetime(db_get("batch_lifetime", "7"))
        file_lifetime       = parse_file_lifetime(db_get("file_lifetime", "7"))

        summary = []

        if entries_lifetime > 0:
            n = db_cleanup_log_entries(entries_lifetime)
            if n:
                summary.append(f"log_entries: -{n}")
                pipeline_log(None, f"[cleanup] Удалено log_entries: {n}")

        if log_lifetime > 0:
            n = db_cleanup_logs(log_lifetime)
            if n:
                summary.append(f"log: -{n}")
                pipeline_log(None, f"[cleanup] Удалено log-записей: {n}")

        if batch_lifetime > 0:
            n = db_cleanup_batches(batch_lifetime)
            if n:
                summary.append(f"batches: -{n}")
                pipeline_log(None, f"[cleanup] Удалено батчей: {n}")

        if file_lifetime > 0:
            n = db_cleanup_video_data(file_lifetime)
            if n:
                summary.append(f"movies.video_data: -{n}")
                pipeline_log(None, f"[cleanup] Обнулено movies.raw_data/transcoded_data: {n}")

        if summary:
            db_log_pipeline("cleanup", "Очистка: " + ", ".join(summary), status="ok")

    except Exception as e:
        pipeline_log(None, f"[cleanup] Необработанная ошибка: {e}")
        raise
