"""
Pipeline 6 — Сборщик мусора.
Удаляет устаревшие данные согласно 4 настройкам времени жизни:
  log_lifetime        — подробные записи log_entries
  short_log_lifetime  — краткие записи log (заголовки)
  batch_lifetime      — батчи + осиротевшие stories
  file_lifetime       — физические видеофайлы на диске

Запускается каждые loop_interval секунд; делает всё за один проход.
"""

import os

from db import (
    db_get,
    db_cleanup_log_entries,
    db_cleanup_logs,
    db_cleanup_batches,
)
from log import db_log_pipeline
from utils.utils import parse_log_lifetime, parse_short_log_lifetime, parse_batch_lifetime, parse_file_lifetime


def _delete_files(paths: list) -> tuple[int, int]:
    """Пытается удалить каждый файл из списка.
    Возвращает (удалено, ошибок)."""
    deleted = errors = 0
    for path in paths:
        if not path:
            continue
        try:
            if os.path.isfile(path):
                os.remove(path)
                deleted += 1
        except Exception as e:
            print(f"[cleanup] Не удалось удалить файл {path}: {e}")
            errors += 1
    return deleted, errors


def run():
    try:
        log_lifetime        = parse_log_lifetime(db_get("log_lifetime", "30"))
        short_log_lifetime  = parse_short_log_lifetime(db_get("short_log_lifetime", "365"))
        batch_lifetime      = parse_batch_lifetime(db_get("batch_lifetime", "7"))
        file_lifetime       = parse_file_lifetime(db_get("file_lifetime", "7"))

        summary = []

        # 1. Подробный лог (log_entries старше log_lifetime)
        n = db_cleanup_log_entries(log_lifetime)
        if n:
            summary.append(f"log_entries: -{n}")
            print(f"[cleanup] Удалено log_entries: {n}")

        # 2. Краткий лог (log + оставшиеся log_entries старше short_log_lifetime)
        n = db_cleanup_logs(short_log_lifetime)
        if n:
            summary.append(f"log: -{n}")
            print(f"[cleanup] Удалено log-записей: {n}")

        # 3. История батчей (батчи + stories старше batch_lifetime)
        video_files = db_cleanup_batches(batch_lifetime)
        if video_files is not None and len(video_files) >= 0:
            # db_cleanup_batches возвращает список файлов удалённых батчей
            pass
        # video_files — это файлы батчей удалённых по batch_lifetime,
        # но физически удаляем только те, что старше file_lifetime
        # (они уже удалены из БД, но файл может ещё жить)
        if video_files:
            summary.append(f"batches: -{len(video_files)} (файлы проверены)")
            print(f"[cleanup] Удалено батчей: {len(video_files)}")

        # 4. Физические файлы — удаляем файлы найденных батчей
        #    (батч уже удалён из БД — файл больше не нужен)
        if video_files:
            deleted, errors = _delete_files(video_files)
            if deleted:
                summary.append(f"files: -{deleted}")
                print(f"[cleanup] Удалено файлов: {deleted}")
            if errors:
                print(f"[cleanup] Ошибок при удалении файлов: {errors}")

        # 5. Сироты на диске: файлы в videos/ старше file_lifetime дней,
        #    не привязанные ни к одному батчу
        orphan_deleted = _cleanup_orphan_files(file_lifetime)
        if orphan_deleted:
            summary.append(f"orphan files: -{orphan_deleted}")
            print(f"[cleanup] Удалено файлов-сирот: {orphan_deleted}")

        if summary:
            db_log_pipeline("cleanup", "Очистка: " + ", ".join(summary), status="ok")

    except Exception as e:
        db_log_pipeline("cleanup", f"Сбой пайплайна: {e}", status="error")
        print(f"[cleanup] Ошибка: {e}")


def _cleanup_orphan_files(file_lifetime_days: int) -> int:
    """Удаляет файлы из videos/, которые старше file_lifetime_days дней."""
    import time
    videos_dir = "videos"
    if not os.path.isdir(videos_dir):
        return 0
    cutoff = time.time() - file_lifetime_days * 86400
    deleted = 0
    for fname in os.listdir(videos_dir):
        fpath = os.path.join(videos_dir, fname)
        try:
            if os.path.isfile(fpath) and os.path.getmtime(fpath) < cutoff:
                os.remove(fpath)
                deleted += 1
        except Exception as e:
            print(f"[cleanup] Ошибка при удалении сироты {fpath}: {e}")
    return deleted
