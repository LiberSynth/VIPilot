"""
Pipeline 4 — Транскодирование.

Stage-batch type=transcode:
  pending    -> processing
  processing -> ready
"""

from pathlib import Path
import subprocess

import common.environment as environment
from db import (
    db_get_batch_by_id,
    db_set_batch_status,
    db_set_movie_transcoded,
)
from log import write_log_entry
from utils.utils import fmt_id_msg

_VIDEO_DIR = Path(__file__).resolve().parents[1] / "video"
_RAW_FIELD = "raw_data"
_TRANSCODED_FIELD = "transcoded_data"


def _movie_video_path(movie_id: str, field: str) -> Path:
    return _VIDEO_DIR / f"{movie_id} - {field}.mp4"


def _ffmpeg(src: Path, dst: Path, category):
    cmd = [
        'ffmpeg', '-y',
        '-i', str(src),
        '-f', 'lavfi', '-i', 'anullsrc=r=44100:cl=stereo',
        '-c:v', 'libx264',
        '-profile:v', 'baseline',
        '-preset', 'ultrafast',
        '-crf', '26',
        '-pix_fmt', 'yuv420p',
        '-r', '30',
        '-c:a', 'aac',
        '-b:a', '96k',
        '-af', 'apad',
        '-shortest',
        '-movflags', '+faststart',
    ]
    cmd.append(str(dst))
    write_log_entry(
        batch_id, category,
        f"[transcode] phase=ffmpeg_start, src={src}, dst={dst}",
        level='silent',
    )
    subprocess.run(cmd, check=True, timeout=300)
    write_log_entry(batch_id, category, "[transcode] phase=ffmpeg_done", level='silent')


def run(batch_id, category):
    _snap = environment.snapshot()
    batch = db_get_batch_by_id(batch_id)
    if not batch:
        raise RuntimeError("Батч не найден")
    write_log_entry(
        batch_id, category,
        fmt_id_msg(
            "[transcode] Батч {} — phase=run_start, status={}, type={}",
            batch_id, batch.get('status'), batch.get('type'),
        ),
        level='silent',
    )

    if batch.get('type') != 'transcode':
        write_log_entry(
            batch_id, category,
            fmt_id_msg("[transcode] Батч {} type={} — шаг пропущен", batch_id, batch.get('type')),
            level='silent',
        )
        return

    if batch['status'] not in ('pending', 'processing'):
        return

    if batch['status'] == 'pending':
        db_set_batch_status(batch_id, 'processing')
        write_log_entry(
            batch_id, category,
            fmt_id_msg("[transcode] Батч {} — phase=status_set, from=pending, to=processing", batch_id),
            level='silent',
        )

    movie_id = batch.get('movie_id')
    if not movie_id:
        raise RuntimeError("У transcode-батча отсутствует movie_id")
    src_path = _movie_video_path(str(movie_id), _RAW_FIELD)
    dst_path = _movie_video_path(str(movie_id), _TRANSCODED_FIELD)
    if not src_path.exists():
        raise RuntimeError("Оригинальный raw-файл не найден у transcode-батча")
    dst_path.parent.mkdir(parents=True, exist_ok=True)

    write_log_entry(batch_id, category, 'Начало транскодирования.')
    _ffmpeg(src_path, dst_path, category)
    out_mb = round(dst_path.stat().st_size / 1024 / 1024, 1)

    msg = f'Транскодировано (H.264, {out_mb} МБ)'
    write_log_entry(batch_id, category, msg)
    db_set_movie_transcoded(str(movie_id))
    db_set_batch_status(batch_id, 'ready')
    write_log_entry(batch_id, category, f"[transcode] Готово: {out_mb} МБ → transcoded-файл", level='silent')
    write_log_entry(
        batch_id, category,
        fmt_id_msg("[transcode] Батч {} — phase=run_done, status=ready, transcoded_mb={}", batch_id, out_mb),
        level='silent',
    )
