"""
Pipeline 4 — Транскодирование.
Берёт первый video_ready-батч, скачивает исходник, транскодирует в H.264 / AAC / MP4
и сохраняет результат в базу данных (bytea). Статус: video_ready → transcode_ready / transcode_error.
"""

import os
import subprocess
import tempfile

import requests

from db import (
    db_get_video_ready_batch,
    db_is_batch_scheduled,
    db_set_batch_obsolete,
    db_set_batch_transcode_ready,
    db_set_batch_transcode_error,
)
from log import db_log_pipeline, db_log_entry, db_log_update, db_log_interrupt_running


def _ffmpeg(src, dst, log_id):
    cmd = [
        'ffmpeg', '-y',
        '-i', src,
        '-f', 'lavfi', '-i', 'anullsrc=r=44100:cl=stereo',
        '-c:v', 'libx264',
        '-profile:v', 'baseline',
        '-preset', 'ultrafast',
        '-crf', '26',
        '-pix_fmt', 'yuv420p',
        '-r', '30',
        '-c:a', 'aac',
        '-b:a', '96k',
        '-shortest',
        '-movflags', '+faststart',
        dst,
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, timeout=600)
    except subprocess.TimeoutExpired:
        if log_id:
            db_log_entry(log_id, 'Таймаут ffmpeg (10 мин)', level='error')
        return False

    if result.returncode != 0:
        err = result.stderr.decode(errors='replace')[-600:]
        if log_id:
            db_log_entry(log_id, f'ffmpeg код {result.returncode}: {err}', level='error')
        return False
    return True


def run():
    try:
        db_log_interrupt_running('transcode')

        batch = db_get_video_ready_batch()
        if not batch:
            return

        batch_id  = str(batch['id'])
        target    = batch['target_name']
        video_url = batch['video_url']

        if not db_is_batch_scheduled(batch['scheduled_at'], batch['target_id']):
            db_set_batch_obsolete(batch_id)
            db_log_pipeline('transcode', 'Батч устарел — слот удалён из расписания или таргет отключён',
                            status='прервана', batch_id=batch_id)
            print(f"[transcode] Батч {batch_id[:8]}… устарел, пропускаю")
            return

        print(f"[transcode] Батч {batch_id[:8]}… ({target}) — начало транскодирования")

        log_id = db_log_pipeline(
            'transcode', 'Транскодирование…',
            status='running', batch_id=batch_id,
        )

        if log_id:
            db_log_entry(log_id, f'Скачиваю: {video_url[:80]}…')

        try:
            r = requests.get(video_url, timeout=120, stream=True)
            r.raise_for_status()
        except Exception as e:
            msg = f'Ошибка скачивания видео: {e}'
            db_log_update(log_id, msg, 'error')
            if log_id:
                db_log_entry(log_id, msg, level='error')
            db_set_batch_transcode_error(batch_id)
            print(f"[transcode] {msg}")
            return

        with tempfile.NamedTemporaryFile(suffix='.mp4', delete=False) as tmp_src:
            tmp_src_path = tmp_src.name
            for chunk in r.iter_content(chunk_size=256 * 1024):
                tmp_src.write(chunk)

        src_mb = round(os.path.getsize(tmp_src_path) / 1024 / 1024, 1)
        if log_id:
            db_log_entry(log_id, f'Скачано: {src_mb} МБ, запускаю ffmpeg…')
        print(f"[transcode] Скачано {src_mb} МБ, запускаю ffmpeg…")

        with tempfile.NamedTemporaryFile(suffix='.mp4', delete=False) as tmp_out:
            tmp_out_path = tmp_out.name

        ok = _ffmpeg(tmp_src_path, tmp_out_path, log_id)
        os.unlink(tmp_src_path)

        if not ok:
            try:
                os.unlink(tmp_out_path)
            except Exception:
                pass
            msg = 'Ошибка ffmpeg'
            db_log_update(log_id, msg, 'error')
            db_set_batch_transcode_error(batch_id)
            print(f"[transcode] {msg}")
            return

        out_mb = round(os.path.getsize(tmp_out_path) / 1024 / 1024, 1)

        with open(tmp_out_path, 'rb') as f:
            video_data = f.read()
        os.unlink(tmp_out_path)

        db_set_batch_transcode_ready(batch_id, video_data)
        msg = f'Транскодирование завершено ({out_mb} МБ), сохранено в БД'
        db_log_update(log_id, msg, 'ok')
        if log_id:
            db_log_entry(log_id, msg)
        print(f"[transcode] Готово: {out_mb} МБ → БД")

    except Exception as e:
        db_log_pipeline('transcode', f'Сбой пайплайна: {e}', status='error')
        print(f"[transcode] Ошибка: {e}")
