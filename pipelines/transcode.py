"""
Pipeline 4 — Транскодирование.
Принимает batch_id, переводит батч video_ready → transcoding (для восстановления
после краша), транскодирует видео в H.264 / AAC / MP4.

Если транскодирование выключено для таргета — батч тихо переводится в
transcode_ready без единой записи в лог.

Если включено:
  - Отсутствие оригинала в БД → fatal_error (ошибка логики приложения).
  - Любой другой сбой (ffmpeg и т.п.) → некритично: лог-предупреждение,
    батч всё равно переходит в transcode_ready с пустым movies.transcoded_data
    (пайплайн публикации возьмёт оригинал как запасной вариант).

Статус: video_ready → transcoding → transcode_ready / transcode_error.
"""

import os
import subprocess
import tempfile

from utils.notify import notify_failure
from db import (
    db_get,
    db_get_batch_by_id,
    db_get_active_targets,
    db_set_batch_transcoding_by_id,
    db_get_batch_original_video,
    db_set_batch_transcode_skip,
    db_set_batch_transcode_ready,
    db_set_batch_transcode_error,
)
from log import db_log_pipeline, db_log_entry, db_log_update
from pipelines.base import check_cancelled, handle_critical_error


def _probe_duration(src):
    """Возвращает длительность видео в секундах через ffprobe, или None при ошибке."""
    try:
        result = subprocess.run(
            [
                'ffprobe', '-v', 'error',
                '-select_streams', 'v:0',
                '-show_entries', 'format=duration',
                '-of', 'default=noprint_wrappers=1:nokey=1',
                src,
            ],
            capture_output=True,
            timeout=30,
        )
        if result.returncode == 0:
            return float(result.stdout.decode().strip())
    except Exception:
        pass
    return None


def _ffmpeg(src, dst, log_id):
    duration = _probe_duration(src)

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
        '-movflags', '+faststart',
    ]

    if duration is not None:
        cmd += ['-t', str(duration)]
    else:
        cmd += ['-shortest']

    cmd.append(dst)

    timeout_sec = 300
    proc = None
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        _, stderr_bytes = proc.communicate(timeout=timeout_sec)
        returncode = proc.returncode
    except subprocess.TimeoutExpired:
        if proc:
            proc.kill()
            proc.communicate()
        msg = f'Таймаут ffmpeg ({timeout_sec // 60} мин)'
        if log_id:
            db_log_entry(log_id, msg, level='warn')
        print(f"[transcode] {msg}")
        return False
    except Exception as e:
        msg = f'ffmpeg исключение: {e}'
        if log_id:
            db_log_entry(log_id, msg, level='warn')
        print(f"[transcode] {msg}")
        return False

    if returncode != 0:
        err = stderr_bytes.decode(errors='replace')[-600:]
        if log_id:
            db_log_entry(log_id, f'ffmpeg код {returncode}: {err}', level='warn')
        print(f"[transcode] ffmpeg завершился с кодом {returncode}. Хвост stderr:\n{err}")
        return False
    return True


def run(batch_id):
    log_id = None
    try:
        batch = db_get_batch_by_id(batch_id)
        if not batch:
            return

        if batch['status'] not in ('video_ready', 'transcoding'):
            return

        if batch['status'] == 'video_ready':
            if not db_set_batch_transcoding_by_id(batch_id):
                return

        is_probe = batch['type'] == 'movie_probe'

        if not is_probe and check_cancelled('transcode', batch_id, batch):
            return

        if is_probe:
            target       = 'пробный'
            do_transcode = db_get('vk_transcode', '1') == '1'
        else:
            active_targets = db_get_active_targets()
            tgt          = active_targets[0] if active_targets else {}
            target       = tgt.get('name') or 'adhoc'
            do_transcode = bool(tgt.get('transcode', True))

        if not do_transcode:
            db_set_batch_transcode_skip(batch_id)
            print(f"[transcode] Батч {batch_id[:8]}… ({target}) — транскод отключён, пропускаю")
            return

        print(f"[transcode] Батч {batch_id[:8]}… ({target}) — начало транскодирования")

        log_id = db_log_pipeline(
            'transcode', 'Транскодирование…',
            status='running', batch_id=batch_id,
        )

        original_data = db_get_batch_original_video(batch_id)
        if original_data is None:
            msg = 'movies.raw_data = NULL у батча в статусе video_ready — ошибка логики'
            if log_id:
                db_log_entry(log_id, msg, level='error')
            raise RuntimeError(msg)

        src_mb = round(len(original_data) / 1024 / 1024, 1)
        if log_id:
            db_log_entry(log_id, f'Оригинал получен из БД ({src_mb} МБ), запускаю ffmpeg…')
        print(f"[transcode] Оригинал {src_mb} МБ, запускаю ffmpeg…")

        tmp_src_path = None
        tmp_out_path = None
        try:
            with tempfile.NamedTemporaryFile(suffix='.mp4', delete=False) as tmp_src:
                tmp_src_path = tmp_src.name
                tmp_src.write(original_data)

            with tempfile.NamedTemporaryFile(suffix='.mp4', delete=False) as tmp_out:
                tmp_out_path = tmp_out.name

            ok = _ffmpeg(tmp_src_path, tmp_out_path, log_id)
        finally:
            if tmp_src_path:
                try:
                    os.unlink(tmp_src_path)
                except Exception:
                    pass

        if not ok:
            try:
                os.unlink(tmp_out_path)
            except Exception:
                pass
            msg = 'Ошибка ffmpeg — транскод пропущен, публикация использует оригинал'
            db_log_update(log_id, msg, 'warn')
            if log_id:
                db_log_entry(log_id, msg, level='warn')
            db_set_batch_transcode_skip(batch_id)
            print(f"[transcode] {msg}")
            notify_failure(f"transcode: ffmpeg сбой (некритично) — батч {batch_id[:8]}", partial=True)
            return

        out_mb = round(os.path.getsize(tmp_out_path) / 1024 / 1024, 1)
        with open(tmp_out_path, 'rb') as f:
            video_data = f.read()
        os.unlink(tmp_out_path)

        msg = f'Транскодировано (H.264, {out_mb} МБ)'
        db_log_update(log_id, msg, 'ok')
        if log_id:
            db_log_entry(log_id, msg)
        db_set_batch_transcode_ready(batch_id, video_data)
        print(f"[transcode] Готово: {out_mb} МБ → БД")

    except Exception as e:
        handle_critical_error('transcode', batch_id, log_id, e)
