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

import common.environment as environment
from utils.notify import notify_failure
from db import (
    cycle_config_get,
    db_get_batch_by_id,
    db_get_active_targets,
    db_claim_batch_status,
    db_get_batch_original_video,
    db_save_transcoded_data,
    db_set_batch_status,
)
from log import db_log_update, write_log_entry
from pipelines.base import check_cancelled
from common.exceptions import FatalError
from utils.utils import fmt_id_msg


def _ffmpeg(src, dst, log_id):
    duration = float(cycle_config_get('video_duration'))

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

    cmd += ['-t', str(duration)]

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
        write_log_entry(log_id, msg, level='warn')
        write_log_entry(log_id, f"[transcode] {msg}", level='silent')
        return False
    except Exception as e:
        msg = f'ffmpeg исключение: {e}'
        write_log_entry(log_id, msg, level='warn')
        write_log_entry(log_id, f"[transcode] {msg}", level='silent')
        return False

    if returncode != 0:
        err = stderr_bytes.decode(errors='replace')[-600:]
        write_log_entry(log_id, f'ffmpeg завершился с кодом {returncode}', level='warn')
        write_log_entry(log_id, f"[transcode] ffmpeg код {returncode}. Хвост stderr:\n{err}", level='silent')
        return False
    return True


def run(batch_id, log_id):
    snap = environment.snapshot()
    batch = db_get_batch_by_id(batch_id)
    if not batch:
        return

    if batch['status'] not in ('video_ready', 'transcoding'):
        return

    if batch['status'] == 'video_ready':
        if not db_claim_batch_status(batch_id, 'video_ready', 'transcoding'):
            return

    is_probe = batch['type'] == 'movie_probe'

    if not is_probe and check_cancelled('transcode', batch_id, batch, log_id):
        return

    if is_probe:
        target       = 'пробный'
        do_transcode = True
    else:
        active_targets = db_get_active_targets()
        tgt          = active_targets[0] if active_targets else {}
        target       = tgt.get('name') or 'adhoc'
        do_transcode = bool(tgt.get('transcode', True))

    if not do_transcode:
        db_set_batch_status(batch_id, 'transcode_ready')
        write_log_entry(log_id, 'Транскод отключён — пропускаю')
        write_log_entry(log_id, fmt_id_msg("[transcode] Батч {} ({}) — транскод отключен, пропускаю", batch_id, target), level='silent')
        return

    write_log_entry(log_id, 'Начало транскодирования.')
    write_log_entry(log_id, fmt_id_msg("[transcode] Батч {} ({}) — начало транскодирования", batch_id, target), level='silent')

    db_log_update(log_id, 'Транскодирование.', 'running')

    original_data = db_get_batch_original_video(batch_id)
    if original_data is None:
        msg = 'movies.raw_data = NULL у батча в статусе video_ready — ошибка логики'
        write_log_entry(log_id, msg, level='error')
        raise FatalError(msg)

    src_mb = round(len(original_data) / 1024 / 1024, 1)
    write_log_entry(log_id, 'Оригинал получен из БД, запускаю ffmpeg.')
    write_log_entry(log_id, f"[transcode] Оригинал {src_mb} МБ, запускаю ffmpeg.", level='silent')

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
            except Exception as e:
                write_log_entry(log_id, f"Не удалось удалить временный файл {tmp_src_path}: {e}", level='warn')

    if not ok:
        try:
            os.unlink(tmp_out_path)
        except Exception as e:
            write_log_entry(log_id, f"Не удалось удалить временный файл {tmp_out_path}: {e}", level='warn')
        msg = 'Ошибка ffmpeg — транскод пропущен, публикация использует оригинал'
        db_log_update(log_id, msg, 'warn')
        write_log_entry(log_id, msg, level='warn')
        db_set_batch_status(batch_id, 'transcode_ready')
        write_log_entry(log_id, f"[transcode] {msg}", level='silent')
        notify_failure(fmt_id_msg("transcode: ffmpeg сбой (некритично) — батч {}", batch_id), partial=True)
        return

    out_mb = round(os.path.getsize(tmp_out_path) / 1024 / 1024, 1)
    with open(tmp_out_path, 'rb') as f:
        video_data = f.read()
    os.unlink(tmp_out_path)

    msg = f'Транскодировано (H.264, {out_mb} МБ)'
    db_log_update(log_id, msg, 'ok')
    write_log_entry(log_id, msg)
    db_save_transcoded_data(batch_id, video_data)
    db_set_batch_status(batch_id, 'transcode_ready')
    write_log_entry(log_id, f"[transcode] Готово: {out_mb} МБ → БД", level='silent')
