"""
Pipeline 4 — Транскодирование.
Берёт первый video_ready-батч, скачивает исходник, транскодирует в H.264 / AAC / MP4
и сохраняет результат в базу данных (bytea). Статус: video_ready → transcode_ready / transcode_error.
"""

import os
import subprocess
import tempfile

import requests

from utils.notify import notify_failure
from db import (
    env_get,
    db_get_video_ready_batch,
    db_is_batch_scheduled,
    db_set_batch_obsolete,
    db_set_batch_transcode_ready,
    db_set_batch_transcode_error,
    db_get_random_video_data,
)
from log import db_log_pipeline, db_log_entry, db_log_update, db_log_interrupt_running


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
        if log_id:
            db_log_entry(log_id, f'Таймаут ffmpeg ({timeout_sec // 60} мин)', level='error')
        return False
    except Exception as e:
        if log_id:
            db_log_entry(log_id, f'ffmpeg исключение: {e}', level='error')
        return False

    if returncode != 0:
        err = stderr_bytes.decode(errors='replace')[-600:]
        if log_id:
            db_log_entry(log_id, f'ffmpeg код {returncode}: {err}', level='error')
        return False
    return True


def run():
    batch_id = None
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
            db_log_pipeline('transcode', 'Батч отменён — слот удалён из расписания или таргет отключён',
                            status='прервана', batch_id=batch_id)
            print(f"[transcode] Батч {batch_id[:8]}… отменён, пропускаю")
            return

        print(f"[transcode] Батч {batch_id[:8]}… ({target}) — начало транскодирования")

        # ── Режим эмуляции ──────────────────────────────────────────────────
        if env_get("emulation_mode", "0") == "1":
            log_id = db_log_pipeline(
                'transcode', 'Транскод [эмуляция]',
                status='running', batch_id=batch_id,
            )
            sample = db_get_random_video_data()
            if sample is None:
                msg = '[эмуляция] Нет видео в пуле — невозможно эмулировать транскод'
                db_log_update(log_id, msg, 'error')
                if log_id:
                    db_log_entry(log_id, msg, level='error')
                db_set_batch_transcode_error(batch_id)
                print(f"[transcode] {msg}")
                return
            if log_id:
                db_log_entry(log_id, '[эмуляция] Скачивание и ffmpeg пропущены')
                db_log_entry(log_id, '[эмуляция] Взято случайное видео из пула')
            db_set_batch_transcode_ready(batch_id, sample)
            db_log_update(log_id, 'Транскод [эмуляция]', 'ok')
            print(f"[transcode] Батч {batch_id[:8]}… — эмуляция транскода завершена")
            return

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
        db_log_pipeline('transcode', f'Сбой пайплайна: {e}', status='error',
                        batch_id=batch_id)
        print(f"[transcode] Ошибка: {e}")
        notify_failure(f"сбой transcode-пайплайна: {e}")
