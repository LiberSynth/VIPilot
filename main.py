import os
import subprocess
import time
import atexit
import threading
from datetime import datetime, timezone, timedelta
from flask import Flask, request

from db import (
    init_db, run_upgrades, db_get,
    db_get_schedule, db_get_active_targets, db_ensure_batch, db_get_last_pipeline_run,
    db_get_actionable_batches,
    db_get_batches_with_unknown_status,
    db_cancel_waiting_batches,
    KNOWN_BATCH_STATUSES,
    env_get, env_set,
)
from log import db_log_root, db_log_pipeline, db_log_entry, db_log_interrupt_running
from pipelines import story, video, transcode, publish, cleanup
from routes.admin import bp as admin_bp
from routes.api import bp as api_bp
from utils.consts import FLASK_SECRET, MSK
from utils.limiter import limiter
from utils.utils import parse_hhmm
import utils.workflow_state as wf_state

flask_app = Flask(__name__, static_folder="static", static_url_path="/static")
flask_app.secret_key = FLASK_SECRET
flask_app.permanent_session_lifetime = timedelta(days=7)
limiter.init_app(flask_app)

flask_app.register_blueprint(admin_bp)
flask_app.register_blueprint(api_bp)


@flask_app.before_request
def log_request():
    method = request.method
    path = request.full_path if request.query_string else request.path
    remote = request.remote_addr
    headers = dict(request.headers)
    body = ""
    if request.content_length and request.content_length > 0:
        try:
            body = request.get_data(as_text=True)
        except Exception:
            body = "<не удалось прочитать тело>"
    msg = f"[HTTP] {method} {path} | IP: {remote} | Headers: {headers}"
    if body:
        msg += f" | Body: {body}"
    print(msg)


def _keepalive_loop():
    import requests as req
    domain = os.environ.get('REPLIT_DOMAINS', '').split(',')[0].strip()
    if not domain:
        return
    url = f"https://{domain}/healthz"
    print(f"[keepalive] Запущен → {url}")
    while True:
        time.sleep(4 * 60)
        try:
            req.get(url, timeout=10)
        except Exception as e:
            print(f"[keepalive] Ошибка: {e}")


_STATUS_TO_PIPELINE = {
    'pending':          story,
    'story_generating': story,
    'story_ready':      video,
    'video_generating': video,
    'video_pending':    video,
    'video_ready':      transcode,
    'transcoding':      transcode,
    'transcode_ready':  publish,
    'story_posted':     publish,
    'wall_posting':     publish,
}

def _validate_batch_statuses():
    """Проверяет, нет ли батчей с неизвестным статусом. Вызывается при старте."""
    unknown_batches = db_get_batches_with_unknown_status(KNOWN_BATCH_STATUSES)
    if unknown_batches:
        for batch_id, status in unknown_batches.items():
            msg = f"[validate] ВНИМАНИЕ: батч имеет неизвестный статус: {status!r}"
            db_log_pipeline('validate', msg, status='error', batch_id=batch_id)
            print(f"[validate] batch_id={batch_id}: неизвестный статус {status!r}")
    else:
        print("[validate] Все статусы батчей известны.")


def _run_planning(loop_interval: int):
    """Планирование: проверяет расписание и создаёт недостающие батчи."""
    try:
        schedule = db_get_schedule()
        targets  = db_get_active_targets()

        if not schedule or not targets:
            return

        try:
            buffer_hours = int(db_get('buffer_hours', '24'))
        except (ValueError, TypeError):
            buffer_hours = 24

        now           = datetime.now(timezone.utc)
        effective_now = now + timedelta(seconds=loop_interval)
        window_end    = effective_now + timedelta(hours=buffer_hours)
        A             = db_get_last_pipeline_run('planning')

        for day_offset in range(-1, 2):
            day = (now + timedelta(days=day_offset)).date()
            for slot in schedule:
                h, m = parse_hhmm(slot['time_utc'])
                dt = datetime(day.year, day.month, day.day, h, m, tzinfo=timezone.utc)

                in_future_window = effective_now <= dt < window_end

                is_catchup = False
                if dt < now:
                    if A is None:
                        is_catchup = False
                    elif dt < A:
                        is_catchup = False
                    elif dt < A + timedelta(hours=buffer_hours):
                        is_catchup = True
                    else:
                        created_at = slot.get('created_at')
                        is_catchup = (created_at is not None and created_at <= dt)

                if in_future_window or is_catchup:
                    batch_id = db_ensure_batch(dt, targets[0]['id'])
                    if batch_id:
                        log_id = db_log_pipeline(
                            'planning',
                            'Батч запланирован',
                            status='ok',
                            batch_id=batch_id,
                        )
                        if log_id:
                            dt_msk = dt.astimezone(MSK)
                            db_log_entry(log_id, f"Запланирована публикация: {dt_msk.strftime('%d.%m.%Y %H:%M')} МСК")
                            target_names = ', '.join(t['name'] for t in targets)
                            db_log_entry(log_id, f"Таргеты: {target_names}")
                            db_log_entry(log_id, f"Горизонт планирования: {buffer_hours} ч")
                        print(f"[planning] Создан батч: {dt.strftime('%d.%m %H:%M')} UTC")

    except Exception as e:
        db_log_pipeline('planning', f"Сбой планирования: {e}", status='error')
        print(f"[planning] Ошибка: {e}")


def main_loop():
    _cleanup_thread = None

    while True:
        interval = 5
        try:
            wf_state.wait_if_paused()

            try:
                interval = int(db_get('loop_interval', '5'))
            except (ValueError, TypeError):
                interval = 5

            try:
                max_threads = max(1, min(32, int(db_get('max_batch_threads', '2'))))
            except (ValueError, TypeError):
                max_threads = 2

            _run_planning(interval)

            cancelled = db_cancel_waiting_batches()
            for bid in cancelled:
                db_log_pipeline(
                    'publish',
                    'Батч отменён — слот удалён из расписания или таргет отключён',
                    status='прервана',
                    batch_id=bid,
                )
                print(f"[planning] Батч {bid[:8]}… отменён (слот расписания исчез)")

            if wf_state.get_active_threads() < max_threads:
                batches = db_get_actionable_batches()
                for b in batches:
                    if wf_state.get_active_threads() >= max_threads:
                        break
                    bid    = str(b['id'])
                    status = b['status']
                    pipeline_module = _STATUS_TO_PIPELINE.get(status)
                    if pipeline_module is None:
                        continue
                    # Публикация: не запускаем поток пока не наступило время
                    if pipeline_module is publish:
                        sched = b.get('scheduled_at')
                        if sched is not None and sched.tzinfo is None:
                            sched = sched.replace(tzinfo=timezone.utc)
                        if sched is not None and datetime.now(timezone.utc) < sched:
                            continue
                    if not wf_state.claim_batch(bid):
                        continue

                    def _batch_thread(batch_id=bid, pipeline=pipeline_module):
                        try:
                            pipeline.run(batch_id)
                        except Exception as e:
                            print(f"[main_loop] Необработанная ошибка потока {batch_id[:8]}…: {e}")
                        finally:
                            wf_state.release_batch(batch_id)
                            wf_state.wakeup_loop()

                    t = threading.Thread(target=_batch_thread, daemon=True)
                    t.start()

            if _cleanup_thread is None or not _cleanup_thread.is_alive():
                _cleanup_thread = threading.Thread(target=cleanup.run, daemon=True)
                _cleanup_thread.start()

        except Exception as e:
            db_log_root(f"Ошибка главного цикла: {e}", status='error')
            print(f"[main_loop] Ошибка: {e}")

        wf_state.wait_for_wakeup(interval)


_main_loop_started = False


def _on_exit():
    db_log_root("Приложение остановлено", status='info')
    print("[main] Приложение остановлено")


def _check_ffmpeg():
    """Проверяет наличие ffmpeg и выводит версию в консоль при старте."""
    try:
        result = subprocess.run(
            ['ffmpeg', '-version'],
            capture_output=True,
            timeout=10,
        )
        if result.returncode == 0:
            first_line = result.stdout.decode(errors='replace').splitlines()[0]
            print(f"[startup] ffmpeg доступен: {first_line}")
        else:
            err = result.stderr.decode(errors='replace')[:200]
            print(f"[startup] ВНИМАНИЕ: ffmpeg вернул код {result.returncode}: {err}")
    except FileNotFoundError:
        print("[startup] ВНИМАНИЕ: ffmpeg не найден в PATH — транскодирование недоступно")
    except Exception as e:
        print(f"[startup] ВНИМАНИЕ: ffmpeg проверка не удалась: {e}")


def start_main_loop():
    global _main_loop_started
    if not _main_loop_started:
        _main_loop_started = True
        init_db()
        run_upgrades()
        _check_ffmpeg()
        _validate_batch_statuses()
        wf_state.reset_active_threads()
        if env_get('workflow_state', 'running') == 'pause':
            wf_state.set_paused()
        else:
            wf_state.set_running()
        for _pipeline in ('story', 'video', 'transcode', 'publish'):
            db_log_interrupt_running(_pipeline)
        db_log_root("Приложение запущено", status='info')
        atexit.register(_on_exit)
        t = threading.Thread(target=main_loop, daemon=True)
        t.start()
        ka = threading.Thread(target=_keepalive_loop, daemon=True)
        ka.start()


start_main_loop()

app = flask_app

if __name__ == "__main__":
    flask_app.run(host="0.0.0.0", port=5000, debug=False)
