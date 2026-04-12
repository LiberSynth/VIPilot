import atexit
import threading
from datetime import datetime, timezone

from db import (
    init_db, run_upgrades, db_get,
    db_get_actionable_batches,
    db_cancel_waiting_batches,
    env_get,
)
from startup import init_app, create_app
from log import db_log_root, db_log_pipeline, db_log_interrupt_running, db_log_fix_orphaned_running
from pipelines import story, video, transcode, publish, cleanup
import pipelines.planning as planning
from statuses import PUBLISH_ROUTING_SUFFIXES
import utils.workflow_state as wf_state
import utils.keepalive as keepalive
from utils.middleware import register_middleware

flask_app = create_app()
register_middleware(flask_app)

_STATUS_TO_PIPELINE = {
    'pending':          story,
    'story_generating': story,
    'story_ready':      video,
    'video_generating': video,
    'video_pending':    video,
    'video_ready':      transcode,
    'transcoding':      transcode,
    'transcode_ready':  publish,
}

def _get_pipeline(status: str):
    """Возвращает модуль пайплайна для данного статуса батча.
    Составные статусы вида slug.method.pending / slug.method.published → publish."""
    pipeline = _STATUS_TO_PIPELINE.get(status)
    if pipeline is not None:
        return pipeline
    if any(status.endswith(sfx) for sfx in PUBLISH_ROUTING_SUFFIXES):
        return publish
    return None


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
                max_threads = max(1, min(32, int(db_get('max_batch_threads', '5'))))
            except (ValueError, TypeError):
                max_threads = 5

            planning.run(interval)

            cancelled = db_cancel_waiting_batches()
            for bid in cancelled:
                db_log_pipeline(
                    'publish',
                    'Батч отменён — слот удалён из расписания',
                    status='cancelled',
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
                    pipeline_module = _get_pipeline(status)
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


def start_main_loop():
    global _main_loop_started
    if not _main_loop_started:
        _main_loop_started = True
        init_db()
        run_upgrades()
        init_app(flask_app)
        wf_state.reset_active_threads()
        if env_get('workflow_state', 'running') == 'pause':
            wf_state.set_paused()
        else:
            wf_state.set_running()
        for _pipeline in ('story', 'video', 'transcode', 'publish'):
            db_log_interrupt_running(_pipeline)
        db_log_fix_orphaned_running(fix=True)
        db_log_root("Приложение запущено", status='info')
        atexit.register(_on_exit)
        t = threading.Thread(target=main_loop, daemon=True)
        t.start()
        ka = threading.Thread(target=keepalive.loop, daemon=True)
        ka.start()


start_main_loop()

app = flask_app

if __name__ == "__main__":
    flask_app.run(host="0.0.0.0", port=5000, debug=False)
