import atexit
import threading

from db import (
    db_get,
    db_get_actionable_batches,
    db_set_batch_status,
)
from common.exceptions import AppException
from common.startup import init_app, create_app
from log import write_log, write_log_entry
from pipelines import publish, cleanup
import pipelines.planning as planning
from pipelines.routing import get_pipeline
from pipelines.runner import start_batch_thread
import utils.workflow_state as wf_state
import utils.keepalive as keepalive
from utils.middleware import register_middleware
from db.upgrade import check_upgrade

flask_app = create_app()
register_middleware(flask_app)


def main_loop():
    while True:
        interval = 5
        try:
            wf_state.wait_if_paused()

            interval = int(db_get('loop_interval'))
            max_threads = max(1, min(32, int(db_get('max_batch_threads'))))

            planning.run()

            if wf_state.get_active_threads() < max_threads:
                batches = db_get_actionable_batches()
                for b in batches:
                    if wf_state.get_active_threads() >= max_threads:
                        break
                    bid    = str(b['id'])
                    status = b['status']
                    pipeline_module = get_pipeline(status)
                    if pipeline_module is None:
                        continue
                    if pipeline_module is publish and publish.is_scheduled(b):
                        continue
                    if not wf_state.claim_batch(bid):
                        continue
                    pipeline_name = getattr(pipeline_module, '__name__', str(pipeline_module)).split('.')[-1]
                    start_batch_thread(bid, pipeline_module, pipeline_name)

            cleanup.tick()

        except AppException as e:
            if e.batch_id:
                db_set_batch_status(e.batch_id, 'error')
            write_log_entry(e.log_id, f"[main_loop] AppException ({e.pipeline}): {e.message}", level='error')
        except Exception as e:
            write_log_entry(None, f"[main_loop] Ошибка: {e}", level='error')

        wf_state.wait_for_wakeup(interval)


_main_loop_started = False


def _on_exit():
    log_id = write_log('root', "Приложение остановлено", status='info')
    write_log_entry(log_id, "[main] Приложение остановлено", level='info')


def start_main_loop():
    global _main_loop_started
    if not _main_loop_started:
        _main_loop_started = True
        check_upgrade()
        init_app(flask_app)
        wf_state.init_from_db()
        write_log('root', "Приложение запущено", status='info')
        atexit.register(_on_exit)
        t = threading.Thread(target=main_loop, daemon=True)
        t.start()
        ka = threading.Thread(target=keepalive.loop, daemon=True)
        ka.start()


start_main_loop()

app = flask_app

if __name__ == "__main__":
    flask_app.run(host="0.0.0.0", port=5000, debug=False)
