import atexit
import threading

from db import (
    db_get_actionable_batches,
    db_set_batch_status,
)
from common.exceptions import AppException
from common.startup import init_app, create_app
from log import write_log_entry
from pipelines import publish, cleanup
import pipelines.planning as planning
from pipelines.routing import get_pipeline
from pipelines.runner import start_batch_thread
import common.environment as environment
import utils.keepalive as keepalive
from utils.middleware import register_middleware
from db.upgrade import check_upgrade

flask_app = create_app()
register_middleware(flask_app)


def main_loop():
    while True:
        try:
            environment.wait_if_paused()
            environment.refresh_environment()

            planning.run()

            if environment.get_active_threads() < environment.max_threads:
                batches = db_get_actionable_batches()
                for b in batches:
                    if environment.get_active_threads() >= environment.max_threads:
                        break
                    bid    = str(b['id'])
                    status = b['status']
                    pipeline_module = get_pipeline(status)
                    if pipeline_module is None:
                        continue
                    if pipeline_module is publish and publish.is_scheduled(b):
                        continue
                    if not environment.claim_batch(bid):
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

        environment.wait_for_wakeup(environment.loop_interval)


_main_loop_started = False


def _on_exit():
    write_log_entry(None, "[main] Приложение остановлено", level='info')


def start_main_loop():
    global _main_loop_started
    if not _main_loop_started:
        _main_loop_started = True
        check_upgrade()
        init_app(flask_app)
        environment.init_from_db()
        write_log_entry(None, "[main] Приложение запущено", level='info')
        atexit.register(_on_exit)
        t = threading.Thread(target=main_loop, daemon=True)
        t.start()
        ka = threading.Thread(target=keepalive.loop, daemon=True)
        ka.start()


start_main_loop()

app = flask_app

if __name__ == "__main__":
    flask_app.run(host="0.0.0.0", port=5000, debug=False)
