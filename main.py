import atexit
import platform
import signal
import threading

from utils.runtime_bootstrap import ensure_required_software, ensure_single_instance
ensure_required_software()

from common.startup import init_app
init_app()
ensure_single_instance()

from db import (
    db_get_actionable_batches,
    db_create_transcode_batches,
    db_create_publish_batches,
    db_set_batch_status,
)
from common.exceptions import AppException
from common.startup import create_app
import log.log as log_state
from log import app_log, log_app_stopped, write_log_entry
from pipelines import cleanup
from pipelines.recovery import recover_interrupted_batches
import pipelines.planning as planning
from pipelines.dispatch import prepare_batch_dispatch
from pipelines.routing import get_pipeline
from pipelines.runner import start_batch_thread
import common.environment as environment
from routes.register import register_blueprints
from utils.middleware import register_middleware
import db.upgrade as _db_upgrade


def main_loop():
    while True:
        try:
            environment.wait_if_paused()
            environment.refresh_environment()

            planning.tick()
            db_create_transcode_batches()
            db_create_publish_batches()

            if environment.get_active_threads() < environment.max_threads:
                batches = db_get_actionable_batches()
                for b in batches:
                    if environment.get_active_threads() >= environment.max_threads:
                        break
                    bid    = str(b['id'])
                    btype  = b['type']
                    status = b['status']
                    pipeline_module = get_pipeline(btype, status)
                    if pipeline_module is None:
                        continue
                    if not environment.claim_batch(bid):
                        continue
                    if not prepare_batch_dispatch(bid, btype, status):
                        environment.release_batch(bid)
                        continue
                    pipeline_name = getattr(pipeline_module, '__name__', str(pipeline_module)).split('.')[-1]
                    start_batch_thread(bid, pipeline_module, pipeline_name)

            cleanup.tick()

        except AppException as e:
            if e.batch_id:
                db_set_batch_status(e.batch_id, 'error')
            write_log_entry(
                e.batch_id,
                e.pipeline,
                f"[main_loop] AppException ({e.pipeline}): {e.message}",
                level='error',
            )
        except Exception as e:
            app_log("main_loop", f"Ошибка: {e}", level="error")

        environment.wait_for_wakeup(environment.loop_interval)


def _on_exit():
    log_app_stopped()


def _install_shutdown_hooks():
    def _handler(signum, _frame):
        log_app_stopped()
        if signum == signal.SIGINT:
            # Preserve standard Ctrl+C behavior in dev console.
            signal.default_int_handler(signum, _frame)
        raise SystemExit(0)

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, _handler)
        except (ValueError, OSError):
            pass
    if hasattr(signal, "SIGBREAK"):
        try:
            signal.signal(signal.SIGBREAK, _handler)
        except (ValueError, OSError):
            pass


flask_app = create_app()
register_middleware(flask_app)
register_blueprints(flask_app)

_db_upgrade.check_upgrade()
recover_interrupted_batches()
environment.init()

with log_state._system_log_lock:
    log_state._lifecycle_stop_logged = False
    log_state._system_log_id = None
app_log("main", "Приложение запущено", level="info")

_install_shutdown_hooks()
atexit.register(_on_exit)
threading.Thread(target=main_loop, daemon=True).start()

app = flask_app

if __name__ == "__main__":
    if platform.system() == "Windows":
        from waitress import serve
        serve(flask_app, host="0.0.0.0", port=5000)
    else:
        flask_app.run(host="0.0.0.0", port=5000, debug=False)
