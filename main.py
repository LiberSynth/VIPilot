import platform
import threading

from utils.runtime_bootstrap import ensure_required_software, ensure_single_instance
ensure_required_software()

from common.startup import init_app, register_shutdown_hooks
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
from log import write_log_entry
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
            write_log_entry(None, 'main_loop', 'phase=tick_start', level='info')
            environment.wait_if_paused()
            write_log_entry(None, 'main_loop', 'phase=after_pause', level='info')
            environment.refresh_environment()
            write_log_entry(None, 'main_loop', 'phase=after_refresh', level='info')

            planning.tick()
            write_log_entry(None, 'main_loop', 'phase=after_planning', level='info')
            db_create_transcode_batches()
            write_log_entry(None, 'main_loop', 'phase=after_transcode_create', level='info')
            db_create_publish_batches()
            write_log_entry(None, 'main_loop', 'phase=after_publish_create', level='info')

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

            write_log_entry(None, 'main_loop', 'phase=after_dispatch', level='info')
            cleanup.tick()
            write_log_entry(None, 'main_loop', 'phase=after_cleanup', level='info')

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
            write_log_entry(None, 'main_loop', f'Ошибка: {e}', level='error')

        environment.wait_for_wakeup(environment.loop_interval)


flask_app = create_app()
register_middleware(flask_app)
register_blueprints(flask_app)

_db_upgrade.check_upgrade()
recover_interrupted_batches()
environment.init()

with log_state._system_log_lock:
    log_state._lifecycle_stop_logged = False
    log_state._system_log_id = None
write_log_entry(None, 'main', 'Приложение запущено', level='info')

register_shutdown_hooks()
threading.Thread(target=main_loop, daemon=True).start()

app = flask_app

if __name__ == "__main__":
    if platform.system() == "Windows":
        from waitress import serve
        serve(flask_app, host="0.0.0.0", port=5000)
    else:
        flask_app.run(host="0.0.0.0", port=5000, debug=False)
