import threading
import time

from utils.runtime_bootstrap import ensure_required_software, ensure_single_instance, run_foreground
ensure_single_instance()
ensure_required_software()

from common.startup import (
    create_app,
    init_app,
    register_shutdown_hooks,
)
from db import (
    db_get_actionable_batches,
    db_create_transcode_batches,
    db_create_publish_batches,
    db_set_batch_status,
)
from common.exceptions import AppException
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
            _t = time.monotonic()

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
            _work_sec = time.monotonic() - _t
            if _work_sec > 5:
                write_log_entry(
                    None,
                    'main',
                    f'Рабочая фаза главного цикла заняла {_work_sec:.1f} с — дольше порога 5 с',
                    level='info',
                )

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
        environment.wait_paused()

def start_application():
    init_app()
    environment.init()

    flask_app = create_app()
    register_middleware(flask_app)
    register_blueprints(flask_app)
    register_shutdown_hooks()

    _db_upgrade.check_upgrade()
    recover_interrupted_batches()

    write_log_entry(None, 'main', 'Приложение запущено', level='info')

    threading.Thread(target=main_loop, daemon=True).start()
    return flask_app

flask_app = start_application()
app = flask_app
run_foreground(flask_app, __name__)
