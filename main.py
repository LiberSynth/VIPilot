import atexit
import os
import pathlib
import platform
import sys
import threading

_APP_ROOT = pathlib.Path(__file__).resolve().parent
_ENV_FILE = _APP_ROOT / ".env"


def _unquote_env_value(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def _load_env_file_early(env_path: pathlib.Path) -> None:
    """Загружает .env через stdlib, чтобы bootstrap видел переменные и под NSSM."""
    if not env_path.exists():
        return
    try:
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            if key.startswith("export "):
                key = key[7:].strip()
            if not key:
                continue
            os.environ.setdefault(key, _unquote_env_value(value.strip()))
    except Exception:
        # Ошибка чтения .env не должна ломать запуск; fallback на окружение процесса.
        return


_load_env_file_early(_ENV_FILE)

from utils.pkg_bootstrap import ensure_all_packages, drain_bootstrap_events
ensure_all_packages()

if _ENV_FILE.exists():
    try:
        from dotenv import load_dotenv
        load_dotenv(dotenv_path=_ENV_FILE, override=False)
    except Exception:
        pass

from utils.consts import PLAYWRIGHT_BROWSERS_PATH
os.environ.setdefault('PLAYWRIGHT_BROWSERS_PATH', PLAYWRIGHT_BROWSERS_PATH)

from db import (
    db_get_actionable_batches,
    db_set_batch_status,
    db_interrupt_stale_logs,
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
import db.upgrade as _db_upgrade

flask_app = create_app()
register_middleware(flask_app)


def _flush_bootstrap_events_to_log():
    events = drain_bootstrap_events()
    write_log_entry(None, "[main] Стартовый setup окружения завершён.", level='info')
    write_log_entry(None, f"[main] startup_setup: events={len(events)}", level='silent')
    for level, message in events:
        try:
            write_log_entry(None, message, level=level)
        except Exception:
            # Лог bootstrap вспомогательный: не останавливаем запуск приложения.
            continue


def main_loop():
    global _runtime_started_logged
    while True:
        try:
            environment.wait_if_paused()
            environment.refresh_environment()
            if not _runtime_started_logged:
                _runtime_started_logged = True
                write_log_entry(None, "[main] Служба запущена и вошла в рабочий цикл.", level='info')
                write_log_entry(
                    None,
                    f"[main] runtime_start: pid={os.getpid()}, loop_interval={environment.loop_interval}, max_threads={environment.max_threads}",
                    level='silent',
                )

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
_runtime_started_logged = False


def _on_exit():
    write_log_entry(None, "[main] Приложение остановлено", level='info')
    write_log_entry(None, f"[main] shutdown: pid={os.getpid()}", level='silent')


def start_main_loop():
    global _main_loop_started
    if not _main_loop_started:
        _main_loop_started = True
        write_log_entry(None, "[main] Старт службы: вход в инициализацию.", level='info')
        write_log_entry(
            None,
            f"[main] startup_entry: pid={os.getpid()}, cwd={os.getcwd()}, argv={' '.join(sys.argv)}",
            level='silent',
        )
        try:
            _db_upgrade.check_upgrade() if hasattr(_db_upgrade, 'check_upgrade') else _db_upgrade.runcheck_upgrade()
            init_app(flask_app)
            environment.init_from_db()
            environment.refresh_environment()
            _flush_bootstrap_events_to_log()
            db_interrupt_stale_logs()
        except Exception as e:
            write_log_entry(None, "[main] Ошибка инициализации приложения.", level='error')
            write_log_entry(
                None,
                f"[main] startup_init_failed: type={type(e).__name__}, detail={e}",
                level='silent',
            )
            raise
        write_log_entry(None, "[main] Приложение запущено", level='info')
        write_log_entry(
            None,
            f"[main] startup: pid={os.getpid()}, platform={platform.system()}, loop_interval={environment.loop_interval}, max_threads={environment.max_threads}",
            level='silent',
        )
        atexit.register(_on_exit)
        t = threading.Thread(target=main_loop, daemon=True)
        t.start()
        ka = threading.Thread(target=keepalive.loop, daemon=True)
        ka.start()


start_main_loop()

app = flask_app

if __name__ == "__main__":
    if platform.system() == "Windows":
        from waitress import serve
        serve(flask_app, host="0.0.0.0", port=5000)
    else:
        flask_app.run(host="0.0.0.0", port=5000, debug=False)
