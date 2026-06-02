import atexit
import os
import pathlib
import platform
import signal
import threading

from utils.pkg_bootstrap import ensure_all_packages
ensure_all_packages()

if pathlib.Path(".env").exists():
    from dotenv import load_dotenv
    load_dotenv()

from utils.consts import PLAYWRIGHT_BROWSERS_PATH
os.environ.setdefault('PLAYWRIGHT_BROWSERS_PATH', PLAYWRIGHT_BROWSERS_PATH)

from db import (
    db_get_actionable_batches,
    db_create_transcode_batches,
    db_create_publish_batches,
    db_set_batch_status,
    db_interrupt_stale_logs,
)
from common.exceptions import AppException
from common.startup import init_app, create_app
from log import app_log, log_app_started, log_app_stopped, write_log_entry
from pipelines import cleanup
import pipelines.planning as planning
from pipelines.dispatch import prepare_batch_dispatch
from pipelines.routing import get_pipeline
from pipelines.runner import start_batch_thread
import common.environment as environment
from utils.middleware import register_middleware
import db.upgrade as _db_upgrade

flask_app = create_app()
register_middleware(flask_app)


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


_main_loop_started = False
_instance_lock_file = None


def _pid_is_running(pid: int) -> bool:
    if pid <= 0:
        return False
    if platform.system() == "Windows":
        import ctypes
        synchronize = 0x00100000
        handle = ctypes.windll.kernel32.OpenProcess(synchronize, False, pid)
        if handle:
            ctypes.windll.kernel32.CloseHandle(handle)
            return True
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _read_lock_pid(lock_path: pathlib.Path) -> int | None:
    try:
        line = lock_path.read_text(encoding="utf-8").strip().splitlines()[0].strip()
        return int(line) if line else None
    except (OSError, ValueError, IndexError):
        return None


def _try_instance_file_lock(fh) -> None:
    if platform.system() == "Windows":
        import msvcrt
        fh.seek(0)
        msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
    else:
        import fcntl
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def _write_lock_pid(fh, pid: int) -> None:
    fh.seek(0)
    fh.truncate()
    fh.write(f"{pid}\n")
    fh.flush()


def _ensure_single_instance():
    """Не даёт запустить второй процесс с тем же main loop (общий _system_log_id в памяти)."""
    global _instance_lock_file
    import sys
    import tempfile

    lock_path = pathlib.Path(
        os.environ.get("VIPILOT_LOCK_FILE", pathlib.Path(tempfile.gettempdir()) / "vipilot.lock")
    )

    for attempt in range(2):
        fh = open(lock_path, "a+", encoding="utf-8")
        try:
            _try_instance_file_lock(fh)
        except (OSError, BlockingIOError):
            fh.close()
            other_pid = _read_lock_pid(lock_path)
            if other_pid and _pid_is_running(other_pid):
                print(
                    f"VIPilot уже запущен (PID {other_pid}). "
                    f"Завершите процесс или lock: {lock_path}"
                )
                sys.exit(1)
            if attempt == 0:
                try:
                    lock_path.unlink(missing_ok=True)
                except OSError:
                    pass
                continue
            print(
                "Не удалось получить блокировку VIPilot. "
                f"Завершите другой процесс Python или удалите lock: {lock_path}"
            )
            sys.exit(1)
        else:
            _write_lock_pid(fh, os.getpid())
            _instance_lock_file = fh
            return


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


def start_main_loop():
    global _main_loop_started
    if not _main_loop_started:
        _main_loop_started = True
        _db_upgrade.check_upgrade() if hasattr(_db_upgrade, 'check_upgrade') else _db_upgrade.runcheck_upgrade()
        init_app(flask_app)
        environment.init_from_db()
        db_interrupt_stale_logs()
        log_app_started()
        _install_shutdown_hooks()
        atexit.register(_on_exit)
        t = threading.Thread(target=main_loop, daemon=True)
        t.start()


_ensure_single_instance()
start_main_loop()

app = flask_app

if __name__ == "__main__":
    if platform.system() == "Windows":
        from waitress import serve
        serve(flask_app, host="0.0.0.0", port=5000)
    else:
        flask_app.run(host="0.0.0.0", port=5000, debug=False)
