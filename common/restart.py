"""Отложенный перезапуск процесса приложения из UI."""

from __future__ import annotations

import os
import platform
import subprocess
import sys
import threading
import time

_RESPONSE_DELAY_SEC = 2.0
_SPAWN_AFTER_EXIT_SEC = 3.0
_DEFAULT_WINDOWS_SERVICE = "VIPilotService"
_WIN_DETACHED = subprocess.DETACHED_PROCESS | subprocess.CREATE_NO_WINDOW


def _looks_like_windows_service() -> bool:
    session = os.environ.get("SESSIONNAME", "").strip().upper()
    return session == "SERVICES" or os.environ.get("VIPILOT_AS_SERVICE") == "1"


def _windows_service_name() -> str | None:
    explicit = (os.environ.get("VIPILOT_SERVICE_NAME") or "").strip()
    if explicit:
        return explicit
    if _looks_like_windows_service():
        return _DEFAULT_WINDOWS_SERVICE
    return None


def _spawn_detached_python(code: str) -> None:
    subprocess.Popen(
        [sys.executable, "-c", code],
        creationflags=_WIN_DETACHED,
        close_fds=True,
    )


def _restart_via_windows_service(service_name: str) -> None:
    code = f"""
import subprocess
import time

svc = {service_name!r}
time.sleep({_RESPONSE_DELAY_SEC})
subprocess.run(["sc", "stop", svc], check=False)
for _ in range(60):
    r = subprocess.run(
        ["sc", "query", svc],
        capture_output=True,
        text=True,
        errors="replace",
    )
    out = (r.stdout or "") + (r.stderr or "")
    if "STOPPED" in out.upper():
        break
    time.sleep(1)
subprocess.run(["sc", "start", svc], check=False)
"""
    _spawn_detached_python(code)


def _restart_via_windows_spawn() -> None:
    cwd = os.getcwd()
    cmd = [sys.executable] + list(sys.argv)
    code = f"""
import subprocess
import time

time.sleep({_SPAWN_AFTER_EXIT_SEC})
flags = subprocess.DETACHED_PROCESS | subprocess.CREATE_NO_WINDOW
subprocess.Popen(
    {cmd!r},
    creationflags=flags,
    close_fds=True,
    cwd={cwd!r},
)
"""
    _spawn_detached_python(code)
    time.sleep(_RESPONSE_DELAY_SEC)
    import log.log as log_state
    from log import write_log_entry

    with log_state._system_log_lock:
        already = log_state._lifecycle_stop_logged
        if not already:
            log_state._lifecycle_stop_logged = True
    if not already:
        write_log_entry(None, "main", "Приложение остановлено", level="info")
    os._exit(0)


def _restart_via_execv() -> None:
    time.sleep(_RESPONSE_DELAY_SEC)
    import log.log as log_state
    from log import write_log_entry

    with log_state._system_log_lock:
        already = log_state._lifecycle_stop_logged
        if not already:
            log_state._lifecycle_stop_logged = True
    if not already:
        write_log_entry(None, "main", "Приложение остановлено", level="info")
    try:
        os.closerange(3, 4096)
    except Exception:
        pass
    os.execv(sys.executable, [sys.executable] + sys.argv)


def _do_restart() -> None:
    if platform.system() == "Windows":
        service_name = _windows_service_name()
        if service_name:
            _restart_via_windows_service(service_name)
            return
        _restart_via_windows_spawn()
        return
    _restart_via_execv()


def schedule_app_restart() -> None:
    threading.Thread(target=_do_restart, daemon=True).start()
