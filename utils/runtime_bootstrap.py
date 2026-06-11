"""
Проверка и установка необходимого софта — только stdlib, без DB-зависимостей.
Вызывается из main.py до любых других импортов.
"""
import importlib.util
import os
import pathlib
import platform
import shutil
import subprocess
import sys

_PG_REPACK_BOOTSTRAP_ERROR = ""
# Официальных win-бинарников у reorg/pg_repack нет (только исходники на GitHub).
_WINDOWS_USES_VACUUM_FALLBACK = True


def _set_pg_repack_bootstrap_error(msg: str | None) -> None:
    global _PG_REPACK_BOOTSTRAP_ERROR
    _PG_REPACK_BOOTSTRAP_ERROR = (msg or "").strip()


def get_pg_repack_bootstrap_error() -> str:
    return _PG_REPACK_BOOTSTRAP_ERROR


def _project_bin_dir() -> pathlib.Path:
    d = pathlib.Path(__file__).resolve().parent.parent / "bin"
    d.mkdir(exist_ok=True)
    return d


def _pip_install(package: str) -> None:
    subprocess.run(
        [sys.executable, "-m", "pip", "install", package],
        check=True,
    )


def _ensure_dotenv() -> None:
    if importlib.util.find_spec("dotenv") is None:
        _pip_install("python-dotenv")
    if pathlib.Path(".env").exists():
        from dotenv import load_dotenv
        load_dotenv()


def _install_ffmpeg_windows() -> None:
    import urllib.request
    import zipfile

    _FFMPEG_URL = (
        "https://github.com/GyanD/codexffmpeg/releases/download/"
        "7.1.1/ffmpeg-7.1.1-essentials_build.zip"
    )
    dest_dir = pathlib.Path(__file__).resolve().parent.parent / "bin"
    dest_dir.mkdir(exist_ok=True)
    ffmpeg_exe = dest_dir / "ffmpeg.exe"

    if not ffmpeg_exe.exists():
        zip_path = dest_dir / "ffmpeg.zip"
        sys.stdout.write(f"[bootstrap] Скачиваю ffmpeg из {_FFMPEG_URL} ...\n")
        sys.stdout.flush()
        urllib.request.urlretrieve(_FFMPEG_URL, zip_path)
        sys.stdout.write("[bootstrap] Распаковываю ffmpeg...\n")
        sys.stdout.flush()
        with zipfile.ZipFile(zip_path, "r") as zf:
            for member in zf.namelist():
                if member.endswith("bin/ffmpeg.exe"):
                    zf.extract(member, dest_dir)
                    extracted = dest_dir / member
                    extracted.rename(ffmpeg_exe)
                    break
        zip_path.unlink(missing_ok=True)
        for d in dest_dir.rglob("*"):
            if d.is_dir() and not any(d.iterdir()):
                d.rmdir()

    os.environ["PATH"] = str(dest_dir) + os.pathsep + os.environ.get("PATH", "")
    sys.stdout.write(f"[bootstrap] ffmpeg установлен: {ffmpeg_exe}\n")
    sys.stdout.flush()


def _prepend_to_path(path: pathlib.Path) -> None:
    os.environ["PATH"] = str(path) + os.pathsep + os.environ.get("PATH", "")


def _iter_windows_postgres_bin_dirs():
    seen: set[str] = set()
    search_roots = [
        pathlib.Path(r"C:\Program Files\PostgreSQL"),
        pathlib.Path(r"C:\Program Files (x86)\PostgreSQL"),
        pathlib.Path(r"C:\PostgreSQL"),
    ]
    for root in search_roots:
        if not root.exists():
            continue
        for candidate in sorted(root.iterdir(), reverse=True):
            bin_dir = candidate / "bin"
            if not bin_dir.exists():
                continue
            key = str(bin_dir).lower()
            if key in seen:
                continue
            seen.add(key)
            yield bin_dir


def _find_pg_repack_windows() -> bool:
    if shutil.which("pg_repack"):
        return True

    local_candidates = [
        pathlib.Path(__file__).resolve().parent.parent / "bin" / "pg_repack.exe",
        pathlib.Path(__file__).resolve().parent.parent / "bin" / "pg_repack" / "pg_repack.exe",
    ]
    for exe in local_candidates:
        if exe.exists():
            _prepend_to_path(exe.parent)
            sys.stdout.write(f"[bootstrap] pg_repack найден: {exe}\n")
            sys.stdout.flush()
            return True

    for bin_dir in _iter_windows_postgres_bin_dirs():
        exe = bin_dir / "pg_repack.exe"
        if exe.exists():
            _prepend_to_path(exe.parent)
            sys.stdout.write(f"[bootstrap] pg_repack найден: {exe}\n")
            sys.stdout.flush()
            return True
    return False


def _run_cmd(args: list[str], timeout: int) -> tuple[bool, str]:
    try:
        r = subprocess.run(args, capture_output=True, timeout=timeout)
        if r.returncode == 0:
            return True, ""
        err = (
            r.stderr.decode(errors="replace").strip()
            or r.stdout.decode(errors="replace").strip()
            or f"код {r.returncode}"
        )
        return False, err
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def _detect_pg_major_from_database_url() -> int | None:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        return None
    try:
        import psycopg2

        with psycopg2.connect(dsn, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute("SHOW server_version_num")
                row = cur.fetchone()
        if not row or not row[0]:
            return None
        ver_num = int(row[0])
        return ver_num // 10000
    except Exception:
        return None


def _install_pg_repack_linux() -> bool:
    if shutil.which("apt-get") is None:
        return shutil.which("pg_repack") is not None

    major = _detect_pg_major_from_database_url()
    candidates = []
    if major:
        candidates.append(f"postgresql-{major}-repack")
    for v in (17, 16, 15, 14, 13, 12):
        pkg = f"postgresql-{v}-repack"
        if pkg not in candidates:
            candidates.append(pkg)

    ok, err = _run_cmd(["apt-get", "update"], timeout=240)
    if not ok:
        sys.stdout.write(f"[bootstrap] apt-get update для pg_repack: {err}\n")
        sys.stdout.flush()

    for pkg in candidates:
        sys.stdout.write(f"[bootstrap] Пытаюсь установить {pkg}...\n")
        sys.stdout.flush()
        ok, err = _run_cmd(["apt-get", "install", "-y", pkg], timeout=600)
        if not ok:
            sys.stdout.write(f"[bootstrap] {pkg}: {err}\n")
            sys.stdout.flush()
            continue
        if shutil.which("pg_repack"):
            return True
    return shutil.which("pg_repack") is not None


def _install_pg_repack_macos() -> bool:
    if shutil.which("brew") is None:
        return shutil.which("pg_repack") is not None
    ok, err = _run_cmd(["brew", "install", "pg_repack"], timeout=900)
    if not ok:
        sys.stdout.write(f"[bootstrap] brew install pg_repack: {err}\n")
        sys.stdout.flush()
        return False
    return shutil.which("pg_repack") is not None


def _install_pg_repack_windows() -> bool:
    """На Windows готовых бинарников pg_repack нет — только поиск уже установленного CLI."""
    _set_pg_repack_bootstrap_error(
        "на Windows нет официального pg_repack CLI; сжатие БД — через VACUUM FULL"
    )
    dest_dir = _project_bin_dir()
    (dest_dir / "pg_repack.download_failed").unlink(missing_ok=True)
    (dest_dir / "pg_repack.skip").unlink(missing_ok=True)
    return _find_pg_repack_windows()


def _auto_install_pg_repack() -> bool:
    system = platform.system()
    if system == "Linux":
        return _install_pg_repack_linux()
    if system == "Darwin":
        return _install_pg_repack_macos()
    if system == "Windows":
        return _install_pg_repack_windows()
    return False


def ensure_pg_repack_in_path(auto_install: bool = False) -> bool:
    """Гарантирует доступность pg_repack в PATH.

    - Сначала ищет бинарник в PATH/стандартных путях и в bin/ приложения.
    - Linux/macOS: при auto_install ставит пакет (apt/brew).
    - Windows: автоустановки нет (официальных бинарников нет); ищет только уже установленный CLI.
    """
    _set_pg_repack_bootstrap_error("")
    if platform.system() == "Windows":
        if _WINDOWS_USES_VACUUM_FALLBACK:
            auto_install = False
        dest_dir = _project_bin_dir()
        (dest_dir / "pg_repack.download_failed").unlink(missing_ok=True)
        (dest_dir / "pg_repack.skip").unlink(missing_ok=True)
        found = _find_pg_repack_windows()
    else:
        found = shutil.which("pg_repack") is not None
    if found:
        return True
    if not auto_install:
        if platform.system() == "Windows" and _WINDOWS_USES_VACUUM_FALLBACK:
            _set_pg_repack_bootstrap_error(
                "pg_repack не найден; на Windows используется VACUUM FULL"
            )
        else:
            _set_pg_repack_bootstrap_error("pg_repack не найден в PATH")
        return False

    sys.stdout.write("[bootstrap] pg_repack не найден — запускаю автоустановку.\n")
    sys.stdout.flush()
    try:
        ok = _auto_install_pg_repack()
    except Exception as e:
        err = f"{type(e).__name__}: {e}"
        _set_pg_repack_bootstrap_error(err)
        sys.stdout.write(f"[bootstrap] pg_repack: исключение при автоустановке: {e}\n")
        sys.stdout.flush()
        ok = False
    if not ok:
        if not get_pg_repack_bootstrap_error():
            _set_pg_repack_bootstrap_error("автоустановка pg_repack завершилась без результата")
        return False

    if platform.system() == "Windows":
        found = _find_pg_repack_windows()
    else:
        found = shutil.which("pg_repack") is not None
    if not found and not get_pg_repack_bootstrap_error():
        _set_pg_repack_bootstrap_error("pg_repack не найден после автоустановки")
    return found


def _ensure_ffmpeg() -> None:
    if shutil.which("ffmpeg"):
        return
    system = platform.system()
    if system == "Windows":
        _install_ffmpeg_windows()
    elif system == "Linux":
        try:
            subprocess.run(
                ["apt-get", "install", "-y", "ffmpeg"],
                check=True,
                timeout=300,
            )
        except Exception as e:
            raise RuntimeError(f"ffmpeg не удалось установить через apt-get: {e}") from e
    else:
        raise RuntimeError(
            f"ffmpeg не найден. Установите вручную (платформа: {system})."
        )


def ensure_required_software() -> None:
    """Проверяет и при необходимости устанавливает необходимый софт."""
    _ensure_dotenv()
    _packages = [
        ("psycopg2",      "psycopg2-binary"),
        ("flask",         "flask"),
        ("requests",      "requests"),
        ("flask_limiter", "Flask-Limiter"),
        ("playwright",    "playwright"),
        ("waitress",      "waitress"),
        ("yaml",          "pyyaml"),
    ]
    for spec_name, pip_name in _packages:
        if importlib.util.find_spec(spec_name) is None:
            _pip_install(pip_name)
    _ensure_ffmpeg()
    ensure_pg_repack_in_path()


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


def ensure_single_instance() -> None:
    """Не даёт запустить второй процесс с тем же main loop (общий _system_log_id в памяти)."""
    global _instance_lock_file
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
                sys.stdout.write(
                    f"VIPilot уже запущен (PID {other_pid}). "
                    f"Завершите процесс или lock: {lock_path}\n"
                )
                sys.stdout.flush()
                sys.exit(1)
            if attempt == 0:
                try:
                    lock_path.unlink(missing_ok=True)
                except OSError:
                    pass
                continue
            sys.stdout.write(
                "Не удалось получить блокировку VIPilot. "
                f"Завершите другой процесс Python или удалите lock: {lock_path}\n"
            )
            sys.stdout.flush()
            sys.exit(1)
        else:
            _write_lock_pid(fh, os.getpid())
            _instance_lock_file = fh
            return


def run_foreground(flask_app, module_name: str) -> None:
    """HTTP-сервер при запуске python main.py (не при import main:app)."""
    if module_name != "__main__":
        return
    if platform.system() == "Windows":
        from waitress import serve
        serve(flask_app, host="0.0.0.0", port=5000)
    else:
        flask_app.run(host="0.0.0.0", port=5000, debug=False)
