"""
Минимальный загрузчик пакетов — только stdlib, никаких DB-зависимостей.
Вызывается из main.py до любых других импортов.
"""
import importlib.util
import pathlib
import platform
import shutil
import subprocess
import sys

_PG_REPACK_BOOTSTRAP_ERROR = ""


def _set_pg_repack_bootstrap_error(msg: str | None) -> None:
    global _PG_REPACK_BOOTSTRAP_ERROR
    _PG_REPACK_BOOTSTRAP_ERROR = (msg or "").strip()


def get_pg_repack_bootstrap_error() -> str:
    return _PG_REPACK_BOOTSTRAP_ERROR


def _pip_install(package: str) -> None:
    subprocess.run(
        [sys.executable, "-m", "pip", "install", package],
        check=True,
    )


def _install_ffmpeg_windows() -> None:
    import os
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
    import os

    current = os.environ.get("PATH", "")
    parts = current.split(os.pathsep) if current else []
    path_str = str(path)
    if path_str in parts:
        return
    os.environ["PATH"] = path_str + os.pathsep + current


def _iter_windows_postgres_bin_dirs():
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
            if bin_dir.exists():
                yield bin_dir


def _find_pg_config_windows() -> pathlib.Path | None:
    for bin_dir in _iter_windows_postgres_bin_dirs():
        for name in ("pg_config.exe", "pg_config"):
            exe = bin_dir / name
            if exe.exists():
                return exe
    return None


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
        for name in ("pg_repack.exe", "pg_repack"):
            exe = bin_dir / name
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
    import os

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
    # Официальных поддерживаемых бинарников под Windows нет.
    # Пытаемся best-effort через pgxnclient (если в системе есть сборочные инструменты).
    _set_pg_repack_bootstrap_error("")
    if _find_pg_repack_windows():
        return True

    if shutil.which("pgxnclient") is None:
        ok, err = _run_cmd([sys.executable, "-m", "pip", "install", "pgxnclient"], timeout=240)
        if not ok:
            _set_pg_repack_bootstrap_error(f"не удалось установить pgxnclient: {err}")
            sys.stdout.write(f"[bootstrap] Не удалось установить pgxnclient: {err}\n")
            sys.stdout.flush()
            return False

    pgxnclient = shutil.which("pgxnclient")
    if not pgxnclient:
        _set_pg_repack_bootstrap_error("pgxnclient не найден после установки")
        return False

    pg_config = _find_pg_config_windows()
    if pg_config:
        _prepend_to_path(pg_config.parent)
        sys.stdout.write(f"[bootstrap] Найден pg_config: {pg_config}\n")
        sys.stdout.flush()

    sys.stdout.write("[bootstrap] Пытаюсь установить pg_repack через pgxnclient...\n")
    sys.stdout.flush()
    cmd = [pgxnclient, "install", "--yes", "pg_repack"]
    if pg_config:
        cmd += ["--pg_config", str(pg_config)]
    else:
        # pgxnclient на Windows ищет файл буквально, поэтому расширение .exe важно.
        cmd += ["--pg_config", "pg_config.exe"]
    ok, err = _run_cmd(cmd, timeout=1200)
    if not ok:
        _set_pg_repack_bootstrap_error(err)
        sys.stdout.write(f"[bootstrap] pgxnclient install pg_repack: {err}\n")
        sys.stdout.flush()
    found = _find_pg_repack_windows()
    if found:
        _set_pg_repack_bootstrap_error("")
        return True
    if not get_pg_repack_bootstrap_error():
        _set_pg_repack_bootstrap_error("pg_repack не найден после попытки установки")
    return False


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

    - Сначала ищет бинарник в PATH/стандартных путях.
    - При auto_install=True пытается установить автоматически (best-effort).
    """
    _set_pg_repack_bootstrap_error("")
    if platform.system() == "Windows":
        found = _find_pg_repack_windows()
    else:
        found = shutil.which("pg_repack") is not None
    if found:
        return True
    _set_pg_repack_bootstrap_error("pg_repack не найден в PATH")
    if not auto_install:
        return False

    sys.stdout.write("[bootstrap] pg_repack не найден — запускаю автоустановку.\n")
    sys.stdout.flush()
    if not _auto_install_pg_repack():
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


def ensure_all_packages() -> None:
    """Устанавливает все необходимые пакеты если они не найдены."""
    _packages = [
        ("dotenv",        "python-dotenv"),
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
