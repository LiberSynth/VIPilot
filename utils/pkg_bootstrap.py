"""
Минимальный загрузчик пакетов — только stdlib, никаких DB-зависимостей.
Вызывается из main.py до любых других импортов.
"""
import importlib.util
import os
import pathlib
import platform
import shutil
import subprocess
import sys


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


def _find_pg_config_windows() -> pathlib.Path | None:
    configured = os.environ.get("PG_CONFIG", "").strip().strip('"')
    if configured:
        configured_path = pathlib.Path(configured)
        if configured_path.exists():
            return configured_path
    for bin_dir in _iter_windows_postgres_bin_dirs():
        exe = bin_dir / "pg_config.exe"
        if exe.exists():
            return exe
    return None


def _find_pgxn_install_script() -> pathlib.Path | None:
    try:
        from pgxnclient import find_script
        script = find_script("pgxn-install")
        if script:
            p = pathlib.Path(script)
            if p.exists():
                return p
    except Exception:
        pass
    try:
        import pgxnclient
        p = pathlib.Path(pgxnclient.__file__).resolve().parent / "libexec" / "pgxn-install"
        if p.exists():
            return p
    except Exception:
        pass
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
    # На Windows бинарный entrypoint pgxn.exe в pgxnclient ломается (execv no such file),
    # поэтому запускаем внутренний скрипт pgxn-install напрямую через Python.
    if _find_pg_repack_windows():
        return True

    if _find_pg_config_windows() is None:
        sys.stdout.write("[bootstrap] pg_repack: pg_config.exe не найден в установке PostgreSQL.\n")
        sys.stdout.flush()
        return False
    pg_config = _find_pg_config_windows()
    if not pg_config:
        return False
    _prepend_to_path(pg_config.parent)

    lib_dir = pg_config.parent.parent / "lib"
    probe = lib_dir / "__pg_repack_write_test__.tmp"
    try:
        with open(probe, "w", encoding="ascii") as f:
            f.write("ok")
    except Exception:
        sys.stdout.write(
            f"[bootstrap] pg_repack: нет прав на запись в {lib_dir} (запустите терминал от администратора).\n"
        )
        sys.stdout.flush()
        return False
    finally:
        probe.unlink(missing_ok=True)

    if _find_pgxn_install_script() is None:
        ok, err = _run_cmd([sys.executable, "-m", "pip", "install", "pgxnclient"], timeout=240)
        if not ok:
            sys.stdout.write(f"[bootstrap] Не удалось установить pgxnclient: {err}\n")
            sys.stdout.flush()
            return False

    pgxn_install = _find_pgxn_install_script()
    if not pgxn_install:
        sys.stdout.write("[bootstrap] pg_repack: скрипт pgxn-install не найден после установки pgxnclient.\n")
        sys.stdout.flush()
        return False

    sys.stdout.write(f"[bootstrap] Пытаюсь установить pg_repack через {pgxn_install}...\n")
    sys.stdout.flush()
    ok, err = _run_cmd(
        [
            sys.executable,
            str(pgxn_install),
            "--yes",
            "pg_repack",
            "--pg_config",
            str(pg_config),
        ],
        timeout=1200,
    )
    if not ok:
        sys.stdout.write(f"[bootstrap] pgxn-install pg_repack: {err}\n")
        sys.stdout.flush()
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

    - Сначала ищет бинарник в PATH/стандартных путях.
    - При auto_install=True пытается установить автоматически (best-effort).
    """
    if platform.system() == "Windows":
        found = _find_pg_repack_windows()
    else:
        found = shutil.which("pg_repack") is not None
    if found:
        return True
    if not auto_install:
        return False

    sys.stdout.write("[bootstrap] pg_repack не найден — запускаю автоустановку.\n")
    sys.stdout.flush()
    if not _auto_install_pg_repack():
        return False

    if platform.system() == "Windows":
        return _find_pg_repack_windows()
    return shutil.which("pg_repack") is not None


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
