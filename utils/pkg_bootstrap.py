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
_BOOTSTRAP_EVENTS: list[tuple[str, str]] = []


def _emit_bootstrap(message: str, level: str = "silent") -> None:
    """Буферизует bootstrap-сообщение для последующего flush в БД."""
    lvl = level if level in {"silent", "info", "warn", "error"} else "silent"
    _BOOTSTRAP_EVENTS.append((lvl, message))


def _emit_step(info_message: str, detail_message: str | None = None) -> None:
    """Ключевой шаг: пишем и user-friendly info, и технический silent."""
    _emit_bootstrap(info_message, level="info")
    _emit_bootstrap(detail_message or info_message, level="silent")


def drain_bootstrap_events() -> list[tuple[str, str]]:
    """Возвращает накопленные bootstrap-сообщения и очищает буфер."""
    events = list(_BOOTSTRAP_EVENTS)
    _BOOTSTRAP_EVENTS.clear()
    return events


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
        _emit_step(
            "[bootstrap] ffmpeg: скачиваю архив.",
            f"[bootstrap] ffmpeg: phase=download, url={_FFMPEG_URL}",
        )
        urllib.request.urlretrieve(_FFMPEG_URL, zip_path)
        _emit_step("[bootstrap] ffmpeg: распаковываю архив.", "[bootstrap] ffmpeg: phase=extract")
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
    _emit_step("[bootstrap] ffmpeg готов к работе.", f"[bootstrap] ffmpeg: phase=done, path={ffmpeg_exe}")


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
        _emit_step(
            "[bootstrap] pg_repack уже установлен.",
            f"[bootstrap] pg_repack: result=present, path={shutil.which('pg_repack')}",
        )
        return True

    local_candidates = [
        pathlib.Path(__file__).resolve().parent.parent / "bin" / "pg_repack.exe",
        pathlib.Path(__file__).resolve().parent.parent / "bin" / "pg_repack" / "pg_repack.exe",
    ]
    for exe in local_candidates:
        if exe.exists():
            _prepend_to_path(exe.parent)
            _emit_step("[bootstrap] pg_repack найден в локальном bin.", f"[bootstrap] pg_repack: path={exe}")
            return True

    for bin_dir in _iter_windows_postgres_bin_dirs():
        for name in ("pg_repack.exe", "pg_repack"):
            exe = bin_dir / name
            if exe.exists():
                _prepend_to_path(exe.parent)
                _emit_step("[bootstrap] pg_repack найден в установке PostgreSQL.", f"[bootstrap] pg_repack: path={exe}")
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
        _emit_bootstrap(f"[bootstrap] apt-get update для pg_repack: {err}", level="warn")

    for pkg in candidates:
        _emit_bootstrap(f"[bootstrap] Пытаюсь установить {pkg}...")
        ok, err = _run_cmd(["apt-get", "install", "-y", pkg], timeout=600)
        if not ok:
            _emit_bootstrap(f"[bootstrap] {pkg}: {err}", level="warn")
            continue
        if shutil.which("pg_repack"):
            return True
    return shutil.which("pg_repack") is not None


def _install_pg_repack_macos() -> bool:
    if shutil.which("brew") is None:
        return shutil.which("pg_repack") is not None
    ok, err = _run_cmd(["brew", "install", "pg_repack"], timeout=900)
    if not ok:
        _emit_bootstrap(f"[bootstrap] brew install pg_repack: {err}", level="warn")
        return False
    return shutil.which("pg_repack") is not None


def _install_pg_repack_windows() -> bool:
    # Официальных поддерживаемых бинарников под Windows нет.
    # Пытаемся best-effort через pgxnclient (если в системе есть сборочные инструменты).
    _set_pg_repack_bootstrap_error("")
    if _find_pg_repack_windows():
        return True

    if shutil.which("pgxnclient") is None:
        _emit_step(
            "[bootstrap] pgxnclient не найден, устанавливаю.",
            "[bootstrap] pg_repack: phase=install_pgxnclient",
        )
        ok, err = _run_cmd([sys.executable, "-m", "pip", "install", "pgxnclient"], timeout=240)
        if not ok:
            _set_pg_repack_bootstrap_error(f"не удалось установить pgxnclient: {err}")
            _emit_bootstrap(f"[bootstrap] Не удалось установить pgxnclient: {err}", level="warn")
            return False

    pgxnclient = shutil.which("pgxnclient")
    if not pgxnclient:
        _set_pg_repack_bootstrap_error("pgxnclient не найден после установки")
        return False

    pg_config = _find_pg_config_windows()
    if pg_config:
        _prepend_to_path(pg_config.parent)
        _emit_step(
            "[bootstrap] Найден pg_config для установки pg_repack.",
            f"[bootstrap] pg_repack: pg_config={pg_config}",
        )

    _emit_step(
        "[bootstrap] pg_repack не найден, запускаю доустановку.",
        "[bootstrap] pg_repack: phase=install_start, installer=pgxnclient",
    )
    cmd = [pgxnclient, "install", "--yes", "pg_repack"]
    if pg_config:
        cmd += ["--pg_config", str(pg_config)]
    else:
        # pgxnclient на Windows ищет файл буквально, поэтому расширение .exe важно.
        cmd += ["--pg_config", "pg_config.exe"]
    ok, err = _run_cmd(cmd, timeout=1200)
    if not ok:
        _set_pg_repack_bootstrap_error(err)
        _emit_bootstrap(f"[bootstrap] pgxnclient install pg_repack: {err}", level="warn")
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
        if platform.system() != "Windows":
            _emit_step(
                "[bootstrap] pg_repack уже установлен.",
                f"[bootstrap] pg_repack: result=present, path={shutil.which('pg_repack')}",
            )
        return True
    _set_pg_repack_bootstrap_error("pg_repack не найден в PATH")
    if not auto_install:
        _emit_step(
            "[bootstrap] pg_repack не найден.",
            "[bootstrap] pg_repack: result=missing, auto_install=0",
        )
        return False

    _emit_step(
        "[bootstrap] pg_repack не найден, запускаю доустановку.",
        "[bootstrap] pg_repack: result=missing, auto_install=1, action=install",
    )
    if not _auto_install_pg_repack():
        if not get_pg_repack_bootstrap_error():
            _set_pg_repack_bootstrap_error("автоустановка pg_repack завершилась без результата")
        _emit_bootstrap(
            f"[bootstrap] Автоустановка pg_repack неуспешна: {get_pg_repack_bootstrap_error()}",
            level="warn",
        )
        return False

    if platform.system() == "Windows":
        found = _find_pg_repack_windows()
    else:
        found = shutil.which("pg_repack") is not None
    if not found and not get_pg_repack_bootstrap_error():
        _set_pg_repack_bootstrap_error("pg_repack не найден после автоустановки")
    if found:
        _emit_step(
            "[bootstrap] pg_repack доступен.",
            f"[bootstrap] pg_repack: result=ready, path={shutil.which('pg_repack')}",
        )
    else:
        _emit_bootstrap(
            f"[bootstrap] pg_repack всё ещё недоступен: {get_pg_repack_bootstrap_error()}",
            level="warn",
        )
    return found


def _ensure_ffmpeg() -> None:
    ffmpeg_path = shutil.which("ffmpeg")
    if ffmpeg_path:
        _emit_step("[bootstrap] ffmpeg уже установлен.", f"[bootstrap] ffmpeg: result=present, path={ffmpeg_path}")
        return
    system = platform.system()
    _emit_step(
        "[bootstrap] ffmpeg не найден, запускаю доустановку.",
        f"[bootstrap] ffmpeg: result=missing, action=install, platform={system}",
    )
    if system == "Windows":
        _install_ffmpeg_windows()
    elif system == "Linux":
        try:
            subprocess.run(
                ["apt-get", "install", "-y", "ffmpeg"],
                check=True,
                timeout=300,
            )
            _emit_step(
                "[bootstrap] ffmpeg установлен через apt-get.",
                f"[bootstrap] ffmpeg: phase=done, path={shutil.which('ffmpeg')}",
            )
        except Exception as e:
            raise RuntimeError(f"ffmpeg не удалось установить через apt-get: {e}") from e
    else:
        raise RuntimeError(
            f"ffmpeg не найден. Установите вручную (платформа: {system})."
        )


def ensure_all_packages() -> None:
    """Устанавливает все необходимые пакеты если они не найдены."""
    _emit_step(
        "[bootstrap] Вход в проверку окружения.",
        f"[bootstrap] setup: phase=start, os={platform.system()}, py={sys.version.split()[0]}",
    )
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
        _emit_step(
            f"[bootstrap] Проверка Python-пакета {pip_name}.",
            f"[bootstrap] package_check: spec={spec_name}, pip_name={pip_name}, phase=check",
        )
        if importlib.util.find_spec(spec_name) is None:
            _emit_step(
                f"[bootstrap] Python-пакет {pip_name} не установлен, запускаю доустановку.",
                f"[bootstrap] package_check: pip_name={pip_name}, result=missing, action=install",
            )
            try:
                _pip_install(pip_name)
            except Exception as e:
                _emit_bootstrap(f"[bootstrap] Ошибка установки {pip_name}: {e}", level="error")
                _emit_bootstrap(
                    f"[bootstrap] package_check: pip_name={pip_name}, result=error, error={type(e).__name__}: {e}",
                    level="silent",
                )
                raise
            _emit_step(
                f"[bootstrap] Python-пакет {pip_name} установлен.",
                f"[bootstrap] package_check: pip_name={pip_name}, result=installed",
            )
        else:
            _emit_step(
                f"[bootstrap] Python-пакет {pip_name} уже установлен.",
                f"[bootstrap] package_check: pip_name={pip_name}, result=present",
            )
    _ensure_ffmpeg()
    _emit_step(
        "[bootstrap] Проверка pg_repack в окружении.",
        "[bootstrap] pg_repack: phase=check",
    )
    # Доустанавливаем pg_repack только при отсутствии в окружении.
    ensure_pg_repack_in_path(auto_install=True)
    _emit_step("[bootstrap] Проверка окружения завершена.", "[bootstrap] setup: phase=done")
