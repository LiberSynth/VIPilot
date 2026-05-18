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

_PG_REPACK_VERSION = "1.5.2"
_PG_REPACK_MIN_PG_MAJOR = 13
_PG_REPACK_BOOTSTRAP_ERROR = ""


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


def _find_pg_install_dir() -> pathlib.Path | None:
    for bin_dir in _iter_windows_postgres_bin_dirs():
        pg_dir = bin_dir.parent
        if (bin_dir / "postgres.exe").exists():
            return pg_dir
    return None


def _get_pg_major_version(pg_dir: pathlib.Path) -> int | None:
    pg_config = pg_dir / "bin" / "pg_config.exe"
    if pg_config.exists():
        try:
            r = subprocess.run(
                [str(pg_config), "--version"],
                capture_output=True,
                timeout=5,
            )
            out = r.stdout.decode(errors="replace").strip()
            for part in out.split():
                if part and part[0].isdigit():
                    return int(part.split(".")[0])
        except Exception:
            pass
    try:
        return int(pg_dir.name.split(".")[0])
    except ValueError:
        return None


def _write_pg_repack_file(path: pathlib.Path, data: bytes) -> bool:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return True
    except OSError as e:
        sys.stdout.write(
            f"[bootstrap] pg_repack: не удалось записать {path}: {e}\n"
        )
        sys.stdout.flush()
        return False


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
    """Скачивает готовый pg_repack с GitHub в bin/ приложения (без UAC).

    При наличии локального PostgreSQL по возможности копирует dll и extension
    в его каталог; ошибки записи в Program Files не блокируют установку CLI.
    """
    import urllib.error
    import urllib.request
    import zipfile

    _set_pg_repack_bootstrap_error("")
    if _find_pg_repack_windows():
        return True

    dest_dir = _project_bin_dir()
    skip_flag = dest_dir / "pg_repack.download_failed"
    legacy_skip = dest_dir / "pg_repack.skip"
    legacy_skip.unlink(missing_ok=True)
    if skip_flag.exists():
        _set_pg_repack_bootstrap_error(
            "ранее не удалось скачать pg_repack (удалите bin/pg_repack.download_failed и повторите)"
        )
        return False

    exe_dest = dest_dir / "pg_repack.exe"
    if exe_dest.exists():
        _prepend_to_path(dest_dir)
        return True

    pg_dir = _find_pg_install_dir()
    pg_ver = _detect_pg_major_from_database_url()
    if pg_ver is None and pg_dir is not None:
        pg_ver = _get_pg_major_version(pg_dir)
    if pg_ver is None:
        pg_ver = 16

    zip_path = dest_dir / "pg_repack.zip"
    downloaded = False
    last_err = ""
    for try_ver in range(pg_ver, _PG_REPACK_MIN_PG_MAJOR - 1, -1):
        url = (
            f"https://github.com/reorg/pg_repack/releases/download/"
            f"ver_{_PG_REPACK_VERSION}/"
            f"pg_repack_pg{try_ver}-{_PG_REPACK_VERSION}-x86-64.zip"
        )
        sys.stdout.write(f"[bootstrap] Скачиваю pg_repack для PostgreSQL {try_ver}: {url}\n")
        sys.stdout.flush()
        try:
            urllib.request.urlretrieve(url, zip_path)
            downloaded = True
            break
        except urllib.error.HTTPError as e:
            last_err = str(e)
            zip_path.unlink(missing_ok=True)
            if e.code == 404:
                continue
            _set_pg_repack_bootstrap_error(f"ошибка скачивания: {e}")
            sys.stdout.write(f"[bootstrap] pg_repack: ошибка скачивания — {e}\n")
            sys.stdout.flush()
            skip_flag.write_text(last_err, encoding="utf-8")
            return False
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            zip_path.unlink(missing_ok=True)
            if "404" in str(e) or "Not Found" in str(e):
                continue
            _set_pg_repack_bootstrap_error(last_err)
            sys.stdout.write(f"[bootstrap] pg_repack: ошибка скачивания — {e}\n")
            sys.stdout.flush()
            skip_flag.write_text(last_err, encoding="utf-8")
            return False

    if not downloaded:
        msg = (
            f"нет релиза pg_repack {_PG_REPACK_VERSION} для PostgreSQL "
            f"{pg_ver}–{_PG_REPACK_MIN_PG_MAJOR}"
        )
        _set_pg_repack_bootstrap_error(msg)
        sys.stdout.write(f"[bootstrap] pg_repack: {msg}\n")
        sys.stdout.flush()
        skip_flag.write_text(msg, encoding="utf-8")
        return False

    sys.stdout.write("[bootstrap] Устанавливаю pg_repack...\n")
    sys.stdout.flush()
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            for member in zf.namelist():
                fname = pathlib.Path(member).name
                if not fname:
                    continue
                data = zf.read(member)
                if fname == "pg_repack.exe":
                    if not _write_pg_repack_file(exe_dest, data):
                        return False
                elif fname == "pg_repack.dll":
                    _write_pg_repack_file(dest_dir / fname, data)
                    if pg_dir is not None:
                        _write_pg_repack_file(pg_dir / "lib" / fname, data)
                elif fname.endswith(".sql") or fname.endswith(".control"):
                    if pg_dir is not None:
                        _write_pg_repack_file(pg_dir / "share" / "extension" / fname, data)
    except Exception as e:
        last_err = f"{type(e).__name__}: {e}"
        _set_pg_repack_bootstrap_error(last_err)
        sys.stdout.write(f"[bootstrap] pg_repack: ошибка распаковки — {e}\n")
        sys.stdout.flush()
        skip_flag.write_text(last_err, encoding="utf-8")
        return False
    finally:
        zip_path.unlink(missing_ok=True)

    if not exe_dest.exists():
        _set_pg_repack_bootstrap_error("pg_repack.exe отсутствует в архиве")
        skip_flag.write_text("pg_repack.exe отсутствует в архиве", encoding="utf-8")
        return False

    skip_flag.unlink(missing_ok=True)
    _prepend_to_path(dest_dir)
    sys.stdout.write(f"[bootstrap] pg_repack установлен: {exe_dest}\n")
    sys.stdout.flush()
    return True


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
    - На Windows при auto_install скачивает готовый бинарник с GitHub (без UAC).
    """
    _set_pg_repack_bootstrap_error("")
    if platform.system() == "Windows":
        found = _find_pg_repack_windows()
    else:
        found = shutil.which("pg_repack") is not None
    if found:
        return True
    if not auto_install:
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
