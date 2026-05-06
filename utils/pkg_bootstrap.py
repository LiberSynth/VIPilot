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


def _find_pg_install_dir() -> pathlib.Path | None:
    """Ищет директорию установки PostgreSQL на Windows."""
    search_roots = [
        pathlib.Path(r"C:\Program Files\PostgreSQL"),
        pathlib.Path(r"C:\Program Files (x86)\PostgreSQL"),
        pathlib.Path(r"C:\PostgreSQL"),
    ]
    for root in search_roots:
        if not root.exists():
            continue
        candidates = sorted(
            (d for d in root.iterdir() if d.is_dir()),
            key=lambda p: p.name,
            reverse=True,
        )
        for candidate in candidates:
            if (candidate / "bin" / "postgres.exe").exists():
                return candidate
    return None


def _get_pg_major_version(pg_dir: pathlib.Path) -> int | None:
    """Возвращает мажорную версию PostgreSQL (например 17) из директории установки."""
    pg_config = pg_dir / "bin" / "pg_config.exe"
    if pg_config.exists():
        try:
            r = subprocess.run(
                [str(pg_config), "--version"],
                capture_output=True, timeout=5,
            )
            out = r.stdout.decode().strip()
            for part in out.split():
                if part[0].isdigit():
                    return int(part.split(".")[0])
        except Exception:
            pass
    try:
        return int(pg_dir.name.split(".")[0])
    except ValueError:
        return None


def _install_pg_repack_windows() -> bool:
    """Скачивает pg_repack с GitHub, копирует файлы в PostgreSQL и добавляет в PATH.
    Возвращает True при успехе."""
    import os
    import urllib.request
    import zipfile

    _PG_REPACK_VERSION = "1.5.2"

    pg_dir = _find_pg_install_dir()
    if pg_dir is None:
        sys.stdout.write(
            "[bootstrap] pg_repack: директория PostgreSQL не найдена — "
            "авто-установка невозможна.\n"
        )
        sys.stdout.flush()
        return False

    pg_ver = _get_pg_major_version(pg_dir)
    if pg_ver is None:
        sys.stdout.write(
            f"[bootstrap] pg_repack: не удалось определить версию PostgreSQL "
            f"в {pg_dir} — авто-установка невозможна.\n"
        )
        sys.stdout.flush()
        return False

    dest_dir = pathlib.Path(__file__).resolve().parent.parent / "bin"
    dest_dir.mkdir(exist_ok=True)
    exe_dest = dest_dir / "pg_repack.exe"

    if not exe_dest.exists():
        url = (
            f"https://github.com/reorg/pg_repack/releases/download/"
            f"ver_{_PG_REPACK_VERSION}/"
            f"pg_repack_pg{pg_ver}-{_PG_REPACK_VERSION}-x86-64.zip"
        )
        zip_path = dest_dir / "pg_repack.zip"
        sys.stdout.write(f"[bootstrap] Скачиваю pg_repack для PostgreSQL {pg_ver}: {url}\n")
        sys.stdout.flush()
        try:
            urllib.request.urlretrieve(url, zip_path)
        except Exception as e:
            sys.stdout.write(f"[bootstrap] pg_repack: ошибка скачивания — {e}\n")
            sys.stdout.flush()
            zip_path.unlink(missing_ok=True)
            return False

        sys.stdout.write("[bootstrap] Устанавливаю pg_repack...\n")
        sys.stdout.flush()
        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                names = zf.namelist()
                for member in names:
                    fname = pathlib.Path(member).name
                    data = zf.read(member)

                    if fname == "pg_repack.exe":
                        exe_dest.write_bytes(data)

                    elif fname == "pg_repack.dll":
                        lib_dir = pg_dir / "lib"
                        lib_dir.mkdir(exist_ok=True)
                        (lib_dir / fname).write_bytes(data)

                    elif fname.endswith(".sql") or fname.endswith(".control"):
                        ext_dir = pg_dir / "share" / "extension"
                        ext_dir.mkdir(parents=True, exist_ok=True)
                        (ext_dir / fname).write_bytes(data)
        except Exception as e:
            sys.stdout.write(f"[bootstrap] pg_repack: ошибка распаковки — {e}\n")
            sys.stdout.flush()
            return False
        finally:
            zip_path.unlink(missing_ok=True)

    os.environ["PATH"] = str(dest_dir) + os.pathsep + os.environ.get("PATH", "")
    sys.stdout.write(f"[bootstrap] pg_repack установлен: {exe_dest}\n")
    sys.stdout.flush()
    return True


def ensure_pg_repack_in_path() -> bool:
    """Проверяет наличие pg_repack. На Windows — ищет в стандартных папках PostgreSQL,
    затем пробует скачать с GitHub. Возвращает True если доступен."""
    import os
    if platform.system() != "Windows":
        return shutil.which("pg_repack") is not None
    if shutil.which("pg_repack"):
        return True
    search_roots = [
        pathlib.Path(r"C:\Program Files\PostgreSQL"),
        pathlib.Path(r"C:\Program Files (x86)\PostgreSQL"),
        pathlib.Path(r"C:\PostgreSQL"),
    ]
    for root in search_roots:
        if not root.exists():
            continue
        for candidate in sorted(root.iterdir(), reverse=True):
            exe = candidate / "bin" / "pg_repack.exe"
            if exe.exists():
                os.environ["PATH"] = str(exe.parent) + os.pathsep + os.environ.get("PATH", "")
                sys.stdout.write(f"[bootstrap] pg_repack найден: {exe}\n")
                sys.stdout.flush()
                return True
    return _install_pg_repack_windows()


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
