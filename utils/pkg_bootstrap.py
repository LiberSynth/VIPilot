"""
Минимальный загрузчик пакетов — только stdlib, никаких DB-зависимостей.
Вызывается из main.py до любых других импортов.
"""
import importlib.util
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
    ]
    for spec_name, pip_name in _packages:
        if importlib.util.find_spec(spec_name) is None:
            _pip_install(pip_name)
    _ensure_ffmpeg()
