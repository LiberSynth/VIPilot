"""
Минимальный загрузчик пакетов — только stdlib, никаких DB-зависимостей.
Вызывается из main.py до любых других импортов.
"""
import importlib.util
import subprocess
import sys


def _pip_install(package: str) -> None:
    subprocess.run(
        [sys.executable, "-m", "pip", "install", package],
        check=True,
    )


def ensure_dotenv() -> None:
    """Устанавливает python-dotenv через pip если он не найден."""
    if importlib.util.find_spec("dotenv") is None:
        _pip_install("python-dotenv")


def ensure_psycopg2() -> None:
    """Устанавливает psycopg2-binary через pip если он не найден."""
    if importlib.util.find_spec("psycopg2") is None:
        _pip_install("psycopg2-binary")


def ensure_all_packages() -> None:
    """Устанавливает все необходимые пакеты если они не найдены."""
    _packages = [
        ("flask",        "flask"),
        ("requests",     "requests"),
        ("flask_limiter","Flask-Limiter"),
        ("playwright",   "playwright"),
    ]
    for spec_name, pip_name in _packages:
        if importlib.util.find_spec(spec_name) is None:
            _pip_install(pip_name)
