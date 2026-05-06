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


def ensure_all_packages() -> None:
    """Устанавливает все необходимые пакеты если они не найдены."""
    _packages = [
        ("dotenv",       "python-dotenv"),
        ("psycopg2",     "psycopg2-binary"),
        ("flask",        "flask"),
        ("requests",     "requests"),
        ("flask_limiter","Flask-Limiter"),
        ("playwright",   "playwright"),
    ]
    for spec_name, pip_name in _packages:
        if importlib.util.find_spec(spec_name) is None:
            _pip_install(pip_name)
