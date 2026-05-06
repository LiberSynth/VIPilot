"""
Минимальный загрузчик пакетов — только stdlib, никаких DB-зависимостей.
Вызывается из main.py до любых других импортов.
"""
import importlib.util
import subprocess
import sys


def ensure_dotenv() -> None:
    """Устанавливает python-dotenv через pip если он не найден."""
    if importlib.util.find_spec("dotenv") is not None:
        return
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "python-dotenv"],
        check=True,
    )


def ensure_psycopg2() -> None:
    """Устанавливает psycopg2-binary через pip если он не найден."""
    if importlib.util.find_spec("psycopg2") is not None:
        return
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "psycopg2-binary"],
        check=True,
    )
