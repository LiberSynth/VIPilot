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
