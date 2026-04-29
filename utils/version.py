import subprocess
import os

_VERSION_BASE = os.environ.get("APP_VERSION_BASE", "1.0.3")
_BUILD_FILE = os.path.join(os.path.dirname(__file__), "_build.py")


def _get_build_number() -> str:
    try:
        count = subprocess.check_output(
            ["git", "rev-list", "--count", "HEAD"],
            stderr=subprocess.DEVNULL,
        ).decode().strip()
        if count:
            with open(_BUILD_FILE, "w") as f:
                f.write(
                    "# _build.py — фиксированный номер сборки приложения.\n"
                    "#\n"
                    "# Назначение: хранить BUILD между запусками, когда git недоступен на проде.\n"
                    "# Обновляется автоматически при запуске utils/version.py (если доступен git),\n"
                    "# поэтому является исходным файлом, а не временным — должен коммититься в репо.\n"
                    "# Значение используется в upgrade.py для сравнения с build_number в БД.\n"
                    f'BUILD = "{count}"\n'
                )
            return count
    except Exception:
        pass
    try:
        from utils._build import BUILD
        return BUILD
    except Exception:
        return "0"


BUILD = _get_build_number()
VERSION = f"{_VERSION_BASE}.{BUILD}"
