import subprocess
import os

_VERSION_BASE = os.environ.get("APP_VERSION_BASE", "1.0.3")


def _get_build_number() -> str:
    try:
        count = subprocess.check_output(
            ["git", "rev-list", "--count", "HEAD"],
            stderr=subprocess.DEVNULL,
        ).decode().strip()
        if count:
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
