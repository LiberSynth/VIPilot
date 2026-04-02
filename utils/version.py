import subprocess

_VERSION_BASE = "1.0.1"


def _get_build_number() -> str:
    try:
        count = subprocess.check_output(
            ["git", "rev-list", "--count", "HEAD"],
            stderr=subprocess.DEVNULL,
        ).decode().strip()
        return count
    except Exception:
        return "0"


BUILD  = _get_build_number()
VERSION = f"{_VERSION_BASE}.{BUILD}"
