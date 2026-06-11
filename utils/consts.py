import os
import platform
import secrets
from datetime import timezone, timedelta

PLAYWRIGHT_BROWSERS_PATH = (
    r'C:\ProgramData\ms-playwright'
    if platform.system() == 'Windows'
    else '/usr/local/share/ms-playwright'
)

def _get_flask_secret():
    secret = os.environ.get("FLASK_SECRET", "").strip()
    if secret:
        return secret
    generated = secrets.token_hex(32)
    from log.log import write_log_entry
    write_log_entry(None, 'consts', 'FLASK_SECRET не задан в окружении — сгенерирован случайный ключ, сессии будут сброшены при перезапуске', level='warn')
    return generated

FLASK_SECRET = _get_flask_secret()

MSK = timezone(timedelta(hours=3))
