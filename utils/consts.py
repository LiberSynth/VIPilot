import os
import secrets
from datetime import timezone, timedelta


def _get_flask_secret():
    secret = os.environ.get("FLASK_SECRET", "").strip()
    if secret:
        return secret
    generated = secrets.token_hex(32)
    from log.log import write_log_entry
    write_log_entry(None, "[WARN] FLASK_SECRET не задан в окружении — сгенерирован случайный ключ, сессии будут сброшены при перезапуске", level='silent')
    return generated


FLASK_SECRET = _get_flask_secret()

MSK = timezone(timedelta(hours=3))
