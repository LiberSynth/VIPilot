import os
from datetime import timezone, timedelta


def _get_flask_secret():
    secret = os.environ.get("FLASK_SECRET", "").strip()
    if secret:
        return secret
    try:
        from db import env_get, env_set
        stored = env_get("FLASK_SECRET", "")
        if stored:
            return stored
        generated = os.urandom(24).hex()
        env_set("FLASK_SECRET", generated)
        return generated
    except Exception:
        generated = os.urandom(24).hex()
        print("[WARN] FLASK_SECRET: не удалось загрузить из БД, сгенерирован случайный ключ — сессии будут сброшены при перезапуске")
        return generated


FLASK_SECRET = _get_flask_secret()

MSK = timezone(timedelta(hours=3))
