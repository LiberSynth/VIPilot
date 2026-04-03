import time
import hashlib
from flask import session
from utils.consts import ADMIN_PASSWORD


def password_fingerprint():
    return hashlib.sha256(ADMIN_PASSWORD.encode()).hexdigest()[:16]


def is_authenticated():
    if not (session.get("auth") is True and session.get("pw_fp") == password_fingerprint()):
        return False
    auth_ts = session.get("auth_ts", 0)
    return (time.time() - auth_ts) < 604800
