import time
from flask import session


def is_authenticated():
    if session.get("auth") is not True:
        return False
    auth_ts = session.get("auth_ts", 0)
    return (time.time() - auth_ts) < 604800
