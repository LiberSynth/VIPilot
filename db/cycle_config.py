from .connection import get_db

_DEFAULTS = {
    "text_prompt":       "",
    "format_prompt":     "",
    "video_post_prompt": "",
    "video_duration":    6,
    "approve_stories":   False,
    "approve_movies":    False,
    "words_per_second":  8.0,
}

_ALLOWED_KEYS = frozenset(_DEFAULTS.keys())


def _coerce(key, raw):
    if raw is None:
        return _DEFAULTS[key]
    if key == "video_duration":
        try:
            return int(raw)
        except (ValueError, TypeError):
            return _DEFAULTS[key]
    if key in ("approve_stories", "approve_movies"):
        return raw in ("1", "true", "True")
    if key == "words_per_second":
        try:
            return float(raw)
        except (ValueError, TypeError):
            return _DEFAULTS[key]
    return raw


def cycle_config_get(key: str):
    if key not in _ALLOWED_KEYS:
        raise ValueError(f"Unknown cycle_config key: {key!r}")
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT value FROM cycle_config WHERE key = %s",
                (key,),
            )
            row = cur.fetchone()
    return _coerce(key, row[0] if row else None)


def cycle_config_set(key: str, value) -> None:
    if key not in _ALLOWED_KEYS:
        raise ValueError(f"Unknown cycle_config key: {key!r}")
    if isinstance(value, bool):
        str_value = "1" if value else "0"
    else:
        str_value = str(value)
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO cycle_config (key, value) VALUES (%s, %s)"
                " ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                (key, str_value),
            )
        conn.commit()
