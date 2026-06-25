from .connection import get_db

_DEFAULTS = {
    "text_prompt":       "",
    "format_prompt":     "",
    "t2v_conversion_prompt": "",
    "video_post_prompt": "",
    "video_duration":    6,
    "words_per_second":    8,
    "good_samples_count": 25,
}

_ALLOWED_KEYS = frozenset(_DEFAULTS.keys())

def _coerce(key, raw):
    if raw is None:
        return _DEFAULTS[key]
    if key == "video_duration":
        try:
            return int(raw)
        except (ValueError, TypeError):
            return raw
    if key == "words_per_second":
        try:
            return int(float(raw))
        except (ValueError, TypeError):
            return raw
    if key == "good_samples_count":
        try:
            return int(raw)
        except (ValueError, TypeError):
            return raw
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

def _parse_int_value(value) -> int:
    if isinstance(value, bool):
        raise ValueError("invalid integer")
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not value.is_integer():
            raise ValueError("invalid integer")
        return int(value)
    text = str(value).strip()
    if not text:
        raise ValueError("invalid integer")
    try:
        parsed = float(text)
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid integer") from exc
    if not parsed.is_integer():
        raise ValueError("invalid integer")
    return int(parsed)

def cycle_config_set(key: str, value) -> None:
    if key not in _ALLOWED_KEYS:
        raise ValueError(f"Unknown cycle_config key: {key!r}")
    if isinstance(value, bool):
        str_value = "1" if value else "0"
    elif key in ("words_per_second", "good_samples_count", "video_duration"):
        str_value = str(_parse_int_value(value))
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
