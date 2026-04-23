from .connection import get_db

_DEFAULTS = {
    "text_prompt": "",
    "format_prompt": "",
    "video_post_prompt": "",
    "video_duration": 6,
    "approve_stories": False,
    "approve_movies": False,
    "words_per_second": 8.0,
}

_ALLOWED_COLUMNS = frozenset(_DEFAULTS.keys())


def cycle_config_get() -> dict:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT text_prompt, format_prompt, video_post_prompt,"
                " video_duration, approve_stories, approve_movies, words_per_second"
                " FROM cycle_config WHERE id = 1"
            )
            row = cur.fetchone()
    if not row:
        return dict(_DEFAULTS)
    return {
        "text_prompt":       row[0] if row[0] is not None else "",
        "format_prompt":     row[1] if row[1] is not None else "",
        "video_post_prompt": row[2] if row[2] is not None else "",
        "video_duration":    row[3] if row[3] is not None else 6,
        "approve_stories":   bool(row[4]),
        "approve_movies":    bool(row[5]),
        "words_per_second":  float(row[6]) if row[6] is not None else 8.0,
    }


def cycle_config_set(**kwargs) -> None:
    cols = {k: v for k, v in kwargs.items() if k in _ALLOWED_COLUMNS}
    if not cols:
        return
    col_names = list(cols.keys())
    col_values = list(cols.values())
    set_clause = ", ".join(f"{k} = EXCLUDED.{k}" for k in col_names)
    insert_cols = ", ".join(col_names)
    placeholders = ", ".join("%s" for _ in col_names)
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO cycle_config (id, {insert_cols})"
                f" VALUES (1, {placeholders})"
                f" ON CONFLICT (id) DO UPDATE SET {set_clause}",
                col_values,
            )
        conn.commit()
