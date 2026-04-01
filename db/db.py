import time
import json
import psycopg2
import psycopg2.extras

from .init import get_db


# ---------------------------------------------------------------------------
# Helpers (будут вынесены в utils.py на следующем шаге)
# ---------------------------------------------------------------------------

def _parse_history_days(s):
    try:
        return max(1, min(365, int(s)))
    except Exception:
        return 7


def _parse_short_log_days(s):
    try:
        return max(1, min(3650, int(s)))
    except Exception:
        return 365


# ---------------------------------------------------------------------------
# Настройки
# ---------------------------------------------------------------------------

def db_get(key, default=""):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT value FROM settings WHERE key = %s", (key,))
                row = cur.fetchone()
                return row[0] if row else default
    except Exception as e:
        print(f"[DB] Ошибка чтения {key}: {e}")
        return default


def db_set(key, value):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO settings (key, value) VALUES (%s, %s)
                    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                    """,
                    (key, value),
                )
            conn.commit()
    except Exception as e:
        print(f"[DB] Ошибка записи {key}: {e}")


# ---------------------------------------------------------------------------
# Расписание публикаций
# ---------------------------------------------------------------------------

def db_get_schedule():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, time_utc FROM schedule ORDER BY time_utc")
                rows = cur.fetchall()
        return [{"id": str(row[0]), "time_utc": row[1]} for row in rows]
    except Exception as e:
        print(f"[DB] Ошибка получения расписания: {e}")
        return []


def db_add_schedule_slot(time_utc):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO schedule (time_utc) VALUES (%s) RETURNING id",
                    (time_utc,),
                )
                row = cur.fetchone()
            conn.commit()
        return str(row[0])
    except Exception as e:
        print(f"[DB] Ошибка добавления слота расписания: {e}")
        return None


def db_delete_schedule_slot(slot_id):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM schedule WHERE id = %s", (slot_id,))
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка удаления слота расписания: {e}")
        return False


# ---------------------------------------------------------------------------
# Таргеты
# ---------------------------------------------------------------------------

def db_get_active_targets():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, name, aspect_ratio_x, aspect_ratio_y FROM targets WHERE active = TRUE"
                )
                rows = cur.fetchall()
        return [
            {"id": str(row[0]), "name": row[1], "aspect_ratio_x": row[2], "aspect_ratio_y": row[3]}
            for row in rows
        ]
    except Exception as e:
        print(f"[DB] Ошибка db_get_active_targets: {e}")
        return []


# ---------------------------------------------------------------------------
# Батчи
# ---------------------------------------------------------------------------

def db_ensure_batch(scheduled_at, target_id):
    """Создаёт батч если не существует. Возвращает UUID нового батча или None если уже был."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO batches (scheduled_at, target_id)
                    VALUES (%s, %s)
                    ON CONFLICT (scheduled_at, target_id) DO NOTHING
                    RETURNING id
                    """,
                    (scheduled_at, target_id),
                )
                row = cur.fetchone()
            conn.commit()
        return str(row[0]) if row else None
    except Exception as e:
        print(f"[DB] Ошибка db_ensure_batch: {e}")
        return None


# ---------------------------------------------------------------------------
# Лог уровня приложения (без батча)
# ---------------------------------------------------------------------------

def db_log_pipeline(pipeline, message, status='info', batch_id=None):
    """Запись в лог от имени конкретного пайплайна, опционально привязанная к батчу."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO log (batch_id, pipeline, message, status)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (batch_id, pipeline, message, status),
                )
            conn.commit()
    except Exception as e:
        print(f"[DB] Ошибка db_log_pipeline: {e}")


def db_log_root(message, status='info'):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO log (batch_id, pipeline, message, status)
                    VALUES (NULL, 'root', %s, %s)
                    """,
                    (message, status),
                )
            conn.commit()
    except Exception as e:
        print(f"[DB] Ошибка db_log_root: {e}")


def db_get_log(limit=200):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        l.id,
                        l.batch_id,
                        l.pipeline,
                        l.message,
                        l.status,
                        l.time_point,
                        COALESCE(
                            json_agg(
                                json_build_object(
                                    'message', le.message,
                                    'level',   le.level,
                                    'time_point', le.time_point
                                ) ORDER BY le.time_point
                            ) FILTER (WHERE le.id IS NOT NULL),
                            '[]'
                        ) AS entries
                    FROM log l
                    LEFT JOIN log_entries le ON le.log_id = l.id
                    GROUP BY l.id
                    ORDER BY l.time_point DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = cur.fetchall()
        return [
            {
                "id":         str(row[0]),
                "batch_id":   str(row[1]) if row[1] else None,
                "pipeline":   row[2],
                "message":    row[3],
                "status":     row[4],
                "time_point": row[5].isoformat() if row[5] else None,
                "entries":    row[6],
            }
            for row in rows
        ]
    except Exception as e:
        print(f"[DB] Ошибка db_get_log: {e}")
        return []


# ---------------------------------------------------------------------------
# Циклы
# ---------------------------------------------------------------------------

def db_save_cycle(cycle):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO cycles (started, started_ts, status, entries, summary)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        cycle["started"],
                        cycle["started_ts"],
                        cycle["status"],
                        json.dumps(cycle["entries"], ensure_ascii=False),
                        json.dumps(cycle.get("summary", {}), ensure_ascii=False),
                    ),
                )
                row = cur.fetchone()
                cycle["db_id"] = row[0]
            conn.commit()
    except Exception as e:
        print(f"[DB] Ошибка сохранения цикла: {e}")


def db_load_cycles():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, started, started_ts, status, entries, summary
                    FROM cycles ORDER BY started_ts DESC LIMIT 20
                """)
                rows = cur.fetchall()
        result = []
        for row in rows:
            result.append({
                "db_id": row[0],
                "started": row[1],
                "started_ts": row[2],
                "status": row[3],
                "entries": row[4] if isinstance(row[4], list) else json.loads(row[4] or "[]"),
                "summary": row[5] if isinstance(row[5], dict) else json.loads(row[5] or "{}"),
            })
        return result
    except Exception as e:
        print(f"[DB] Ошибка загрузки циклов: {e}")
        return []


def db_trim_cycles(keep=20):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM cycles WHERE id NOT IN (
                        SELECT id FROM cycles ORDER BY started_ts DESC LIMIT %s
                    )
                    """,
                    (keep,),
                )
            conn.commit()
    except Exception as e:
        print(f"[DB] Ошибка обрезки циклов: {e}")


def db_trim_cycles_by_age(cycles=None):
    """Delete cycle rows older than short_log_days from DB.
    Optionally syncs the in-memory cycles deque when passed as argument.
    """
    days = _parse_short_log_days(db_get("short_log_days", "365"))
    cutoff = time.time() - days * 86400
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM cycles WHERE started_ts < %s", (cutoff,))
            conn.commit()
    except Exception as e:
        print(f"[DB] Ошибка очистки краткого лога: {e}")
    if cycles is not None:
        to_remove = [c for c in cycles if c.get("started_ts", 0) < cutoff]
        for c in to_remove:
            try:
                cycles.remove(c)
            except ValueError:
                pass


def db_clear_old_entries(cycles=None):
    """Clear entries[] for cycles older than history_days, keeping summary intact.
    Optionally syncs the in-memory cycles deque when passed as argument.
    """
    days = _parse_history_days(db_get("history_days", "7"))
    cutoff = time.time() - days * 86400
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE cycles SET entries = '[]'::jsonb
                    WHERE started_ts < %s AND entries != '[]'::jsonb
                    """,
                    (cutoff,),
                )
            conn.commit()
    except Exception as e:
        print(f"[DB] Ошибка очистки детального лога: {e}")
    if cycles is not None:
        for c in cycles:
            if c.get("started_ts", 0) < cutoff:
                c["entries"] = []


# ---------------------------------------------------------------------------
# Видео URLs
# ---------------------------------------------------------------------------

def db_save_video_url(url):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM video_urls WHERE url = %s", (url,))
                if cur.fetchone():
                    return
                cur.execute(
                    "INSERT INTO video_urls (url, created_at) VALUES (%s, %s)",
                    (url, time.time()),
                )
                cur.execute("SELECT COUNT(*) FROM video_urls")
                count = cur.fetchone()[0]
                if count > 50:
                    cur.execute(
                        "DELETE FROM video_urls WHERE id IN "
                        "(SELECT id FROM video_urls ORDER BY created_at ASC LIMIT %s)",
                        (count - 50,),
                    )
            conn.commit()
    except Exception as e:
        print(f"[DB] Ошибка сохранения URL видео: {e}")


def db_get_random_video_url():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT url FROM video_urls ORDER BY RANDOM() LIMIT 1")
                row = cur.fetchone()
                return row[0] if row else None
    except Exception as e:
        print(f"[DB] Ошибка получения URL видео: {e}")
        return None


# ---------------------------------------------------------------------------
# Сюжеты
# ---------------------------------------------------------------------------

def db_save_story(model_uuid, result):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO generated_stories (created_at, model_id, result) "
                    "VALUES (%s, %s, %s) RETURNING id",
                    (time.time(), model_uuid, result),
                )
                row = cur.fetchone()
            conn.commit()
        return row[0] if row else None
    except Exception as e:
        print(f"[DB] Ошибка сохранения сюжета: {e}")
        return None


# ---------------------------------------------------------------------------
# Модели
# ---------------------------------------------------------------------------

def get_active_model():
    """Returns (submit_url, body_template, platform_url, model_name) for the active video model,
    or (None, None, None, None)."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT p.url, vm.url, vm.body, vm.name
                    FROM models vm
                    JOIN ai_platforms p ON p.id = vm.ai_platform_id
                    WHERE vm.active = TRUE AND vm.type = 0
                    LIMIT 1
                """)
                row = cur.fetchone()
        if not row:
            return None, None, None, None
        platform_url = row[0]
        model_url = row[1]
        body_tpl = row[2] if isinstance(row[2], dict) else {}
        model_name = row[3]
        submit_url = f"{platform_url}/{model_url}"
        return submit_url, body_tpl, platform_url, model_name
    except Exception as e:
        print(f"[DB] Ошибка получения активной модели: {e}")
        return None, None, None, None


def get_active_text_model():
    """Returns (api_url, model_id, body_tpl, model_name, model_uuid) for the active text model,
    or (None, None, None, None, None)."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT p.url, m.url, m.body, m.name, m.id
                    FROM models m
                    JOIN ai_platforms p ON p.id = m.ai_platform_id
                    WHERE m.active = TRUE AND m.type = 1
                    LIMIT 1
                """)
                row = cur.fetchone()
        if not row:
            return None, None, None, None, None
        api_url = row[0]
        model_id = row[1]
        body_tpl = row[2] if isinstance(row[2], dict) else {}
        model_name = row[3]
        model_uuid = row[4]
        return api_url, model_id, body_tpl, model_name, model_uuid
    except Exception as e:
        print(f"[DB] Ошибка получения активной текстовой модели: {e}")
        return None, None, None, None, None
