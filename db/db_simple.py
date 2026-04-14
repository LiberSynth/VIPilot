import json
import psycopg2
import psycopg2.extras

from .connection import get_db
from common.exceptions import FatalError


def db_insert_log(pipeline, message, status="info", batch_id=None):
    """Вставляет запись в таблицу log. Возвращает log_id (str) или None."""
    from .connection import get_db as _get_db
    try:
        with _get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO log (batch_id, pipeline, message, status)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id
                    """,
                    (batch_id, pipeline, message, status),
                )
                log_id = cur.fetchone()[0]
            conn.commit()
        return str(log_id)
    except Exception as e:
        print(f"[DB] Ошибка db_insert_log: {e}")
        return None


def db_insert_log_entry(log_id, message, level):
    import common.environment as wf_state
    if not wf_state.asserted_log_entry.get():
        raise FatalError("Нарушение конвенции логирования")
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO log_entries (log_id, message, level) VALUES (%s, %s, %s)",
                (log_id, message, level),
            )
        conn.commit()


def env_get(key, default=""):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT value FROM environment WHERE key = %s", (key,))
                row = cur.fetchone()
                return row[0] if row else default
    except Exception as e:
        print(f"[DB] Ошибка чтения env {key}: {e}")
        return default


def env_set(key, value):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO environment (key, value) VALUES (%s, %s)
                    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                    """,
                    (key, value),
                )
            conn.commit()
    except Exception as e:
        print(f"[DB] Ошибка записи env {key}: {e}")


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


def db_get_schedule():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, time_utc, created_at FROM schedule ORDER BY time_utc")
                rows = cur.fetchall()
        return [{"id": str(row[0]), "time_utc": row[1], "created_at": row[2]} for row in rows]
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


def db_get_active_targets():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    'SELECT id, name, aspect_ratio_x, aspect_ratio_y, transcode, config, slug FROM targets WHERE active = TRUE ORDER BY "order", name'
                )
                rows = cur.fetchall()
        return [
            {"id": str(row[0]), "name": row[1], "aspect_ratio_x": row[2], "aspect_ratio_y": row[3], "transcode": bool(row[4]), "config": row[5] or {}, "slug": row[6] or ""}
            for row in rows
        ]
    except Exception as e:
        print(f"[DB] Ошибка db_get_active_targets: {e}")
        return []


def db_update_target_transcode(target_id, value: bool):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE targets SET transcode = %s WHERE id = %s",
                    (value, target_id),
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_update_target_transcode: {e}")
        return False


def db_update_target_aspect_ratio(target_id, x, y):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE targets SET aspect_ratio_x = %s, aspect_ratio_y = %s WHERE id = %s",
                    (x, y, target_id)
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_update_target_aspect_ratio: {e}")
        return False


def db_get_target_by_name(name: str) -> dict | None:
    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT id::text, name, active, aspect_ratio_x, aspect_ratio_y, transcode, config "
                    "FROM targets WHERE name = %s LIMIT 1",
                    (name,),
                )
                row = cur.fetchone()
        return dict(row) if row else None
    except Exception as e:
        print(f"[DB] Ошибка db_get_target_by_name: {e}")
        return None


def db_update_target_config(target_id: str, config: dict) -> bool:
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE targets SET config = %s::jsonb WHERE id = %s::uuid",
                    (json.dumps(config), target_id),
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_update_target_config: {e}")
        return False


def db_update_target_publish_method_by_slug(slug: str, methods: dict) -> bool:
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE targets SET config = config || jsonb_build_object('publish_method', %s::jsonb)"
                    " WHERE slug = %s",
                    (json.dumps(methods), slug),
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_update_target_publish_method_by_slug: {e}")
        return False


def db_get_target_session_context(target_id: str) -> dict | None:
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT session_context FROM targets WHERE id = %s::uuid",
                    (target_id,),
                )
                row = cur.fetchone()
        if row and row[0]:
            return row[0]
        return None
    except Exception as e:
        print(f"[DB] Ошибка db_get_target_session_context: {e}")
        return None


def db_set_target_session_context(target_id: str, state: dict) -> bool:
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE targets SET session_context = %s::jsonb WHERE id = %s::uuid",
                    (json.dumps(state), target_id),
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_set_target_session_context: {e}")
        return False


def db_get_target_session_context_saved_at(target_id: str) -> str | None:
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT session_context->>'saved_at' FROM targets WHERE id = %s::uuid",
                    (target_id,),
                )
                row = cur.fetchone()
        return row[0] if row else None
    except Exception as e:
        print(f"[DB] Ошибка db_get_target_session_context_saved_at: {e}")
        return None


def db_create_story(model_id, title, result):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO stories (model_id, title, content, grade) VALUES (%s, %s, %s, NULL) RETURNING id",
                    (model_id, title, result),
                )
                row = cur.fetchone()
            conn.commit()
        return str(row[0]) if row else None
    except Exception as e:
        print(f"[DB] Ошибка db_create_story: {e}")
        return None


def db_set_story_grade(story_id, grade):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE stories SET grade = %s WHERE id = %s::uuid",
                    (grade, story_id),
                )
                updated = cur.rowcount
            conn.commit()
        return updated > 0
    except Exception as e:
        print(f"[DB] Ошибка db_set_story_grade: {e}")
        return False


def db_get_story_text(story_id):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT content FROM stories WHERE id = %s", (story_id,))
                row = cur.fetchone()
        return row[0] if row else None
    except Exception as e:
        print(f"[DB] Ошибка db_get_story_text: {e}")
        return None


def db_get_story_title(story_id):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT title FROM stories WHERE id = %s", (story_id,))
                row = cur.fetchone()
        return row[0] if row else None
    except Exception as e:
        print(f"[DB] Ошибка db_get_story_title: {e}")
        return None


def db_set_story_model(story_id, model_id):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE stories SET model_id = %s WHERE id = %s",
                    (model_id, story_id),
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_set_story_model: {e}")
        return False


def db_upsert_story_draft(story_id, title, content):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                if story_id:
                    cur.execute(
                        """
                        INSERT INTO stories (id, model_id, title, content, grade, manual_changed)
                        VALUES (%s::uuid, NULL, %s, %s, 'good', TRUE)
                        ON CONFLICT (id) DO UPDATE
                            SET title = EXCLUDED.title,
                                content = EXCLUDED.content,
                                manual_changed = TRUE
                        RETURNING id
                        """,
                        (story_id, title, content),
                    )
                    row = cur.fetchone()
                    if row:
                        conn.commit()
                        return str(row[0])
                cur.execute(
                    "INSERT INTO stories (model_id, title, content, grade, manual_changed) VALUES (NULL, %s, %s, 'good', TRUE) RETURNING id",
                    (title, content),
                )
                row = cur.fetchone()
            conn.commit()
        return str(row[0]) if row else None
    except Exception as e:
        print(f"[DB] Ошибка db_upsert_story_draft: {e}")
        return None


def db_activate_model(model_id: str, model_type: str):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE ai_models SET active = FALSE WHERE type = %s", (model_type,))
                cur.execute("UPDATE ai_models SET active = TRUE  WHERE id = %s", (model_id,))
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_activate_model: {e}")
        return False


def db_toggle_model(model_id: str):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE ai_models SET active = NOT active WHERE id = %s",
                    (model_id,),
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_toggle_model: {e}")
        return False


def db_reorder_models(ids: list):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                for idx, model_id in enumerate(ids, start=1):
                    cur.execute('UPDATE ai_models SET "order" = %s WHERE id = %s', (idx, model_id))
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_reorder_models: {e}")
        return False


def db_set_model_grade(model_id: str, grade: str):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE ai_models SET grade = %s WHERE id = %s",
                    (grade, model_id),
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_set_model_grade: {e}")
        return False


def db_get_distinct_batch_statuses():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT DISTINCT status FROM batches")
                rows = cur.fetchall()
        return {row[0] for row in rows}
    except Exception as e:
        print(f"[DB] Ошибка db_get_distinct_batch_statuses: {e}")
        return set()


def db_get_last_pipeline_run(pipeline):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT MAX(created_at) FROM log WHERE pipeline = %s",
                    (pipeline,),
                )
                row = cur.fetchone()
        return row[0] if row and row[0] is not None else None
    except Exception as e:
        print(f"[DB] Ошибка db_get_last_pipeline_run: {e}")
        return None
