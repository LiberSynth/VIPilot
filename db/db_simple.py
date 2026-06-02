import json
import psycopg2
import psycopg2.extras

from .connection import get_db
from common.exceptions import FatalError


def _assert_log_modification():
    import common.environment as environment
    if not environment.asserted_log_modification.get():
        raise FatalError("Нарушение конвенции: модификация log вне log.py")


def db_get_or_create_log(batch_id):
    """Один log на batch_id. Возвращает (log_id, is_first_batch_log)."""
    _assert_log_modification()
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM log WHERE batch_id = %s::uuid ORDER BY id ASC LIMIT 1",
                (batch_id,),
            )
            row = cur.fetchone()
            if row:
                return str(row[0]), False
            cur.execute(
                "INSERT INTO log (batch_id) VALUES (%s::uuid) RETURNING id",
                (batch_id,),
            )
            log_id = cur.fetchone()[0]
        conn.commit()
    return str(log_id), True


def db_insert_log(batch_id):
    """Новое окно log (batch_id=None — блок «Приложение»)."""
    _assert_log_modification()
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO log (batch_id) VALUES (%s) RETURNING id",
                (batch_id,),
            )
            log_id = cur.fetchone()[0]
        conn.commit()
    return str(log_id)


def db_insert_log_entry(log_id, channel, message, level):
    import common.environment as environment

    if not environment.asserted_log_entry.get():
        raise FatalError("Нарушение конвенции логирования")
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO log_entries (log_id, channel, message, level)
                VALUES (%s, %s, %s, %s)
                """,
                (log_id, channel, message, level),
            )
        conn.commit()


def env_get(key, default=""):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM environment WHERE key = %s", (key,))
            row = cur.fetchone()
            return row[0] if row else default


def env_set(key, value):
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


def db_next_publication_number() -> int:
    """Атомарно инкрементирует счётчик публикаций и возвращает новое значение."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE environment
                SET value = (CAST(value AS INTEGER) + 1)::TEXT
                WHERE key = 'publication_counter'
                RETURNING CAST(value AS INTEGER)
            """)
            row = cur.fetchone()
        conn.commit()
    return row[0] if row else 1


def settings_get(key, default=""):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM settings WHERE key = %s", (key,))
            row = cur.fetchone()
            return row[0] if row else default


def settings_set(key, value):
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


def db_get_schedule():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, time_utc, created_at FROM schedule ORDER BY time_utc"
            )
            rows = cur.fetchall()
    return [
        {"id": str(row[0]), "time_utc": row[1], "created_at": row[2]} for row in rows
    ]


def db_add_schedule_slot(time_utc):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO schedule (time_utc) VALUES (%s) RETURNING id",
                (time_utc,),
            )
            row = cur.fetchone()
        conn.commit()
    return str(row[0])


def db_delete_schedule_slot(slot_id):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM schedule WHERE id = %s", (slot_id,))
        conn.commit()
    return True


def db_get_active_targets():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'SELECT id, name, config, slug FROM targets WHERE active = TRUE ORDER BY "order", name'
            )
            rows = cur.fetchall()
    return [
        {
            "id": str(row[0]),
            "name": row[1],
            "config": row[2] or {},
            "slug": row[3] or "",
        }
        for row in rows
    ]


def db_get_all_targets():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'SELECT id::text, name, active, slug FROM targets ORDER BY "order", name'
            )
            rows = cur.fetchall()
    return [
        {"id": row[0], "name": row[1], "active": bool(row[2]), "slug": row[3] or ""}
        for row in rows
    ]


def db_get_target_by_name(name: str) -> dict | None:
    with get_db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id::text, name, active, config, slug "
                "FROM targets WHERE name = %s LIMIT 1",
                (name,),
            )
            row = cur.fetchone()
    return dict(row) if row else None


def db_update_target_publish_method_by_slug(slug: str, methods: dict) -> bool:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE targets SET config = config || jsonb_build_object('publish_method', %s::jsonb)"
                " WHERE slug = %s",
                (json.dumps(methods), slug),
            )
        conn.commit()
    return True


def db_get_target_session_context(target_id: str) -> dict | None:
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


def db_set_target_session_context(target_id: str, state: dict) -> bool:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE targets SET session_context = %s::jsonb WHERE id = %s::uuid",
                (json.dumps(state), target_id),
            )
            updated = cur.rowcount > 0
        conn.commit()
    return updated


def db_get_target_session_context_saved_at(target_id: str) -> str | None:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT session_context->>'saved_at' FROM targets WHERE id = %s::uuid",
                (target_id,),
            )
            row = cur.fetchone()
    return row[0] if row else None


def db_create_story(model_id, title, result):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO stories (model_id, title, content, grade) VALUES (%s, %s, %s, NULL) RETURNING id",
                (model_id, title, result),
            )
            row = cur.fetchone()
        conn.commit()
    return str(row[0]) if row else None


def db_set_story_grade(story_id, grade):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE stories SET grade = %s WHERE id = %s::uuid",
                (grade, story_id),
            )
            updated = cur.rowcount
        conn.commit()
    return updated > 0


def db_set_story_pinned(story_id, value: bool):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE stories SET pinned = %s WHERE id = %s::uuid",
                (value, story_id),
            )
            updated = cur.rowcount
        conn.commit()
    return updated > 0


def db_set_movie_grade(movie_id, grade):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE movies SET grade = %s WHERE id = %s::uuid",
                (grade, movie_id),
            )
            updated = cur.rowcount
        conn.commit()
    return updated > 0


def db_get_story_text(story_id):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT content FROM stories WHERE id = %s", (story_id,))
            row = cur.fetchone()
    return row[0] if row else None


def db_get_story_title(story_id):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT title FROM stories WHERE id = %s", (story_id,))
            row = cur.fetchone()
    return row[0] if row else None


def db_get_story_export_data(story_id):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    s.content,
                    s.title,
                    s.model_id IS NOT NULL AS ai_generated,
                    s.manual_changed,
                    p.name AS platform_name,
                    m.name AS model_name,
                    m.body
                FROM stories s
                LEFT JOIN ai_models m ON m.id = s.model_id
                LEFT JOIN ai_platforms p ON p.id = m.platform_id
                WHERE s.id = %s
                """,
                (story_id,),
            )
            row = cur.fetchone()
    if not row:
        return None
    return {
        "text": row[0],
        "title": row[1] or "",
        "ai_generated": bool(row[2]),
        "manual_changed": bool(row[3]),
        "platform_name": row[4] or "",
        "model_name": row[5] or "",
        "model_body": row[6],
    }


def db_set_story_model(story_id, model_id):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE stories SET model_id = %s WHERE id = %s",
                (model_id, story_id),
            )
        conn.commit()
    return True


def db_update_story_content(story_id, content):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE stories SET content = %s, manual_changed = TRUE WHERE id = %s::uuid RETURNING id",
                (content, story_id),
            )
            row = cur.fetchone()
        conn.commit()
    return str(row[0]) if row else None


def db_upsert_story_draft(story_id, title, content):
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


def db_toggle_model(model_id: str):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE ai_models SET active = NOT active WHERE id = %s",
                (model_id,),
            )
        conn.commit()
    return True


def db_reorder_models(ids: list):
    with get_db() as conn:
        with conn.cursor() as cur:
            for idx, model_id in enumerate(ids, start=1):
                cur.execute(
                    'UPDATE ai_models SET "order" = %s WHERE id = %s', (idx, model_id)
                )
        conn.commit()
    return True


def db_set_model_grade(model_id: str, grade: str):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE ai_models SET grade = %s WHERE id = %s",
                (grade, model_id),
            )
        conn.commit()
    return True


def db_set_model_note(model_id: str, note: str):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE ai_models SET note = %s WHERE id = %s",
                (note, model_id),
            )
        conn.commit()
    return True


def db_set_model_body(model_id: str, body: dict):
    import json
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE ai_models SET body = %s::jsonb WHERE id = %s",
                (json.dumps(body), model_id),
            )
        conn.commit()
    return True


def db_set_target_config_body(target_id: str, body: dict):
    import json
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE targets SET config = %s::jsonb WHERE id = %s::uuid",
                (json.dumps(body), target_id),
            )
        conn.commit()
    return True


def db_set_target_active(target_id: str, active: bool) -> bool:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE targets SET active = %s WHERE id = %s::uuid",
                (active, target_id),
            )
        conn.commit()
    return True


def db_get_last_pipeline_run(pipeline, scheduled_only: bool = False):
    with get_db() as conn:
        with conn.cursor() as cur:
            if scheduled_only:
                cur.execute(
                    """
                    SELECT MAX(le.created_at)
                    FROM log_entries le
                    JOIN log l ON l.id = le.log_id
                    JOIN batches b ON b.id = l.batch_id
                    WHERE le.channel = %s
                      AND b.scheduled_at IS NOT NULL
                    """,
                    (pipeline,),
                )
            else:
                cur.execute(
                    """
                    SELECT MAX(le.created_at)
                    FROM log_entries le
                    WHERE le.channel = %s
                    """,
                    (pipeline,),
                )
            row = cur.fetchone()
    return row[0] if row and row[0] is not None else None


def db_get_graded_stories():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT title, content, grade FROM stories"
                " WHERE grade IN ('good', 'bad') AND pinned = TRUE"
                " ORDER BY CASE WHEN grade = 'good' THEN 0 ELSE 1 END, created_at DESC, id DESC"
            )
            rows = cur.fetchall()
    return [{"title": row[0], "content": row[1], "grade": row[2]} for row in rows]


def db_get_used_stories() -> list:
    used_cond = (
        "EXISTS ("
        "SELECT 1 FROM movies m"
        " WHERE m.story_id = s.id"
        " AND m.grade = 'good')"
    )
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT title, content FROM stories s"
                f" WHERE {used_cond}"
                f" ORDER BY created_at DESC, id DESC"
            )
            rows = cur.fetchall()
    return [{"title": row[0], "content": row[1]} for row in rows]
