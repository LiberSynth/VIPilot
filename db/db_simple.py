import json
import psycopg2
import psycopg2.extras

from .connection import get_db
from common.exceptions import FatalError


def db_insert_log(pipeline, message, status="info", batch_id=None):
    """Вставляет запись в таблицу log. Возвращает log_id (str).

    Если batch_id задан и status == 'running' — сначала проверяет, есть ли уже
    'running'-запись для того же (batch_id, pipeline). Если есть — возвращает её id
    без создания новой записи. Это предотвращает дублирование логов после рестарта.
    """
    with get_db() as conn:
        with conn.cursor() as cur:
            if batch_id is not None and status == 'running':
                cur.execute(
                    "SELECT id FROM log WHERE batch_id = %s AND pipeline = %s AND status = 'running'"
                    " ORDER BY id DESC LIMIT 1",
                    (batch_id, pipeline),
                )
                row = cur.fetchone()
                if row:
                    return str(row[0])
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


def db_insert_log_entry(log_id, message, level):
    import common.environment as environment

    if not environment.asserted_log_entry.get():
        raise FatalError("Нарушение конвенции логирования")
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO log_entries (log_id, message, level) VALUES (%s, %s, %s)",
                (log_id, message, level),
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
                'SELECT id, name, aspect_ratio_x, aspect_ratio_y, transcode, config, slug FROM targets WHERE active = TRUE ORDER BY "order", name'
            )
            rows = cur.fetchall()
    return [
        {
            "id": str(row[0]),
            "name": row[1],
            "aspect_ratio_x": row[2],
            "aspect_ratio_y": row[3],
            "transcode": bool(row[4]),
            "config": row[5] or {},
            "slug": row[6] or "",
        }
        for row in rows
    ]


def db_update_target_aspect_ratio(target_id, x, y):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE targets SET aspect_ratio_x = %s, aspect_ratio_y = %s WHERE id = %s",
                (x, y, target_id),
            )
        conn.commit()
    return True


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
                "SELECT id::text, name, active, aspect_ratio_x, aspect_ratio_y, transcode, config "
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


def db_get_story_model_info(story_id):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT p.name AS platform_name, m.name AS model_name, m.body
                FROM stories s
                JOIN ai_models m ON m.id = s.model_id
                JOIN ai_platforms p ON p.id = m.platform_id
                WHERE s.id = %s
                """,
                (story_id,),
            )
            row = cur.fetchone()
    if not row:
        return None
    return {"platform_name": row[0], "model_name": row[1], "body": row[2]}


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


def db_get_distinct_batch_statuses():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT status FROM batches")
            rows = cur.fetchall()
    return {row[0] for row in rows}


def db_get_last_pipeline_run(pipeline):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT MAX(created_at) FROM log WHERE pipeline = %s",
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


def db_get_used_stories(approve_movies: bool) -> list:
    if approve_movies:
        used_cond = (
            "EXISTS ("
            "SELECT 1 FROM batches b"
            " JOIN movies m ON m.id = b.movie_id"
            " WHERE b.story_id = s.id AND b.movie_id IS NOT NULL"
            " AND m.grade = 'good')"
        )
    else:
        used_cond = (
            "EXISTS ("
            "SELECT 1 FROM batches b"
            " WHERE b.story_id = s.id AND b.movie_id IS NOT NULL)"
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
