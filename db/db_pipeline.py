import json
import psycopg2
import psycopg2.extras

from .connection import get_db
from common.statuses import _assert_known_status, PIPELINE_RESET_STATUS


def _get_story_source_batch_id(cur, story_id: str | None) -> str | None:
    if not story_id:
        return None
    cur.execute(
        """
        SELECT id::text
        FROM batches
        WHERE type = 'story' AND story_id = %s::uuid
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        (story_id,),
    )
    row = cur.fetchone()
    return row[0] if row else None


def db_create_planning_batch(scheduled_at):
    with get_db() as conn:
        with conn.cursor() as cur:
            if scheduled_at is not None:
                cur.execute(
                    """
                    SELECT id FROM batches
                    WHERE scheduled_at = %s
                      AND type        = 'planning'
                      AND status      != 'cancelled'
                    LIMIT 1
                    """,
                    (scheduled_at,),
                )
                if cur.fetchone():
                    return None
            cur.execute(
                """
                INSERT INTO batches (scheduled_at, type)
                VALUES (%s, 'planning')
                RETURNING id
                """,
                (scheduled_at,),
            )
            row = cur.fetchone()
        batch_id = str(row[0])
        db_set_batch_status(batch_id, 'pending', conn)
    return batch_id


def db_create_video_batch(batch_type, model_id=None, story_id=None):
    data = (
        json.dumps({"model_id": str(model_id)}) if model_id else None
    )
    with get_db() as conn:
        with conn.cursor() as cur:
            resolved_story_id = str(story_id) if story_id else None
            if batch_type == "movie" and not resolved_story_id:
                return None
            batch_id_source = _get_story_source_batch_id(cur, resolved_story_id)
            cur.execute(
                """
                INSERT INTO batches (type, story_id, batch_id_source, data)
                VALUES (%s, %s, %s, %s)
                RETURNING id
                """,
                (batch_type, resolved_story_id, batch_id_source, data),
            )
            row = cur.fetchone()
        batch_id = str(row[0])
        db_set_batch_status(batch_id, 'pending', conn)
    return batch_id


def db_create_story_batch(text_model_id: str | None = None):
    data = (
        json.dumps({"story_model_id": str(text_model_id)})
        if text_model_id
        else None
    )
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO batches (type, data)
                VALUES ('story', %s)
                RETURNING id
                """,
                (data,),
            )
            row = cur.fetchone()
        batch_id = str(row[0])
        db_set_batch_status(batch_id, 'pending', conn)
    return batch_id


def db_set_batch_story(batch_id, story_id):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE batches SET story_id = %s WHERE id = %s",
                (story_id, batch_id),
            )
        db_set_batch_status(batch_id, "ready", conn)
    return True


def db_set_batch_status(batch_id: str, status: str, conn=None):
    _assert_known_status(status)
    if conn is None:
        with get_db() as _conn:
            db_set_batch_status(batch_id, status, _conn)
        return
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE batches SET status = %s WHERE id = %s",
            (status, batch_id),
        )
    conn.commit()


def db_claim_batch_status(batch_id: str, from_status: str, to_status: str) -> bool:
    _assert_known_status(from_status)
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM batches WHERE id = %s AND status = %s FOR UPDATE",
                (batch_id, from_status),
            )
            row = cur.fetchone()
        if row is None:
            return False
        db_set_batch_status(batch_id, to_status, conn)
    return True


def db_cancel_orphaned_planning_batches():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id FROM batches
                WHERE status = 'pending'
                  AND type = 'planning'
                  AND scheduled_at IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM schedule
                      WHERE time_utc = to_char(
                          batches.scheduled_at AT TIME ZONE 'UTC', 'HH24:MI'
                      )
                  )
                FOR UPDATE
            """)
            batch_ids = [str(r[0]) for r in cur.fetchall()]
        for batch_id in batch_ids:
            db_set_batch_status(batch_id, "cancelled", conn)
    return batch_ids


def db_claim_unused_movie_for_batch(batch_id: str) -> dict | None:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id
                FROM batches
                WHERE id = %s::uuid
                  AND type = 'planning'
                  AND status = 'pending'
                FOR UPDATE
                """,
                (batch_id,),
            )
            if cur.fetchone() is None:
                return None

            cur.execute(
                """
                SELECT m.id::text, m.story_id::text
                FROM movies m
                WHERE m.grade = 'good'
                  AND m.used = B'0'
                ORDER BY m.created_at ASC, m.id ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
                """
            )
            movie_row = cur.fetchone()
            if movie_row is None:
                return None
            movie_id, story_id = movie_row

            cur.execute(
                """
                SELECT b.id::text
                FROM batches b
                WHERE b.type = 'movie'
                  AND b.movie_id = %s::uuid
                ORDER BY b.created_at DESC, b.id DESC
                LIMIT 1
                """,
                (movie_id,),
            )
            source_row = cur.fetchone()
            source_batch_id = source_row[0] if source_row else None

            cur.execute(
                "UPDATE movies SET used = B'1' WHERE id = %s::uuid",
                (movie_id,),
            )
            cur.execute(
                """
                UPDATE batches
                SET movie_id = %s::uuid,
                    story_id = %s::uuid,
                    batch_id_source = %s::uuid,
                    status = 'ready'
                WHERE id = %s::uuid
                """,
                (movie_id, story_id, source_batch_id, batch_id),
            )
        conn.commit()
    return {
        "movie_id": movie_id,
        "story_id": story_id,
        "batch_id_source": source_batch_id,
    }


def db_get_movie_pool_count(good_only: bool = False) -> int:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM movies
                WHERE used = B'0'
                  AND (%s::boolean = FALSE OR grade = 'good')
                """,
                (good_only,),
            )
            return cur.fetchone()[0]


def db_create_transcode_batches() -> list[str]:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO batches (type, movie_id, batch_id_source, scheduled_at, story_id)
                SELECT
                    'transcode',
                    m.id,
                    bs.id,
                    bs.scheduled_at,
                    bs.story_id
                FROM movies m
                LEFT JOIN batches bt
                    ON bt.movie_id = m.id
                   AND bt.type = 'transcode'
                   AND bt.status IN ('pending', 'processing')
                LEFT JOIN LATERAL (
                    SELECT b.id, b.scheduled_at, b.story_id
                    FROM batches b
                    WHERE b.movie_id = m.id
                      AND b.type = 'planning'
                    ORDER BY b.created_at DESC, b.id DESC
                    LIMIT 1
                ) bs ON TRUE
                WHERE m.used = B'1'
                  AND m.transcoded = B'0'
                  AND bt.id IS NULL
                RETURNING id::text
                """
            )
            rows = cur.fetchall()
        for row in rows:
            db_set_batch_status(str(row[0]), 'pending', conn)
        conn.commit()
    return [str(row[0]) for row in rows]


def db_create_publish_batches() -> list[str]:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO batches (type, movie_id, story_id, batch_id_source, scheduled_at)
                SELECT
                    'publish',
                    tc.movie_id,
                    tc.story_id,
                    tc.id,
                    tc.scheduled_at
                FROM batches tc
                WHERE tc.type = 'transcode'
                  AND tc.status NOT IN ('pending', 'processing')
                  AND (
                      tc.scheduled_at IS NULL
                      OR tc.scheduled_at <= clock_timestamp()
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM batches pb
                      WHERE pb.type = 'publish'
                        AND pb.batch_id_source = tc.id
                  )
                RETURNING id::text
                """
            )
            rows = [str(r[0]) for r in cur.fetchall()]

        for batch_id in rows:
            db_set_batch_status(batch_id, 'pending', conn)
        conn.commit()
    return rows


def db_get_actionable_batches():
    with get_db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, type, status, created_at, scheduled_at
                FROM batches
                WHERE status = 'pending'
                   OR status = 'generated'
                   OR (type = 'movie' AND status = 'generating')
                   OR status IN ('video_generating', 'video_pending')
                   OR status LIKE '%.pending'
                   OR status LIKE '%.published'
                ORDER BY created_at ASC, id ASC
            """)
            rows = cur.fetchall()
    return [dict(r) for r in rows]


def db_get_batch_by_id(batch_id):
    with get_db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT b.id, b.scheduled_at, b.type, b.batch_id_source, b.story_id,
                       m.url AS video_url, b.status, b.data,
                       m.model_id AS video_model_id,
                       b.movie_id, b.title
                FROM batches b
                LEFT JOIN movies m ON m.id = b.movie_id
                WHERE b.id = %s
            """,
                (batch_id,),
            )
            row = cur.fetchone()
    return dict(row) if row else None


def db_is_batch_scheduled(scheduled_at, batch_type="planning"):
    if scheduled_at is None:
        return True
    if batch_type not in ("planning", "publish"):
        return True
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT 1 FROM schedule
                    WHERE time_utc = to_char(%s::timestamptz AT TIME ZONE 'UTC', 'HH24:MI')
                )
                """,
                (scheduled_at,),
            )
            row = cur.fetchone()
            return bool(row[0])


def db_reset_stalled_batches() -> list[dict]:
    typed_resets = [
        ("story", "generating", "pending"),
        ("transcode", "processing", "pending"),
    ]
    affected = []
    with get_db() as conn:
        for batch_type, old_status, new_status in typed_resets:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM batches WHERE type = %s AND status = %s FOR UPDATE",
                    (batch_type, old_status),
                )
                ids = [str(r[0]) for r in cur.fetchall()]
            for batch_id in ids:
                db_set_batch_status(batch_id, new_status, conn)
                affected.append(
                    {"id": batch_id, "old_status": old_status, "new_status": new_status}
                )

        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT status FROM batches WHERE status LIKE '%.posting'"
            )
            posting_statuses = [r[0] for r in cur.fetchall()]

        for posting_status in posting_statuses:
            pending_status = posting_status[: -len(".posting")] + ".pending"
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM batches WHERE status = %s FOR UPDATE",
                    (posting_status,),
                )
                ids = [str(r[0]) for r in cur.fetchall()]
            for batch_id in ids:
                db_set_batch_status(batch_id, pending_status, conn)
                affected.append(
                    {
                        "id": batch_id,
                        "old_status": posting_status,
                        "new_status": pending_status,
                    }
                )

    return affected


def _video_reset_status(batch_data) -> str:
    """Статус для перезапуска video: подхват URL/handshake вместо нового submit."""
    if not isinstance(batch_data, dict):
        return PIPELINE_RESET_STATUS['video']
    if batch_data.get('generated_video_url'):
        return 'generated'
    if (
        batch_data.get('request_id')
        and batch_data.get('status_url')
        and batch_data.get('response_url')
    ):
        return 'generating'
    return PIPELINE_RESET_STATUS['video']


def db_reset_batch_pipeline(batch_id: str, pipeline: str) -> bool:
    if pipeline not in PIPELINE_RESET_STATUS:
        return False
    batch = db_get_batch_by_id(batch_id)
    if not batch:
        return False
    if pipeline == 'video':
        target_status = _video_reset_status(batch.get('data') or {})
    else:
        target_status = PIPELINE_RESET_STATUS[pipeline]
    db_set_batch_status(batch_id, target_status)
    return True


def db_get_active_text_models():
    with get_db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT p.url AS platform_url, p.env_key_name, m.url AS model_url,
                       m.body, m.name, m.id
                FROM ai_models m
                JOIN ai_platforms p ON p.id = m.platform_id
                WHERE m.active = TRUE AND m.type = 'text'
                  AND (m.grade IS NULL OR m.grade != 'rejected')
                ORDER BY m."order"
            """)
            rows = cur.fetchall()
    result = []
    for row in rows:
        result.append(
            {
                "platform_url": row["platform_url"],
                "env_key_name": row["env_key_name"],
                "model_url": row["model_url"],
                "body_tpl": row["body"] if isinstance(row["body"], dict) else {},
                "name": row["name"],
                "id": str(row["id"]),
            }
        )
    return result


def db_get_text_model_by_id(model_id: str):
    with get_db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT p.url AS platform_url, p.env_key_name, m.url AS model_url,
                       m.body, m.name, m.id
                FROM ai_models m
                JOIN ai_platforms p ON p.id = m.platform_id
                WHERE m.id = %s AND m.type = 'text'
            """,
                (model_id,),
            )
            row = cur.fetchone()
    if not row:
        return None
    return {
        "platform_url": row["platform_url"],
        "env_key_name": row["env_key_name"],
        "model_url": row["model_url"],
        "body_tpl": row["body"] if isinstance(row["body"], dict) else {},
        "name": row["name"],
        "id": str(row["id"]),
    }


def _fetch_allowed_durations(cur, model_ids):
    """Вспомогательная функция: возвращает dict {model_id_str: [int, ...]}."""
    if not model_ids:
        return {}
    ids_str = [str(mid) for mid in model_ids]
    cur.execute(
        """
        SELECT model_id::text, ARRAY_AGG(duration ORDER BY duration ASC) AS durations
        FROM model_durations
        WHERE model_id::text = ANY(%s)
        GROUP BY model_id
    """,
        (ids_str,),
    )
    return {row["model_id"]: list(row["durations"]) for row in cur.fetchall()}


def db_get_active_video_models():
    with get_db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT p.url AS platform_url, p.name AS platform_name,
                       m.url AS model_url, m.body, m.name, m.id
                FROM ai_models m
                JOIN ai_platforms p ON p.id = m.platform_id
                WHERE m.active = TRUE AND m.type = 'text-to-video'
                ORDER BY m."order"
            """)
            rows = cur.fetchall()
            model_ids = [row["id"] for row in rows]
            durations_map = _fetch_allowed_durations(cur, model_ids)
    result = []
    for row in rows:
        mid = str(row["id"])
        result.append(
            {
                "platform_url": row["platform_url"],
                "platform_name": row["platform_name"],
                "model_url": row["model_url"],
                "body_tpl": row["body"] if isinstance(row["body"], dict) else {},
                "name": row["name"],
                "id": mid,
                "submit_url": f"{row['platform_url']}/{row['model_url']}",
                "allowed_durations": durations_map.get(mid, [0]),
            }
        )
    return result


def db_get_video_model_by_id(model_id: str):
    with get_db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT p.url AS platform_url, p.name AS platform_name,
                       m.url AS model_url, m.body, m.name, m.id
                FROM ai_models m
                JOIN ai_platforms p ON p.id = m.platform_id
                WHERE m.id = %s AND m.type = 'text-to-video'
            """,
                (model_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            durations_map = _fetch_allowed_durations(cur, [row["id"]])
    platform_url = row["platform_url"]
    platform_name = row["platform_name"]
    model_url = row["model_url"]
    mid = str(row["id"])
    return {
        "platform_url": platform_url,
        "platform_name": platform_name,
        "model_url": model_url,
        "body_tpl": row["body"] if isinstance(row["body"], dict) else {},
        "name": row["name"],
        "id": mid,
        "submit_url": f"{platform_url}/{model_url}",
        "allowed_durations": durations_map.get(mid, [0]),
    }


def db_set_batch_title(batch_id: str, title: str) -> None:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE batches SET title = %s WHERE id = %s",
                (title, batch_id),
            )
        conn.commit()


def db_set_batch_vkvideo_clip_url(batch_id: str, clip_url: str) -> None:
    """Сохраняет ссылку на клип VK Видео в batches.data['vkvideo_clip_url']."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE batches
                   SET data = COALESCE(data::jsonb, '{}'::jsonb)
                           || jsonb_build_object('vkvideo_clip_url', %s::text)
                 WHERE id = %s
                """,
                (clip_url, batch_id),
            )
        conn.commit()


def db_get_batch_vkvideo_clip_url(batch_id: str) -> str:
    """Читает ссылку на клип VK Видео из batches.data['vkvideo_clip_url']."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT data->>'vkvideo_clip_url' FROM batches WHERE id = %s",
                (batch_id,),
            )
            row = cur.fetchone()
    return (row[0] or '') if row else ''
