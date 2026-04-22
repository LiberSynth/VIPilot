import json
import psycopg2
import psycopg2.extras

from .connection import get_db
from common.statuses import _assert_known_status, PIPELINE_RESET_STATUS


def db_ensure_batch(scheduled_at):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id FROM batches
                WHERE scheduled_at = %s
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
                VALUES (%s, 'slot')
                RETURNING id
                """,
                (scheduled_at,),
            )
            row = cur.fetchone()
        conn.commit()
    return str(row[0]) if row else None


def db_create_adhoc_batch():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO batches (scheduled_at, type)
                VALUES (NULL, 'adhoc')
                RETURNING id
            """)
            row = cur.fetchone()
        conn.commit()
    return str(row[0]) if row else None


def db_create_video_batch(batch_type, movie_model_id=None, story_id=None):
    data = (
        json.dumps({"movie_model_id": str(movie_model_id)}) if movie_model_id else None
    )
    with get_db() as conn:
        with conn.cursor() as cur:
            if story_id:
                cur.execute(
                    """
                    INSERT INTO batches (type, story_id, data)
                    VALUES (%s, %s, %s)
                    RETURNING id
                    """,
                    (batch_type, story_id, data),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO batches (type, data)
                    VALUES (%s, %s)
                    RETURNING id
                    """,
                    (batch_type, data),
                )
            row = cur.fetchone()
        batch_id = str(row[0])
        db_set_batch_status(batch_id, 'pending', conn)
    return batch_id


def db_create_story_probe_batch(text_model_id):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO batches (type, data)
                VALUES ('story_probe', %s)
                RETURNING id
                """,
                (json.dumps({"story_model_id": str(text_model_id)}),),
            )
            row = cur.fetchone()
        batch_id = str(row[0])
        db_set_batch_status(batch_id, 'pending', conn)
    return batch_id


def db_create_story_autogenerate_batch():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO batches (type)
                VALUES ('story_probe')
                RETURNING id
            """)
            row = cur.fetchone()
        batch_id = str(row[0])
        db_set_batch_status(batch_id, 'pending', conn)
    return batch_id


def db_update_batch_current_movie_model_id(batch_id, model_id):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE batches
                   SET data = COALESCE(data::jsonb, '{}'::jsonb)
                           || jsonb_build_object('current_movie_model_id', %s::text)
                 WHERE id = %s
                """,
                (str(model_id), batch_id),
            )
        conn.commit()


def db_finalize_story_probe(batch_id, story_id):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE batches SET story_id = %s WHERE id = %s",
                (story_id, batch_id),
            )
        db_set_batch_status(batch_id, "story_probe", conn)


def db_set_batch_story(batch_id, story_id):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE batches SET story_id = %s WHERE id = %s",
                (story_id, batch_id),
            )
        db_set_batch_status(batch_id, "story_ready", conn)


def db_link_batch_story_only(batch_id, story_id):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE batches SET story_id = %s WHERE id = %s",
                (story_id, batch_id),
            )
        conn.commit()


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


def db_cancel_orphaned_slot_batches():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id FROM batches
                WHERE (
                    status = 'transcode_ready'
                    OR status LIKE '%.published'
                    OR status LIKE '%.pending'
                )
                  AND type = 'slot'
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


def db_reset_batch_to_pending(batch_id):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE batches SET story_id = NULL, movie_id = NULL WHERE id = %s",
                (batch_id,),
            )
        db_set_batch_status(batch_id, "pending", conn)


def db_claim_unused_story_for_batch(batch_id: str, grade_required: bool) -> dict | None:
    with get_db() as conn:
        with conn.cursor() as cur:
            if grade_required:
                cur.execute("""
                    SELECT id::text, title, content
                    FROM stories
                    WHERE id NOT IN (
                        SELECT story_id
                        FROM batches
                        WHERE story_id IS NOT NULL AND type != 'story_probe'
                    )
                      AND grade = 'good'
                    ORDER BY created_at ASC, id ASC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                """)
            else:
                cur.execute("""
                    SELECT id::text, title, content
                    FROM stories
                    WHERE id NOT IN (
                        SELECT story_id
                        FROM batches
                        WHERE story_id IS NOT NULL AND type != 'story_probe'
                    )
                    ORDER BY created_at ASC, id ASC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                """)
            row = cur.fetchone()
            if not row:
                return None
            story_id = row[0]
            cur.execute(
                "UPDATE batches SET story_id = %s::uuid WHERE id = %s::uuid",
                (story_id, batch_id),
            )
        db_set_batch_status(batch_id, 'story_ready', conn)
    return {"id": story_id, "title": row[1] or "", "content": row[2] or ""}


def db_claim_donor_batch(batch_id: str, good_only: bool = False) -> None:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT claim_donor_batch(%s::uuid, %s)", (batch_id, good_only))
        conn.commit()


def db_set_batch_story_ready_from_donor(
    batch_id: str, donor_batch_id: str, donor_story_id: str | None
):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE batches
                SET story_id = COALESCE(story_id, %s),
                    data     = COALESCE(data, '{}'::jsonb) || jsonb_build_object('donor_batch_id', %s)
                WHERE id = %s
                RETURNING story_id
                """,
                (donor_story_id, donor_batch_id, batch_id),
            )
            row = cur.fetchone()
            used_story_id = row[0] if row else None
            if (
                donor_story_id
                and used_story_id
                and str(used_story_id) == str(donor_story_id)
            ):
                cur.execute(
                    "UPDATE batches SET story_id = NULL WHERE id = %s::uuid",
                    (donor_batch_id,),
                )
        db_set_batch_status(batch_id, 'story_ready', conn)


def db_transfer_donor_movie(donor_batch_id: str, batch_id: str) -> str | None:
    """Переносит movie_id из батча-донора в целевой батч.

    Побочный эффект: обнуляет movie_id у батча-донора и переводит целевой
    батч в статус video_ready или transcode_ready.

    Возвращает donor_batch_id: str если перенос выполнен, None если донор не найден.
    """
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT b.id, b.movie_id,
                       (m.transcoded_data IS NOT NULL) AS has_transcoded,
                       (m.raw_data IS NOT NULL) AS has_raw
                FROM batches b
                JOIN movies m ON m.id = b.movie_id
                WHERE b.id = %s::uuid
            """,
                (donor_batch_id,),
            )
            donor = cur.fetchone()
            if not donor:
                return None

            donor_id, donor_movie_id, has_transcoded, has_raw = donor

            new_status = "transcode_ready" if has_transcoded else "video_ready"
            cur.execute(
                "UPDATE batches SET movie_id = %s WHERE id = %s::uuid",
                (donor_movie_id, batch_id),
            )
            cur.execute(
                "UPDATE batches SET movie_id = NULL WHERE id = %s::uuid",
                (donor_batch_id,),
            )
        db_set_batch_status(batch_id, new_status, conn)
    return str(donor_id)


def db_get_donor_count(good_only: bool = False) -> int:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT get_donor_count(%s)", (good_only,))
            return cur.fetchone()[0]


def db_get_actionable_batches():
    with get_db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, status, created_at, scheduled_at
                FROM batches
                WHERE status IN (
                    'pending', 'story_generating',
                    'story_ready', 'video_generating', 'video_pending',
                    'video_ready', 'transcoding',
                    'transcode_ready'
                )
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
                SELECT b.id, b.scheduled_at, b.type, b.story_id,
                       m.url AS video_url, b.status, b.data,
                       m.model_id AS video_model_id,
                       b.movie_id
                FROM batches b
                LEFT JOIN movies m ON m.id = b.movie_id
                WHERE b.id = %s
            """,
                (batch_id,),
            )
            row = cur.fetchone()
    return dict(row) if row else None


def db_get_batches_with_unknown_status(known_statuses):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, status FROM batches")
            rows = cur.fetchall()
    return {row[0]: row[1] for row in rows if row[1] not in known_statuses}


def db_is_batch_scheduled(scheduled_at, batch_type="slot"):
    if batch_type != "slot" or scheduled_at is None:
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
    static_resets = [
        ("story_generating", "pending"),
        ("video_generating", "story_ready"),
    ]
    affected = []
    with get_db() as conn:
        for old_status, new_status in static_resets:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM batches WHERE status = %s FOR UPDATE",
                    (old_status,),
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


def db_reset_batch_pipeline(batch_id: str, pipeline: str) -> bool:
    target_status = PIPELINE_RESET_STATUS.get(pipeline)
    if not target_status:
        return False
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
