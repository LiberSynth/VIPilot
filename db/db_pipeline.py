import json
import psycopg2
import psycopg2.extras

from .connection import get_db
from common.statuses import _assert_known_status, PIPELINE_RESET_STATUS


def db_ensure_batch(scheduled_at):
    try:
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
    except Exception as e:
        print(f"[DB] Ошибка db_ensure_batch: {e}")
        return None


def db_create_adhoc_batch():
    try:
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
    except Exception as e:
        print(f"[DB] Ошибка db_create_adhoc_batch: {e}")
        return None


def db_create_probe_batch(video_model_id, story_id=None):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO movies (model_id)
                    VALUES (%s)
                    RETURNING id
                """,
                    (video_model_id,),
                )
                movie_row = cur.fetchone()
                movie_id = movie_row[0]
                if story_id:
                    cur.execute(
                        """
                        INSERT INTO batches
                            (status, type, movie_id, story_id)
                        VALUES ('pending', 'movie_probe', %s, %s)
                        RETURNING id
                    """,
                        (movie_id, story_id),
                    )
                else:
                    cur.execute(
                        """
                        INSERT INTO batches
                            (status, type, movie_id)
                        VALUES ('pending', 'movie_probe', %s)
                        RETURNING id
                    """,
                        (movie_id,),
                    )
                row = cur.fetchone()
            conn.commit()
        return str(row[0]) if row else None
    except Exception as e:
        print(f"[DB] Ошибка db_create_probe_batch: {e}")
        return None


def db_create_story_probe_batch(text_model_id):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO batches
                        (status, type, data)
                    VALUES ('pending', 'story_probe', %s)
                    RETURNING id
                """,
                    (json.dumps({"probe_model_id": str(text_model_id)}),),
                )
                row = cur.fetchone()
            conn.commit()
        return str(row[0]) if row else None
    except Exception as e:
        print(f"[DB] Ошибка db_create_story_probe_batch: {e}")
        return None


def db_create_story_generate_batch():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO batches
                        (status, type)
                    VALUES ('pending', 'story_probe')
                    RETURNING id
                """)
                row = cur.fetchone()
            conn.commit()
        return str(row[0]) if row else None
    except Exception as e:
        print(f"[DB] Ошибка db_create_story_generate_batch: {e}")
        return None


def db_set_batch_story_probe(batch_id, story_id):
    _assert_known_status("story_probe")
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE batches SET status = 'story_probe', story_id = %s, completed_at = now() WHERE id = %s",
                    (story_id, batch_id),
                )
            conn.commit()
    except Exception as e:
        print(f"[DB] Ошибка db_set_batch_story_probe: {e}")


def db_set_batch_story(batch_id, story_id):
    _assert_known_status("story_ready")
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE batches SET story_id = %s, status = 'story_ready' WHERE id = %s",
                    (story_id, batch_id),
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_set_batch_story: {e}")
        return False


def db_set_batch_story_id(batch_id, story_id):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE batches SET story_id = %s WHERE id = %s",
                    (story_id, batch_id),
                )
            conn.commit()
    except Exception as e:
        print(f"[DB] Ошибка db_set_batch_story_id: {e}")


def db_set_batch_status(batch_id: str, status: str) -> bool:
    _assert_known_status(status)
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE batches SET status = %s WHERE id = %s",
                    (status, batch_id),
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_set_batch_status: {e}")
        return False


def db_claim_batch_status(batch_id: str, from_status: str, to_status: str) -> bool:
    _assert_known_status(from_status)
    _assert_known_status(to_status)
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE batches SET status = %s
                    WHERE id = %s AND status = %s
                    RETURNING id
                    """,
                    (to_status, batch_id, from_status),
                )
                row = cur.fetchone()
            conn.commit()
        return row is not None
    except Exception as e:
        print(f"[DB] Ошибка db_claim_batch_status: {e}")
        return False


def db_set_batch_story_generating_by_id(batch_id):
    _assert_known_status("story_generating")
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE batches SET status = 'story_generating'
                    WHERE id = %s AND status = 'pending'
                """,
                    (batch_id,),
                )
                n = cur.rowcount
            conn.commit()
        return n > 0
    except Exception as e:
        print(f"[DB] Ошибка db_set_batch_story_generating_by_id: {e}")
        return False


def db_set_batch_video_generating_by_id(batch_id):
    _assert_known_status("video_generating")
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE batches SET status = 'video_generating'
                    WHERE id = %s AND status = 'story_ready'
                """,
                    (batch_id,),
                )
                n = cur.rowcount
            conn.commit()
        return n > 0
    except Exception as e:
        print(f"[DB] Ошибка db_set_batch_video_generating_by_id: {e}")
        return False


def db_set_batch_transcoding_by_id(batch_id):
    _assert_known_status("transcoding")
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE batches SET status = 'transcoding'
                    WHERE id = %s AND status = 'video_ready'
                """,
                    (batch_id,),
                )
                n = cur.rowcount
            conn.commit()
        return n > 0
    except Exception as e:
        print(f"[DB] Ошибка db_set_batch_transcoding_by_id: {e}")
        return False


def db_cancel_waiting_batches():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE batches
                    SET status = 'cancelled'
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
                    RETURNING id
                """)
                rows = cur.fetchall()
            conn.commit()
        return [str(r[0]) for r in rows]
    except Exception as e:
        print(f"[DB] Ошибка db_cancel_waiting_batches: {e}")
        return []


def db_set_batch_story_ready_from_error(batch_id):
    _assert_known_status("story_ready")
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE batches SET status = 'story_ready' WHERE id = %s",
                    (batch_id,),
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_set_batch_story_ready_from_error: {e}")
        return False


def db_set_batch_pending(batch_id):
    _assert_known_status("pending")
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """UPDATE batches
                       SET status = 'pending', story_id = NULL,
                           movie_id = NULL
                       WHERE id = %s""",
                    (batch_id,),
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_set_batch_pending: {e}")
        return False


def db_set_batch_movie_probe(batch_id):
    _assert_known_status("movie_probe")
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE batches SET status = 'movie_probe', completed_at = now() WHERE id = %s",
                    (batch_id,),
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_set_batch_movie_probe: {e}")
        return False


def db_set_batch_published(batch_id):
    _assert_known_status("published")
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE batches SET status = 'published', completed_at = now() WHERE id = %s",
                    (batch_id,),
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_set_batch_published: {e}")
        return False


def db_set_batch_published_partially(batch_id):
    _assert_known_status("published_partially")
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE batches SET status = 'published_partially', completed_at = now() WHERE id = %s",
                    (batch_id,),
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_set_batch_published_partially: {e}")
        return False


def db_claim_unused_story_for_batch(batch_id: str, grade_required: bool) -> dict | None:
    _assert_known_status("story_ready")
    try:
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
                        ORDER BY created_at ASC
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
                        ORDER BY created_at ASC
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED
                    """)
                row = cur.fetchone()
                if not row:
                    return None
                story_id = row[0]
                cur.execute(
                    "UPDATE batches SET story_id = %s::uuid, status = 'story_ready' WHERE id = %s::uuid",
                    (story_id, batch_id),
                )
            conn.commit()
        return {"id": story_id, "title": row[1] or "", "content": row[2] or ""}
    except Exception as e:
        print(f"[DB] Ошибка db_claim_unused_story_for_batch: {e}")
        return None


def db_claim_donor_batch(batch_id: str) -> None:
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT claim_donor_batch(%s::uuid)", (batch_id,))
            conn.commit()
    except Exception as e:
        print(f"[DB] Ошибка db_claim_donor_batch: {e}")


def db_set_batch_story_ready_from_donor(
    batch_id: str, donor_batch_id: str, donor_story_id: str | None
) -> bool:
    _assert_known_status("story_ready")
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE batches
                    SET status   = 'story_ready',
                        story_id = COALESCE(story_id, %s),
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
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_set_batch_story_ready_from_donor: {e}")
        return False


def db_get_movie_from_donor(donor_batch_id: str, batch_id: str) -> str | None:
    try:
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
                    """
                    UPDATE batches
                    SET movie_id = %s,
                        status   = %s
                    WHERE id = %s::uuid AND movie_id IS NULL
                """,
                    (donor_movie_id, new_status, batch_id),
                )
                if cur.rowcount == 0:
                    return None

                cur.execute(
                    "UPDATE batches SET movie_id = NULL WHERE id = %s::uuid",
                    (donor_batch_id,),
                )

            conn.commit()
        return str(donor_id)
    except Exception as e:
        print(f"[DB] Ошибка db_get_movie_from_donor: {e}")
        return None


def db_get_donor_count() -> int:
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT get_donor_count()")
                return cur.fetchone()[0]
    except Exception as e:
        print(f"[DB] Ошибка db_get_donor_count: {e}")
        return 0


def db_get_actionable_batches():
    try:
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
                    ORDER BY created_at ASC
                """)
                rows = cur.fetchall()
        return [dict(r) for r in rows]
    except Exception as e:
        print(f"[DB] Ошибка db_get_actionable_batches: {e}")
        return []


def db_get_batch_by_id(batch_id):
    try:
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
    except Exception as e:
        print(f"[DB] Ошибка db_get_batch_by_id: {e}")
        return None


def db_get_batches_with_unknown_status(known_statuses):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, status FROM batches")
                rows = cur.fetchall()
        return {row[0]: row[1] for row in rows if row[1] not in known_statuses}
    except Exception as e:
        print(f"[DB] Ошибка db_get_batches_with_unknown_status: {e}")
        return {}


def db_is_batch_scheduled(scheduled_at, batch_type="slot"):
    if batch_type != "slot" or scheduled_at is None:
        return True
    try:
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
    except Exception as e:
        print(f"[DB] Ошибка db_is_batch_scheduled: {e}")
        return True


def db_reset_stalled_batches() -> list[dict]:
    static_resets = [
        ("story_generating", "pending"),
        ("video_generating", "story_ready"),
    ]
    affected = []
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                for old_status, new_status in static_resets:
                    cur.execute(
                        """
                        UPDATE batches
                           SET status = %s
                         WHERE status = %s
                        RETURNING id
                        """,
                        (new_status, old_status),
                    )
                    rows = cur.fetchall()
                    for row in rows:
                        affected.append(
                            {
                                "id": str(row[0]),
                                "old_status": old_status,
                                "new_status": new_status,
                            }
                        )

                cur.execute("""
                    SELECT DISTINCT status FROM batches
                    WHERE status LIKE '%.posting'
                """)
                posting_rows = cur.fetchall()
                for (posting_status,) in posting_rows:
                    pending_status = posting_status[: -len(".posting")] + ".pending"
                    cur.execute(
                        """
                        UPDATE batches
                           SET status = %s
                         WHERE status = %s
                        RETURNING id
                        """,
                        (pending_status, posting_status),
                    )
                    rows = cur.fetchall()
                    for row in rows:
                        affected.append(
                            {
                                "id": str(row[0]),
                                "old_status": posting_status,
                                "new_status": pending_status,
                            }
                        )

            conn.commit()
    except Exception as e:
        print(f"[DB] Ошибка db_reset_stalled_batches: {e}")
    return affected


def db_reset_batch_pipeline(batch_id: str, pipeline: str) -> bool:
    target_status = PIPELINE_RESET_STATUS.get(pipeline)
    if not target_status:
        return False
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE batches SET status = %s WHERE id = %s",
                    (target_status, batch_id),
                )
                updated = cur.rowcount
            conn.commit()
        return updated > 0
    except Exception as e:
        print(f"[DB] Ошибка db_reset_batch_pipeline: {e}")
        return False


def db_get_active_text_model():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT p.url, m.url, m.body, m.name, m.id
                    FROM ai_models m
                    JOIN ai_platforms p ON p.id = m.platform_id
                    WHERE m.active = TRUE AND m.type = 'text'
                      AND (m.grade IS NULL OR m.grade != 'rejected')
                    ORDER BY m."order"
                    LIMIT 1
                """)
                row = cur.fetchone()
        if not row:
            return None, None, None, None, None
        return (
            row[0],
            row[1],
            row[2] if isinstance(row[2], dict) else {},
            row[3],
            str(row[4]),
        )
    except Exception as e:
        print(f"[DB] Ошибка db_get_active_text_model: {e}")
        return None, None, None, None, None


def db_get_active_text_models():
    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT p.url AS platform_url, m.url AS model_url,
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
                    "model_url": row["model_url"],
                    "body_tpl": row["body"] if isinstance(row["body"], dict) else {},
                    "name": row["name"],
                    "id": str(row["id"]),
                }
            )
        return result
    except Exception as e:
        print(f"[DB] Ошибка db_get_active_text_models: {e}")
        return []


def db_get_text_model_by_id(model_id: str):
    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT p.url AS platform_url, m.url AS model_url,
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
            "model_url": row["model_url"],
            "body_tpl": row["body"] if isinstance(row["body"], dict) else {},
            "name": row["name"],
            "id": str(row["id"]),
        }
    except Exception as e:
        print(f"[DB] Ошибка db_get_text_model_by_id: {e}")
        return None


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
    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT p.url AS platform_url, m.url AS model_url,
                           m.body, m.name, m.id
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
                    "model_url": row["model_url"],
                    "body_tpl": row["body"] if isinstance(row["body"], dict) else {},
                    "name": row["name"],
                    "id": mid,
                    "submit_url": f"{row['platform_url']}/{row['model_url']}",
                    "allowed_durations": durations_map.get(mid, [0]),
                }
            )
        return result
    except Exception as e:
        print(f"[DB] Ошибка db_get_active_video_models: {e}")
        return []


def db_get_video_model_by_id(model_id: str):
    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT p.url AS platform_url, m.url AS model_url,
                           m.body, m.name, m.id
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
        model_url = row["model_url"]
        mid = str(row["id"])
        return {
            "platform_url": platform_url,
            "model_url": model_url,
            "body_tpl": row["body"] if isinstance(row["body"], dict) else {},
            "name": row["name"],
            "id": mid,
            "submit_url": f"{platform_url}/{model_url}",
            "allowed_durations": durations_map.get(mid, [0]),
        }
    except Exception as e:
        print(f"[DB] Ошибка db_get_video_model_by_id: {e}")
        return None
