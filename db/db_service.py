from .connection import get_db
from common.statuses import FINAL_BATCH_STATUSES
from log.log import write_log_entry
from utils.utils import fmt_id_msg


def db_log_update(log_id, message, status):
    """Обновляет message и status существующей записи лога."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE log SET message = %s, status = %s WHERE id = %s",
                (message, status, log_id),
            )
        conn.commit()


def db_get_log_entries(log_id):
    """Возвращает список субзаписей для указанного log_id."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT message, level, created_at FROM log_entries WHERE log_id = %s ORDER BY created_at",
                (log_id,),
            )
            rows = cur.fetchall()
    return [
        {"msg": r[0], "level": r[1], "ts": r[2].isoformat() if r[2] else None}
        for r in rows
    ]


def db_get_monitor(batch_limit=50):
    """
    Возвращает структурированные данные для монитора.
    Первичная таблица — log. Батч появляется только если есть хотя бы одна запись в log.
    - batches: батчи с вложенными log-записями, сортировка по последнему событию DESC
    - system:  системные события без батча
    """
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    b.id,
                    b.scheduled_at,
                    b.type,
                    b.status,
                    b.created_at,
                    MAX(l.created_at) AS last_event_at,
                    b.story_id,
                    m.has_video_data,
                    COALESCE(
                        json_agg(
                            json_build_object(
                                'id',         l.id::text,
                                'pipeline',   l.pipeline,
                                'message',    l.message,
                                'status',     l.status,
                                'created_at', l.created_at,
                                'entries', (
                                    SELECT COALESCE(
                                        json_agg(
                                            json_build_object(
                                                'message',    le.message,
                                                'level',      le.level,
                                                'created_at', le.created_at
                                            ) ORDER BY le.created_at
                                        ),
                                        '[]'::json
                                    )
                                    FROM log_entries le WHERE le.log_id = l.id
                                )
                            ) ORDER BY l.created_at
                        ) FILTER (WHERE l.id IS NOT NULL),
                        '[]'::json
                    ) AS logs,
                    tm.name AS text_model_name,
                    vm.name AS video_model_name
                FROM batches b
                LEFT JOIN (
                    SELECT id, model_id,
                           (transcoded_data IS NOT NULL OR raw_data IS NOT NULL) AS has_video_data
                    FROM movies
                ) m ON m.id = b.movie_id
                LEFT JOIN log l ON l.batch_id = b.id
                LEFT JOIN stories s ON s.id = b.story_id
                LEFT JOIN ai_models tm ON tm.id = s.model_id
                LEFT JOIN ai_models vm ON vm.id = m.model_id
                GROUP BY b.id, b.scheduled_at, b.type, b.status, b.created_at,
                         b.story_id, m.has_video_data,
                         tm.name, vm.name
                ORDER BY COALESCE(MAX(l.created_at), b.created_at) DESC
                LIMIT %s
                """,
                (batch_limit,),
            )
            batch_rows = cur.fetchall()

            cur.execute(
                """
                SELECT
                    l.id, l.pipeline, l.message, l.status, l.created_at,
                    COALESCE(
                        json_agg(
                            json_build_object(
                                'message',    le.message,
                                'level',      le.level,
                                'created_at', le.created_at
                            ) ORDER BY le.created_at
                        ) FILTER (WHERE le.id IS NOT NULL),
                        '[]'::json
                    ) AS entries
                FROM log l
                LEFT JOIN log_entries le ON le.log_id = l.id
                WHERE l.batch_id IS NULL
                GROUP BY l.id
                ORDER BY l.created_at DESC
                LIMIT 100
                """
            )
            sys_rows = cur.fetchall()

            cur.execute(
                """
                SELECT message, level, created_at
                FROM log_entries
                WHERE log_id IS NULL
                ORDER BY created_at DESC
                LIMIT 500
                """
            )
            orphan_rows = cur.fetchall()

    batches = [
        {
            "batch_id": str(r[0]),
            "scheduled_at": r[1].isoformat() if r[1] else None,
            "type": r[2],
            "batch_status": r[3],
            "created_at": r[4].isoformat() if r[4] else None,
            "last_event_at": r[5].isoformat() if r[5] else None,
            "story_id": str(r[6]) if r[6] else None,
            "has_video_data": bool(r[7]),
            "logs": r[8],
            "text_model_name": r[9],
            "video_model_name": r[10],
        }
        for r in batch_rows
    ]
    system = [
        {
            "id": str(r[0]),
            "pipeline": r[1],
            "message": r[2],
            "status": r[3],
            "created_at": r[4].isoformat() if r[4] else None,
            "entries": r[5],
        }
        for r in sys_rows
    ]
    orphan_entries = [
        {
            "message":    r[0],
            "level":      r[1],
            "created_at": r[2].isoformat() if r[2] else None,
        }
        for r in orphan_rows
    ]
    return {"batches": batches, "system": system, "orphan_entries": orphan_entries}


def db_cleanup_log_entries(log_lifetime_days: int) -> int:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM log_entries
                WHERE log_id IN (
                    SELECT id FROM log
                    WHERE created_at < now() - make_interval(days => %s)
                )
            """, (log_lifetime_days,))
            count = cur.rowcount
        conn.commit()
    return count


def db_cleanup_logs(short_log_lifetime_days: int) -> int:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM log_entries
                WHERE log_id IN (
                    SELECT id FROM log
                    WHERE created_at < now() - make_interval(days => %s)
                )
            """, (short_log_lifetime_days,))
            cur.execute("""
                DELETE FROM log
                WHERE created_at < now() - make_interval(days => %s)
            """, (short_log_lifetime_days,))
            count = cur.rowcount
        conn.commit()
    return count


def db_clear_all_history():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM log_entries")
            le = cur.rowcount
            cur.execute("DELETE FROM log")
            ll = cur.rowcount
            cur.execute("DELETE FROM batches")
            bl = cur.rowcount
        conn.commit()
    write_log_entry(None, f"[DB] Очистка истории: log_entries={le}, log={ll}, batches={bl}", level='silent')
    return {"log_entries": le, "logs": ll, "batches": bl}


def db_cleanup_video_data(file_lifetime_days: int) -> int:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE movies
                   SET transcoded_data = NULL,
                       raw_data        = NULL
                WHERE id IN (
                    SELECT movie_id FROM batches
                    WHERE status = ANY(%s)
                      AND created_at < now() - make_interval(days => %s)
                      AND movie_id IS NOT NULL
                )
                  AND COALESCE(transcoded_data, raw_data) IS NOT NULL
            """, (list(FINAL_BATCH_STATUSES), file_lifetime_days))
            count = cur.rowcount
        conn.commit()
    return count


def db_clear_stories() -> dict:
    with get_db() as conn:
        with conn.cursor() as cur:
            final_statuses_sql = ', '.join(f"'{s}'" for s in FINAL_BATCH_STATUSES)
            cur.execute(f"""
                SELECT DISTINCT s.id
                FROM stories s
                LEFT JOIN batches b
                       ON b.story_id = s.id
                      AND (b.movie_id IS NOT NULL OR b.status NOT IN ({final_statuses_sql}))
                WHERE (s.grade IS NULL OR s.grade = 'bad')
                  AND NOT s.pinned
                  AND b.id IS NULL
            """)
            story_ids = [r[0] for r in cur.fetchall()]

            if not story_ids:
                conn.commit()
                write_log_entry(None, "[DB] Очистка сюжетов: stories=0, batches=0, log=0, log_entries=0", level='silent')
                return {"stories": 0, "batches": 0, "logs": 0, "log_entries": 0}

            fmt = ','.join(['%s'] * len(story_ids))

            cur.execute(f"SELECT id FROM batches WHERE story_id IN ({fmt})", story_ids)
            batch_ids = [r[0] for r in cur.fetchall()]

            le_count = 0
            ll_count = 0
            bl_count = 0

            if batch_ids:
                bfmt = ','.join(['%s'] * len(batch_ids))
                cur.execute(f"""
                    DELETE FROM log_entries
                    WHERE log_id IN (
                        SELECT id FROM log WHERE batch_id IN ({bfmt})
                    )
                """, batch_ids)
                le_count = cur.rowcount

                cur.execute(f"DELETE FROM log WHERE batch_id IN ({bfmt})", batch_ids)
                ll_count = cur.rowcount

                cur.execute(f"DELETE FROM batches WHERE id IN ({bfmt})", batch_ids)
                bl_count = cur.rowcount

            cur.execute(f"DELETE FROM stories WHERE id IN ({fmt})", story_ids)
            sl_count = cur.rowcount

        conn.commit()

    write_log_entry(None, f"[DB] Очистка сюжетов: stories={sl_count}, batches={bl_count}, log={ll_count}, log_entries={le_count}", level='silent')
    return {"stories": sl_count, "batches": bl_count, "logs": ll_count, "log_entries": le_count}


def db_delete_bad_movies() -> dict:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM movies WHERE grade = 'bad'")
            movie_ids = [r[0] for r in cur.fetchall()]

            if not movie_ids:
                conn.commit()
                write_log_entry(None, "[DB] Удалены неудачные видео: movies=0, batches=0, log=0, log_entries=0", level='silent')
                return {"movies": 0, "batches": 0, "logs": 0, "log_entries": 0}

            mfmt = ','.join(['%s'] * len(movie_ids))

            cur.execute(f"SELECT id FROM batches WHERE movie_id IN ({mfmt})", movie_ids)
            batch_ids = [r[0] for r in cur.fetchall()]

            le_count = 0
            ll_count = 0
            bl_count = 0

            if batch_ids:
                bfmt = ','.join(['%s'] * len(batch_ids))
                cur.execute(f"""
                    DELETE FROM log_entries
                    WHERE log_id IN (
                        SELECT id FROM log WHERE batch_id IN ({bfmt})
                    )
                """, batch_ids)
                le_count = cur.rowcount

                cur.execute(f"DELETE FROM log WHERE batch_id IN ({bfmt})", batch_ids)
                ll_count = cur.rowcount

                cur.execute(f"DELETE FROM batches WHERE id IN ({bfmt})", batch_ids)
                bl_count = cur.rowcount

            cur.execute(f"DELETE FROM movies WHERE id IN ({mfmt})", movie_ids)
            ml_count = cur.rowcount

        conn.commit()

    write_log_entry(None, f"[DB] Удалены неудачные видео: movies={ml_count}, batches={bl_count}, log={ll_count}, log_entries={le_count}", level='silent')
    return {"movies": ml_count, "batches": bl_count, "logs": ll_count, "log_entries": le_count}


def db_get_good_movies_meta() -> list[dict]:
    """Возвращает список {id, model_name, story_title} для movies с grade='good' и ненулевым видео."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT ON (m.id)
                    m.id::text,
                    a.name AS model_name,
                    s.title AS story_title
                FROM movies m
                LEFT JOIN ai_models a ON a.id = m.model_id
                LEFT JOIN batches b ON b.movie_id = m.id
                LEFT JOIN stories s ON s.id = b.story_id
                WHERE m.grade = 'good'
                  AND COALESCE(m.transcoded_data, m.raw_data) IS NOT NULL
                ORDER BY m.id, b.created_at DESC NULLS LAST
            """)
            rows = cur.fetchall()
    return [
        {"id": r[0], "model_name": r[1] or "", "story_title": r[2] or ""}
        for r in rows
    ]


def db_get_batch_status(batch_id: str) -> str | None:
    """Возвращает текущий статус батча или None если не найден."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT status FROM batches WHERE id = %s", (batch_id,))
            row = cur.fetchone()
            return row[0] if row else None


def db_delete_batch(batch_id: str) -> bool:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM log_entries
                WHERE log_id IN (
                    SELECT id FROM log WHERE batch_id = %s
                )
                """,
                (batch_id,),
            )
            cur.execute("DELETE FROM log WHERE batch_id = %s", (batch_id,))
            cur.execute(
                "SELECT movie_id FROM batches WHERE id = %s AND movie_id IS NOT NULL",
                (batch_id,),
            )
            row = cur.fetchone()
            movie_id = row[0] if row else None
            cur.execute("DELETE FROM batches WHERE id = %s", (batch_id,))
            deleted = cur.rowcount
            if movie_id:
                cur.execute("DELETE FROM movies WHERE id = %s", (movie_id,))
        conn.commit()
    return deleted > 0


def db_delete_story(story_id: str) -> dict:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM batches WHERE story_id = %s", (story_id,))
            batch_ids = [r[0] for r in cur.fetchall()]

            le_count = 0
            ll_count = 0
            bl_count = 0

            if batch_ids:
                bfmt = ','.join(['%s'] * len(batch_ids))
                cur.execute(f"""
                    DELETE FROM log_entries
                    WHERE log_id IN (
                        SELECT id FROM log WHERE batch_id IN ({bfmt})
                    )
                """, batch_ids)
                le_count = cur.rowcount

                cur.execute(f"DELETE FROM log WHERE batch_id IN ({bfmt})", batch_ids)
                ll_count = cur.rowcount

                cur.execute(f"DELETE FROM batches WHERE id IN ({bfmt})", batch_ids)
                bl_count = cur.rowcount

            cur.execute("DELETE FROM stories WHERE id = %s", (story_id,))
            sl_count = cur.rowcount

        conn.commit()

    write_log_entry(None, fmt_id_msg("[DB] Удалён сюжет {}: batches=" + str(bl_count) + ", log=" + str(ll_count) + ", log_entries=" + str(le_count), story_id), level='silent')
    return {"stories": sl_count, "batches": bl_count, "logs": ll_count, "log_entries": le_count}


def db_delete_movie(movie_id: str) -> dict:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM movies WHERE id = %s", (movie_id,))
            if not cur.fetchone():
                return {}

            cur.execute("SELECT id FROM batches WHERE movie_id = %s", (movie_id,))
            batch_ids = [r[0] for r in cur.fetchall()]

            le_count = 0
            ll_count = 0
            bl_count = 0

            if batch_ids:
                bfmt = ','.join(['%s'] * len(batch_ids))
                cur.execute(f"""
                    DELETE FROM log_entries
                    WHERE log_id IN (
                        SELECT id FROM log WHERE batch_id IN ({bfmt})
                    )
                """, batch_ids)
                le_count = cur.rowcount

                cur.execute(f"DELETE FROM log WHERE batch_id IN ({bfmt})", batch_ids)
                ll_count = cur.rowcount

                cur.execute(f"DELETE FROM batches WHERE id IN ({bfmt})", batch_ids)
                bl_count = cur.rowcount

            cur.execute("DELETE FROM movies WHERE id = %s", (movie_id,))
            ml_count = cur.rowcount

        conn.commit()

    write_log_entry(None, fmt_id_msg("[DB] Удалено видео {}: batches=" + str(bl_count) + ", log=" + str(ll_count) + ", log_entries=" + str(le_count), movie_id), level='silent')
    return {"movies": ml_count, "batches": bl_count, "logs": ll_count, "log_entries": le_count}


def db_cleanup_batches(batch_lifetime_days: int) -> int:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id FROM batches
                WHERE (
                    status IN ('published', 'cancelled')
                    OR (type = 'movie_probe' AND status = 'movie_probe')
                    OR (type = 'story_probe' AND status = 'story_probe')
                )
                  AND created_at < now() - make_interval(days => %s)
            """, (batch_lifetime_days,))
            rows = cur.fetchall()
            if not rows:
                return 0
            batch_ids = [str(r[0]) for r in rows]

            fmt = ','.join(['%s'] * len(batch_ids))

            cur.execute(f"""
                DELETE FROM log_entries
                WHERE log_id IN (
                    SELECT id FROM log WHERE batch_id IN ({fmt})
                )
            """, batch_ids)

            cur.execute(f"DELETE FROM log WHERE batch_id IN ({fmt})", batch_ids)

            cur.execute(f"DELETE FROM batches WHERE id IN ({fmt})", batch_ids)

        conn.commit()
    return len(batch_ids)
