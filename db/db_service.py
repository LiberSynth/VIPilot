from .connection import get_db
from common.statuses import FINAL_BATCH_STATUSES


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
    return {"batches": batches, "system": system}


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
    print(f"[DB] Очистка истории: log_entries={le}, log={ll}, batches={bl}")
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
                      AND completed_at < now() - make_interval(days => %s)
                      AND movie_id IS NOT NULL
                )
                  AND COALESCE(transcoded_data, raw_data) IS NOT NULL
            """, (list(FINAL_BATCH_STATUSES), file_lifetime_days))
            count = cur.rowcount
        conn.commit()
    return count


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
                  AND completed_at < now() - make_interval(days => %s)
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
