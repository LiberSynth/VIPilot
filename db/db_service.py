from .connection import get_db
from statuses import FINAL_BATCH_STATUSES


def db_cleanup_log_entries(log_lifetime_days: int) -> int:
    try:
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
    except Exception as e:
        print(f"[DB] Ошибка db_cleanup_log_entries: {e}")
        return 0


def db_cleanup_logs(short_log_lifetime_days: int) -> int:
    try:
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
    except Exception as e:
        print(f"[DB] Ошибка db_cleanup_logs: {e}")
        return 0


def db_clear_all_history():
    try:
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
    except Exception as e:
        print(f"[DB] Ошибка db_clear_all_history: {e}")
        raise


def db_cleanup_video_data(file_lifetime_days: int) -> int:
    try:
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
    except Exception as e:
        print(f"[DB] Ошибка db_cleanup_video_data: {e}")
        return 0


def db_cleanup_batches(batch_lifetime_days: int) -> int:
    try:
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
    except Exception as e:
        print(f"[DB] Ошибка db_cleanup_batches: {e}")
        return 0
