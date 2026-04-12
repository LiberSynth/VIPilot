import psycopg2

from .connection import get_db
from statuses import _assert_known_status


def db_set_batch_original_video(batch_id, video_data: bytes):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT movie_id FROM batches WHERE id = %s", (batch_id,))
                row = cur.fetchone()
                movie_id = row[0] if row else None
                if movie_id is None:
                    cur.execute(
                        "INSERT INTO movies (raw_data) VALUES (%s) RETURNING id",
                        (psycopg2.Binary(video_data),),
                    )
                    movie_id = cur.fetchone()[0]
                    cur.execute(
                        "UPDATE batches SET movie_id = %s WHERE id = %s",
                        (movie_id, batch_id),
                    )
                else:
                    cur.execute(
                        "UPDATE movies SET raw_data = %s WHERE id = %s",
                        (psycopg2.Binary(video_data), movie_id),
                    )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_set_batch_original_video: {e}")
        return False


def db_get_batch_original_video(batch_id) -> bytes | None:
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT m.raw_data FROM batches b
                    JOIN movies m ON m.id = b.movie_id
                    WHERE b.id = %s
                """, (batch_id,))
                row = cur.fetchone()
        if row and row[0] is not None:
            return bytes(row[0])
        return None
    except Exception as e:
        print(f"[DB] Ошибка db_get_batch_original_video: {e}")
        return None


def db_set_batch_video_ready(batch_id, video_url):
    _assert_known_status('video_ready')
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT movie_id FROM batches WHERE id = %s", (batch_id,))
                row = cur.fetchone()
                movie_id = row[0] if row else None
                if movie_id is None:
                    cur.execute(
                        "INSERT INTO movies (url) VALUES (%s) RETURNING id",
                        (video_url,),
                    )
                    movie_id = cur.fetchone()[0]
                    cur.execute(
                        "UPDATE batches SET status = 'video_ready', movie_id = %s WHERE id = %s",
                        (movie_id, batch_id),
                    )
                else:
                    cur.execute(
                        "UPDATE movies SET url = %s WHERE id = %s",
                        (video_url, movie_id),
                    )
                    cur.execute(
                        "UPDATE batches SET status = 'video_ready' WHERE id = %s",
                        (batch_id,),
                    )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_set_batch_video_ready: {e}")
        return False


def db_set_batch_video_pending(batch_id, job_data):
    import json
    _assert_known_status('video_pending')
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """UPDATE batches
                       SET status = 'video_pending',
                           data   = %s::jsonb
                       WHERE id = %s""",
                    (json.dumps(job_data), batch_id),
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_set_batch_video_pending: {e}")
        return False


def db_set_batch_video_model(batch_id, model_id):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT movie_id FROM batches WHERE id = %s", (batch_id,))
                row = cur.fetchone()
                movie_id = row[0] if row else None
                if movie_id is None:
                    cur.execute(
                        "INSERT INTO movies (model_id) VALUES (%s) RETURNING id",
                        (model_id,),
                    )
                    movie_id = cur.fetchone()[0]
                    cur.execute(
                        "UPDATE batches SET movie_id = %s WHERE id = %s",
                        (movie_id, batch_id),
                    )
                else:
                    cur.execute(
                        "UPDATE movies SET model_id = %s WHERE id = %s",
                        (model_id, movie_id),
                    )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_set_batch_video_model: {e}")
        return False


def db_set_batch_transcode_ready(batch_id, video_data: bytes):
    _assert_known_status('transcode_ready')
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT movie_id FROM batches WHERE id = %s", (batch_id,))
                row = cur.fetchone()
                movie_id = row[0] if row else None
                if movie_id is None:
                    cur.execute(
                        "INSERT INTO movies (transcoded_data) VALUES (%s) RETURNING id",
                        (psycopg2.Binary(video_data),),
                    )
                    movie_id = cur.fetchone()[0]
                    cur.execute(
                        "UPDATE batches SET status = 'transcode_ready', movie_id = %s WHERE id = %s",
                        (movie_id, batch_id),
                    )
                else:
                    cur.execute(
                        "UPDATE movies SET transcoded_data = %s WHERE id = %s",
                        (psycopg2.Binary(video_data), movie_id),
                    )
                    cur.execute(
                        "UPDATE batches SET status = 'transcode_ready' WHERE id = %s",
                        (batch_id,),
                    )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_set_batch_transcode_ready: {e}")
        return False


def db_set_batch_transcode_skip(batch_id):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE batches SET status = 'transcode_ready' WHERE id = %s",
                    (batch_id,),
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_set_batch_transcode_skip: {e}")
        return False


def db_get_batch_video_data(batch_id) -> bytes | None:
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT m.transcoded_data, m.raw_data FROM batches b
                    JOIN movies m ON m.id = b.movie_id
                    WHERE b.id = %s
                """, (batch_id,))
                row = cur.fetchone()
        if row:
            if row[0] is not None:
                return bytes(row[0])
            if row[1] is not None:
                return bytes(row[1])
        return None
    except Exception as e:
        print(f"[DB] Ошибка db_get_batch_video_data: {e}")
        return None


def db_get_random_real_original_video() -> tuple[bytes, str, str | None] | None:
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT m.raw_data, b.id, b.story_id::text FROM batches b
                    JOIN movies m ON m.id = b.movie_id
                    WHERE m.raw_data IS NOT NULL
                    ORDER BY (m.url NOT LIKE 'emulation://%') DESC, random()
                    LIMIT 1
                """)
                row = cur.fetchone()
                return (bytes(row[0]), str(row[1]), row[2]) if row else None
    except Exception as e:
        print(f"[DB] Ошибка db_get_random_real_original_video: {e}")
        return None
