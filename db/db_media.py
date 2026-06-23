import json
from pathlib import Path

from .connection import get_db
from .db_pipeline import db_set_batch_status

_RAW_FIELD = "raw_data"
_TRANSCODED_FIELD = "transcoded_data"
_VIDEO_DIR = Path(__file__).resolve().parents[1] / "video"

def _video_file_path(movie_id: str, field: str) -> Path:
    return _VIDEO_DIR / f"{movie_id} - {field}.mp4"

def _write_video_file(movie_id: str, field: str, video_data: bytes) -> None:
    _VIDEO_DIR.mkdir(parents=True, exist_ok=True)
    _video_file_path(movie_id, field).write_bytes(video_data)

def _read_video_file_or_none(movie_id: str, field: str) -> bytes | None:
    try:
        return _video_file_path(movie_id, field).read_bytes()
    except FileNotFoundError:
        return None

def db_delete_movie_video_files(movie_id: str) -> None:
    """Только через db_delete_movie / db_delete_bad_movies."""
    movie_id = str(movie_id)
    for field in (_RAW_FIELD, _TRANSCODED_FIELD):
        try:
            _video_file_path(movie_id, field).unlink()
        except FileNotFoundError:
            pass

def db_create_batch_movie(batch_id, video_data: bytes, video_url: str, model_id=None):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT story_id, movie_id::text FROM batches WHERE id = %s FOR UPDATE",
                (batch_id,),
            )
            row = cur.fetchone()
            story_id = row[0] if row else None
            existing_movie_id = row[1] if row else None

            movie_id = None
            if existing_movie_id:
                cur.execute(
                    """
                    UPDATE movies
                    SET story_id = %s, url = %s, model_id = %s
                    WHERE id = %s::uuid
                    """,
                    (story_id, video_url, model_id, existing_movie_id),
                )
                if cur.rowcount:
                    movie_id = str(existing_movie_id)

            if not movie_id:
                cur.execute(
                    "INSERT INTO movies (story_id, url, model_id) VALUES (%s, %s, %s) RETURNING id",
                    (story_id, video_url, model_id),
                )
                movie_id = str(cur.fetchone()[0])

            cur.execute(
                "UPDATE batches SET movie_id = %s WHERE id = %s",
                (movie_id, batch_id),
            )
            _write_video_file(movie_id, _RAW_FIELD, video_data)
        conn.commit()
    return True

def db_get_batch_original_video(batch_id) -> bytes | None:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT m.id::text FROM batches b
                JOIN movies m ON m.id = b.movie_id
                WHERE b.id = %s
            """, (batch_id,))
            row = cur.fetchone()
    if not row:
        return None
    return _read_video_file_or_none(str(row[0]), _RAW_FIELD)

def db_save_video_job_and_set_pending(batch_id, job_data):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE batches
                   SET data = COALESCE(data::jsonb, '{}'::jsonb) || %s::jsonb
                   WHERE id = %s""",
                (json.dumps(job_data), batch_id),
            )
        db_set_batch_status(batch_id, 'pending', conn)

def db_save_video_job_and_set_processing(batch_id, job_data):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE batches
                   SET data = COALESCE(data::jsonb, '{}'::jsonb) || %s::jsonb
                   WHERE id = %s""",
                (json.dumps(job_data), batch_id),
            )
        db_set_batch_status(batch_id, 'processing', conn)

def db_save_generated_video_url(batch_id, video_url: str):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE batches
                   SET data = COALESCE(data::jsonb, '{}'::jsonb)
                              || %s::jsonb
                   WHERE id = %s""",
                (json.dumps({"generated_video_url": video_url}), batch_id),
            )
        db_set_batch_status(batch_id, 'processed', conn)

def db_save_transcoded_data(batch_id, video_data: bytes):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT m.id::text
                FROM batches b
                JOIN movies m ON m.id = b.movie_id
                WHERE b.id = %s
                """,
                (batch_id,),
            )
            row = cur.fetchone()
        if not row:
            return
        movie_id = str(row[0])
        _write_video_file(movie_id, _TRANSCODED_FIELD, video_data)
        db_set_movie_transcoded(movie_id, conn)

def db_set_movie_transcoded(movie_id: str, conn=None):
    if conn is None:
        with get_db() as _conn:
            db_set_movie_transcoded(movie_id, _conn)
        return
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE movies SET transcoded = B'1' WHERE id = %s::uuid",
            (movie_id,),
        )
    conn.commit()

def db_set_movie_published(movie_id: str, conn=None):
    if conn is None:
        with get_db() as _conn:
            db_set_movie_published(movie_id, _conn)
        return
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE movies SET published = B'1' WHERE id = %s::uuid",
            (movie_id,),
        )
    conn.commit()

def db_get_batch_video_data_with_source(batch_id) -> tuple[bytes | None, str | None]:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT m.id::text FROM batches b
                JOIN movies m ON m.id = b.movie_id
                WHERE b.id = %s
            """, (batch_id,))
            row = cur.fetchone()
    if not row:
        return None, None
    movie_id = str(row[0])
    transcoded = _read_video_file_or_none(movie_id, _TRANSCODED_FIELD)
    if transcoded is not None:
        return transcoded, _TRANSCODED_FIELD
    raw = _read_video_file_or_none(movie_id, _RAW_FIELD)
    if raw is not None:
        return raw, _RAW_FIELD
    return None, None

def db_get_batch_video_data(batch_id) -> bytes | None:
    video_data, _source = db_get_batch_video_data_with_source(batch_id)
    return video_data

def _resolve_movie_video_path(movie_id: str) -> Path | None:
    movie_id = str(movie_id)
    transcoded_path = _video_file_path(movie_id, _TRANSCODED_FIELD)
    if transcoded_path.exists():
        return transcoded_path
    raw_path = _video_file_path(movie_id, _RAW_FIELD)
    if raw_path.exists():
        return raw_path
    return None

def db_get_movie_video_data(movie_id) -> bytes | None:
    """Возвращает видеоданные ролика по id.
    Предпочитает transcoded_data; при отсутствии возвращает raw_data.
    """
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id::text FROM movies
                WHERE id = %s
            """, (movie_id,))
            row = cur.fetchone()
    if not row:
        return None
    path = _resolve_movie_video_path(str(row[0]))
    if path is None:
        return None
    return path.read_bytes()

def db_get_movie_video_path(movie_id) -> Path | None:
    """Путь к mp4 ролика (transcoded_data, иначе raw_data)."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id::text FROM movies WHERE id = %s",
                (movie_id,),
            )
            row = cur.fetchone()
    if not row:
        return None
    return _resolve_movie_video_path(str(row[0]))

def db_get_batch_video_path(batch_id) -> Path | None:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT m.id::text FROM batches b
                JOIN movies m ON m.id = b.movie_id
                WHERE b.id = %s
            """, (batch_id,))
            row = cur.fetchone()
    if not row:
        return None
    return _resolve_movie_video_path(str(row[0]))

def db_create_manual_movie(title: str, video_data: bytes) -> str:
    """Создаёт сюжет и ролик из файла в одной транзакции.

    Если в stories уже есть запись с таким же title — переиспользует её.
    Ролик попадает в пул (used=0). Возвращает movie_id.
    """
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM stories WHERE title = %s LIMIT 1",
                (title,),
            )
            row = cur.fetchone()
            if row:
                story_id = row[0]
            else:
                cur.execute(
                    "INSERT INTO stories (model_id, title, content, grade, manual_changed)"
                    " VALUES (NULL, %s, '', 'good', TRUE) RETURNING id",
                    (title,),
                )
                story_id = cur.fetchone()[0]
            cur.execute(
                "INSERT INTO movies (story_id, url, grade) VALUES (%s, NULL, 'good') RETURNING id",
                (story_id,),
            )
            movie_id = str(cur.fetchone()[0])
            _write_video_file(movie_id, _RAW_FIELD, video_data)
        conn.commit()
    return movie_id

