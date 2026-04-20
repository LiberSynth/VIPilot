import psycopg2

from .connection import get_db
from common.statuses import _assert_known_status

# АРХИТЕКТУРНОЕ РЕШЕНИЕ: хранение видеофайлов в PostgreSQL (BYTEA)
#
# Видеофайлы намеренно хранятся непосредственно в базе данных в виде BYTEA-столбцов,
# а не в файловой системе или внешнем объектном хранилище (S3 и т.п.).
#
# Обоснование:
#   1. Транзакционная целостность — файл и его метаданные сохраняются или
#      откатываются вместе в рамках одной транзакции; не возникает ситуаций,
#      когда запись есть, а файл отсутствует (или наоборот).
#   2. Единая точка резервного копирования — стандартный pg_dump захватывает
#      и данные, и файлы одновременно; не нужно синхронизировать отдельные
#      хранилища.
#   3. Упрощённое развёртывание — приложение не зависит от внешних сервисов
#      хранения объектов, что снижает операционную сложность для текущего
#      масштаба проекта.
#   4. Контроль доступа — разграничение прав на уровне БД распространяется
#      автоматически; не нужна отдельная политика доступа к бакетам.
#
# Компромисс: такой подход увеличивает размер БД и нагрузку при потоковой
# передаче больших файлов. Перенос в объектное хранилище следует рассмотреть
# при значительном росте объёма видеоданных.


def db_create_batch_movie(batch_id, video_data: bytes, video_url: str, model_id=None):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO movies (raw_data, url, model_id) VALUES (%s, %s, %s) RETURNING id",
                (psycopg2.Binary(video_data), video_url, model_id),
            )
            movie_id = cur.fetchone()[0]
            cur.execute(
                "UPDATE batches SET movie_id = %s WHERE id = %s",
                (movie_id, batch_id),
            )
        conn.commit()
    return True


def db_get_batch_original_video(batch_id) -> bytes | None:
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


def db_set_batch_video_pending(batch_id, job_data):
    import json
    _assert_known_status('video_pending')
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE batches
                   SET status = 'video_pending',
                       data   = COALESCE(data::jsonb, '{}'::jsonb) || %s::jsonb
                   WHERE id = %s""",
                (json.dumps(job_data), batch_id),
            )
        conn.commit()
    return True


def db_set_batch_transcode_ready(batch_id, video_data: bytes):
    _assert_known_status('transcode_ready')
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE movies m
                      SET transcoded_data = %s
                     FROM batches b
                    WHERE b.id = %s AND b.movie_id = m.id""",
                (psycopg2.Binary(video_data), batch_id),
            )
            cur.execute(
                "UPDATE batches SET status = 'transcode_ready' WHERE id = %s",
                (batch_id,),
            )
        conn.commit()
    return True


def db_get_batch_video_data(batch_id) -> bytes | None:
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


def db_get_movie_video_data(movie_id) -> bytes | None:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT transcoded_data, raw_data FROM movies
                WHERE id = %s
            """, (movie_id,))
            row = cur.fetchone()
    if row:
        if row[0] is not None:
            return bytes(row[0])
        if row[1] is not None:
            return bytes(row[1])
    return None


def db_get_random_real_original_video() -> tuple[bytes, str, str | None] | None:
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
