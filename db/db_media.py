import json

import psycopg2

from .connection import get_db
from .db_pipeline import db_set_batch_status

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
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE batches
                   SET data = COALESCE(data::jsonb, '{}'::jsonb) || %s::jsonb
                   WHERE id = %s""",
                (json.dumps(job_data), batch_id),
            )
        db_set_batch_status(batch_id, 'video_pending', conn)


def db_save_transcoded_data(batch_id, video_data: bytes):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE movies m
                      SET transcoded_data = %s
                     FROM batches b
                    WHERE b.id = %s AND b.movie_id = m.id""",
                (psycopg2.Binary(video_data), batch_id),
            )
        conn.commit()


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


def db_get_good_movie_video_data(movie_id) -> bytes | None:
    """Возвращает видеоданные ролика по id."""
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


def db_get_random_real_original_video() -> tuple[str, str, str | None] | None:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT m.id::text, b.id::text, b.story_id::text FROM batches b
                JOIN movies m ON m.id = b.movie_id
                WHERE m.raw_data IS NOT NULL
                ORDER BY (m.url NOT LIKE 'emulation://%') DESC, random()
                LIMIT 1
            """)
            row = cur.fetchone()
            return (row[0], row[1], row[2]) if row else None


def db_copy_movie_for_emulation(source_movie_id: str, target_batch_id: str):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO movies (raw_data, url, model_id)
                SELECT raw_data, 'emulation://skipped', model_id
                FROM movies
                WHERE id = %s
                RETURNING id
            """, (source_movie_id,))
            new_movie_id = cur.fetchone()[0]
            cur.execute(
                "UPDATE batches SET movie_id = %s WHERE id = %s",
                (new_movie_id, target_batch_id),
            )
        conn.commit()
