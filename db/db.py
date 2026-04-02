import time
import json
import psycopg2
import psycopg2.extras

from .init import get_db


# ---------------------------------------------------------------------------
# Настройки
# ---------------------------------------------------------------------------

def db_get(key, default=""):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT value FROM settings WHERE key = %s", (key,))
                row = cur.fetchone()
                return row[0] if row else default
    except Exception as e:
        print(f"[DB] Ошибка чтения {key}: {e}")
        return default


def db_set(key, value):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO settings (key, value) VALUES (%s, %s)
                    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                    """,
                    (key, value),
                )
            conn.commit()
    except Exception as e:
        print(f"[DB] Ошибка записи {key}: {e}")


# ---------------------------------------------------------------------------
# Расписание публикаций
# ---------------------------------------------------------------------------

def db_get_schedule():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, time_utc FROM schedule ORDER BY time_utc")
                rows = cur.fetchall()
        return [{"id": str(row[0]), "time_utc": row[1]} for row in rows]
    except Exception as e:
        print(f"[DB] Ошибка получения расписания: {e}")
        return []


def db_add_schedule_slot(time_utc):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO schedule (time_utc) VALUES (%s) RETURNING id",
                    (time_utc,),
                )
                row = cur.fetchone()
            conn.commit()
        return str(row[0])
    except Exception as e:
        print(f"[DB] Ошибка добавления слота расписания: {e}")
        return None


def db_delete_schedule_slot(slot_id):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM schedule WHERE id = %s", (slot_id,))
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка удаления слота расписания: {e}")
        return False


# ---------------------------------------------------------------------------
# Таргеты
# ---------------------------------------------------------------------------

def db_get_active_targets():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, name, aspect_ratio_x, aspect_ratio_y FROM targets WHERE active = TRUE"
                )
                rows = cur.fetchall()
        return [
            {"id": str(row[0]), "name": row[1], "aspect_ratio_x": row[2], "aspect_ratio_y": row[3]}
            for row in rows
        ]
    except Exception as e:
        print(f"[DB] Ошибка db_get_active_targets: {e}")
        return []


# ---------------------------------------------------------------------------
# Батчи
# ---------------------------------------------------------------------------

def db_ensure_batch(scheduled_at, target_id):
    """Создаёт батч если не существует.
    Если батч существует со статусом 'устарел' — сбрасывает его в pending.
    Возвращает UUID нового/восстановленного батча или None если батч уже активен."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO batches (scheduled_at, target_id)
                    VALUES (%s, %s)
                    ON CONFLICT (scheduled_at, target_id) DO UPDATE
                        SET status       = 'pending',
                            story_id     = NULL,
                            video_url    = NULL,
                            video_file   = NULL,
                            data         = NULL,
                            completed_at = NULL
                        WHERE batches.status = 'устарел'
                    RETURNING id
                    """,
                    (scheduled_at, target_id),
                )
                row = cur.fetchone()
            conn.commit()
        return str(row[0]) if row else None
    except Exception as e:
        print(f"[DB] Ошибка db_ensure_batch: {e}")
        return None


# ---------------------------------------------------------------------------
# Батчи — пайплайновые функции
# ---------------------------------------------------------------------------

def db_get_pending_batch():
    """Атомарно захватывает первый pending-батч: переводит его в 'story_generating'
    и возвращает данные. Гарантирует, что два потока не возьмут один и тот же батч."""
    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    WITH claimed AS (
                        UPDATE batches SET status = 'story_generating'
                        WHERE id = (
                            SELECT id FROM batches
                            WHERE status = 'pending' AND story_id IS NULL
                            ORDER BY scheduled_at
                            LIMIT 1
                            FOR UPDATE SKIP LOCKED
                        )
                        RETURNING *
                    )
                    SELECT c.id, c.scheduled_at, c.target_id,
                           t.name AS target_name,
                           t.aspect_ratio_x, t.aspect_ratio_y
                    FROM claimed c
                    JOIN targets t ON t.id = c.target_id
                """)
                row = cur.fetchone()
            conn.commit()
        return dict(row) if row else None
    except Exception as e:
        print(f"[DB] Ошибка db_get_pending_batch: {e}")
        return None


def db_recover_story_generating():
    """Сбрасывает застрявшие story_generating-батчи в pending при старте."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE batches SET status = 'pending'
                    WHERE status = 'story_generating'
                """)
                n = cur.rowcount
            conn.commit()
        if n:
            print(f"[DB] Восстановлено story_generating → pending: {n}")
        return n
    except Exception as e:
        print(f"[DB] Ошибка db_recover_story_generating: {e}")
        return 0


def db_get_story_ready_batch_atomic():
    """Атомарно захватывает первый story_ready-батч: переводит его в 'video_generating'
    и возвращает данные. Гарантирует, что два потока не возьмут один и тот же батч."""
    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    WITH claimed AS (
                        UPDATE batches SET status = 'video_generating'
                        WHERE id = (
                            SELECT id FROM batches
                            WHERE status = 'story_ready'
                            ORDER BY scheduled_at
                            LIMIT 1
                            FOR UPDATE SKIP LOCKED
                        )
                        RETURNING *
                    )
                    SELECT c.id, c.scheduled_at, c.target_id, c.story_id,
                           t.name AS target_name,
                           t.aspect_ratio_x, t.aspect_ratio_y
                    FROM claimed c
                    JOIN targets t ON t.id = c.target_id
                """)
                row = cur.fetchone()
            conn.commit()
        return dict(row) if row else None
    except Exception as e:
        print(f"[DB] Ошибка db_get_story_ready_batch_atomic: {e}")
        return None


def db_recover_video_generating():
    """Сбрасывает застрявшие video_generating-батчи в story_ready при старте."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE batches SET status = 'story_ready'
                    WHERE status = 'video_generating'
                """)
                n = cur.rowcount
            conn.commit()
        if n:
            print(f"[DB] Восстановлено video_generating → story_ready: {n}")
        return n
    except Exception as e:
        print(f"[DB] Ошибка db_recover_video_generating: {e}")
        return 0


def db_reset_video_generating(batch_id):
    """Сбрасывает video_generating → story_ready для конкретного батча
    (только если батч ещё в этом статусе — безопасно вызывать в finally)."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE batches SET status = 'story_ready'
                    WHERE id = %s AND status = 'video_generating'
                """, (batch_id,))
                n = cur.rowcount
            conn.commit()
        if n:
            print(f"[DB] Восстановлено video_generating → story_ready: {batch_id[:8]}…")
    except Exception as e:
        print(f"[DB] Ошибка db_reset_video_generating: {e}")


def db_set_batch_story(batch_id, story_id):
    """Привязывает story к батчу и переводит статус в 'story_ready'."""
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


# ---------------------------------------------------------------------------
# Истории (stories)
# ---------------------------------------------------------------------------

def db_create_story(model_id, result):
    """Сохраняет сгенерированный сюжет в таблицу stories. Возвращает UUID или None."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO stories (model_id, result) VALUES (%s, %s) RETURNING id",
                    (model_id, result),
                )
                row = cur.fetchone()
            conn.commit()
        return str(row[0]) if row else None
    except Exception as e:
        print(f"[DB] Ошибка db_create_story: {e}")
        return None


# ---------------------------------------------------------------------------
# AI-модели (ai_models)
# ---------------------------------------------------------------------------

def db_get_active_text_model():
    """Возвращает (platform_url, model_url, body_tpl, model_name, model_id)
    для активной text-модели из таблицы ai_models, или все None."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT p.url, m.url, m.body, m.name, m.id
                    FROM ai_models m
                    JOIN ai_platforms p ON p.id = m.platform_id
                    WHERE m.active = TRUE AND m.type = 'text'
                    ORDER BY m."order"
                    LIMIT 1
                """)
                row = cur.fetchone()
        if not row:
            return None, None, None, None, None
        return row[0], row[1], row[2] if isinstance(row[2], dict) else {}, row[3], str(row[4])
    except Exception as e:
        print(f"[DB] Ошибка db_get_active_text_model: {e}")
        return None, None, None, None, None


# ---------------------------------------------------------------------------
# Pipeline 3 — видео
# ---------------------------------------------------------------------------

def db_get_story_ready_batch():
    """Возвращает первый батч со status='story_ready', или None."""
    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT b.id, b.scheduled_at, b.target_id, b.story_id,
                           t.name AS target_name,
                           t.aspect_ratio_x, t.aspect_ratio_y
                    FROM batches b
                    JOIN targets t ON t.id = b.target_id
                    WHERE b.status = 'story_ready'
                    ORDER BY b.scheduled_at
                    LIMIT 1
                """)
                row = cur.fetchone()
        return dict(row) if row else None
    except Exception as e:
        print(f"[DB] Ошибка db_get_story_ready_batch: {e}")
        return None


def db_get_video_pending_batch():
    """Возвращает первый батч со status='video_pending' и заполненным data, или None."""
    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT b.id, b.scheduled_at, b.target_id, b.story_id, b.data,
                           t.name AS target_name,
                           t.aspect_ratio_x, t.aspect_ratio_y
                    FROM batches b
                    JOIN targets t ON t.id = b.target_id
                    WHERE b.status = 'video_pending'
                      AND b.data IS NOT NULL
                    ORDER BY b.scheduled_at
                    LIMIT 1
                """)
                row = cur.fetchone()
        return dict(row) if row else None
    except Exception as e:
        print(f"[DB] Ошибка db_get_video_pending_batch: {e}")
        return None


def db_is_batch_scheduled(scheduled_at, target_id):
    """Проверяет актуальность батча: слот расписания существует И таргет активен.
    Возвращает True если оба условия выполнены, False если хотя бы одно нарушено."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        EXISTS (
                            SELECT 1 FROM schedule
                            WHERE time_utc = to_char(%s::timestamptz AT TIME ZONE 'UTC', 'HH24:MI')
                        ) AS slot_ok,
                        EXISTS (
                            SELECT 1 FROM targets
                            WHERE id = %s AND active = TRUE
                        ) AS target_ok
                    """,
                    (scheduled_at, target_id),
                )
                row = cur.fetchone()
                slot_ok, target_ok = row
                return slot_ok and target_ok
    except Exception as e:
        print(f"[DB] Ошибка db_is_batch_scheduled: {e}")
        return True  # безопасный fallback: не блокируем работу


def db_set_batch_obsolete(batch_id):
    """Переводит батч в status='устарел'."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE batches SET status = 'устарел' WHERE id = %s",
                    (batch_id,),
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_set_batch_obsolete: {e}")
        return False


def db_get_story_text(story_id):
    """Возвращает text из stories.result по UUID, или None."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT result FROM stories WHERE id = %s", (story_id,))
                row = cur.fetchone()
        return row[0] if row else None
    except Exception as e:
        print(f"[DB] Ошибка db_get_story_text: {e}")
        return None


def db_get_active_video_model():
    """Возвращает (submit_url, platform_url, body_tpl, model_name, model_id)
    для активной video-модели из таблицы ai_models (type='text-to-video'), или все None."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT p.url, m.url, m.body, m.name, m.id
                    FROM ai_models m
                    JOIN ai_platforms p ON p.id = m.platform_id
                    WHERE m.active = TRUE AND m.type = 'text-to-video'
                    ORDER BY m."order"
                    LIMIT 1
                """)
                row = cur.fetchone()
        if not row:
            return None, None, None, None, None
        platform_url = row[0]
        model_url    = row[1]
        body_tpl     = row[2] if isinstance(row[2], dict) else {}
        model_name   = row[3]
        model_id     = str(row[4])
        submit_url   = f"{platform_url}/{model_url}"
        return submit_url, platform_url, body_tpl, model_name, model_id
    except Exception as e:
        print(f"[DB] Ошибка db_get_active_video_model: {e}")
        return None, None, None, None, None


def db_set_batch_video_pending(batch_id, job_data):
    """Сохраняет данные задания и переводит батч в status='video_pending'."""
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


def db_set_batch_video_ready(batch_id, video_url):
    """Сохраняет video_url и переводит батч в status='video_ready'."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE batches SET status = 'video_ready', video_url = %s WHERE id = %s",
                    (video_url, batch_id),
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_set_batch_video_ready: {e}")
        return False


def db_get_video_ready_batch():
    """Возвращает первый батч со status='video_ready', или None."""
    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT b.id, b.scheduled_at, b.target_id, b.story_id, b.video_url,
                           t.name AS target_name,
                           t.aspect_ratio_x, t.aspect_ratio_y
                    FROM batches b
                    JOIN targets t ON t.id = b.target_id
                    WHERE b.status = 'video_ready'
                    ORDER BY b.scheduled_at
                    LIMIT 1
                """)
                row = cur.fetchone()
        return dict(row) if row else None
    except Exception as e:
        print(f"[DB] Ошибка db_get_video_ready_batch: {e}")
        return None


def db_set_batch_transcode_ready(batch_id, video_data: bytes):
    """Сохраняет транскодированный файл в БД и переводит батч в status='transcode_ready'."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE batches SET status = 'transcode_ready', video_data = %s WHERE id = %s",
                    (psycopg2.Binary(video_data), batch_id),
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_set_batch_transcode_ready: {e}")
        return False


def db_get_batch_video_data(batch_id) -> bytes | None:
    """Возвращает транскодированные байты видео для батча, или None."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT video_data FROM batches WHERE id = %s", (batch_id,))
                row = cur.fetchone()
        if row and row[0] is not None:
            return bytes(row[0])
        return None
    except Exception as e:
        print(f"[DB] Ошибка db_get_batch_video_data: {e}")
        return None


def db_set_batch_transcode_error(batch_id):
    """Переводит батч в status='transcode_error'."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE batches SET status = 'transcode_error' WHERE id = %s",
                    (batch_id,),
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_set_batch_transcode_error: {e}")
        return False


def db_get_transcode_ready_batch():
    """Возвращает первый батч со status='transcode_ready', или None."""
    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT b.id, b.scheduled_at, b.target_id, b.story_id,
                           b.video_url, b.video_file,
                           t.name AS target_name,
                           t.aspect_ratio_x, t.aspect_ratio_y
                    FROM batches b
                    JOIN targets t ON t.id = b.target_id
                    WHERE b.status = 'transcode_ready'
                    ORDER BY b.scheduled_at
                    LIMIT 1
                """)
                row = cur.fetchone()
        return dict(row) if row else None
    except Exception as e:
        print(f"[DB] Ошибка db_get_transcode_ready_batch: {e}")
        return None


def db_set_batch_published(batch_id):
    """Переводит батч в status='published' и ставит completed_at."""
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


def db_set_batch_publish_error(batch_id):
    """Переводит батч в status='publish_error'."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE batches SET status = 'publish_error' WHERE id = %s",
                    (batch_id,),
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_set_batch_publish_error: {e}")
        return False


def db_set_batch_video_error(batch_id):
    """Переводит батч в status='video_error'."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE batches SET status = 'video_error' WHERE id = %s",
                    (batch_id,),
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_set_batch_video_error: {e}")
        return False


def db_set_batch_pending(batch_id):
    """Сбрасывает батч в статус 'pending', очищая story_id, video_url и data.
    Используется для перезапуска цикла генерации после фатального сбоя видео."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """UPDATE batches
                       SET status = 'pending', story_id = NULL,
                           video_url = NULL, video_file = NULL, data = NULL
                       WHERE id = %s""",
                    (batch_id,),
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_set_batch_pending: {e}")
        return False


# ---------------------------------------------------------------------------
# AI-модели — управление (используется в routes/api.py)
# ---------------------------------------------------------------------------

def db_get_models(model_type: str):
    """Возвращает список моделей заданного типа ('text' или 'text-to-video')."""
    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT m.id, m.name, m.url, m.body, m."order", m.active,
                           p.name AS platform_name
                    FROM ai_models m
                    LEFT JOIN ai_platforms p ON p.id = m.platform_id
                    WHERE m.type = %s
                    ORDER BY m."order" ASC
                """, (model_type,))
                rows = cur.fetchall()
        return [dict(r) for r in rows]
    except Exception as e:
        print(f"[DB] Ошибка db_get_models({model_type}): {e}")
        return []


def db_activate_model(model_id: str, model_type: str):
    """Деактивирует все модели типа, активирует указанную."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE ai_models SET active = FALSE WHERE type = %s", (model_type,))
                cur.execute("UPDATE ai_models SET active = TRUE  WHERE id = %s", (model_id,))
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_activate_model: {e}")
        return False


def db_reorder_models(ids: list):
    """Выставляет поле order по порядку переданных UUID."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                for idx, model_id in enumerate(ids, start=1):
                    cur.execute('UPDATE ai_models SET "order" = %s WHERE id = %s', (idx, model_id))
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_reorder_models: {e}")
        return False


# ---------------------------------------------------------------------------
# Cleanup — очистка устаревших данных
# ---------------------------------------------------------------------------

def db_cleanup_log_entries(log_lifetime_days: int) -> int:
    """Удаляет log_entries записи, чей родительский лог старше log_lifetime_days.
    Возвращает количество удалённых строк."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM log_entries
                    WHERE log_id IN (
                        SELECT id FROM log
                        WHERE time_point < now() - make_interval(days => %s)
                    )
                """, (log_lifetime_days,))
                count = cur.rowcount
            conn.commit()
        return count
    except Exception as e:
        print(f"[DB] Ошибка db_cleanup_log_entries: {e}")
        return 0


def db_cleanup_logs(short_log_lifetime_days: int) -> int:
    """Удаляет записи log (вместе с оставшимися log_entries) старше short_log_lifetime_days.
    Возвращает количество удалённых строк."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM log_entries
                    WHERE log_id IN (
                        SELECT id FROM log
                        WHERE time_point < now() - make_interval(days => %s)
                    )
                """, (short_log_lifetime_days,))
                cur.execute("""
                    DELETE FROM log
                    WHERE time_point < now() - make_interval(days => %s)
                """, (short_log_lifetime_days,))
                count = cur.rowcount
            conn.commit()
        return count
    except Exception as e:
        print(f"[DB] Ошибка db_cleanup_logs: {e}")
        return 0


def db_clear_all_history():
    """Удаляет всю историю: log_entries, log, завершённые батчи и осиротевшие сюжеты."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM log_entries")
                le = cur.rowcount
                cur.execute("DELETE FROM log")
                ll = cur.rowcount
                cur.execute("""
                    DELETE FROM batches
                    WHERE status IN (
                        'published', 'устарел',
                        'publish_error', 'video_error', 'transcode_error'
                    )
                """)
                bl = cur.rowcount
                cur.execute("""
                    DELETE FROM stories
                    WHERE NOT EXISTS (
                        SELECT 1 FROM batches WHERE story_id = stories.id
                    )
                """)
                sl = cur.rowcount
            conn.commit()
        print(f"[DB] Очистка истории: log_entries={le}, log={ll}, batches={bl}, stories={sl}")
        return {"log_entries": le, "logs": ll, "batches": bl, "stories": sl}
    except Exception as e:
        print(f"[DB] Ошибка db_clear_all_history: {e}")
        raise


def db_cleanup_video_data(file_lifetime_days: int) -> int:
    """Обнуляет video_data у опубликованных/устаревших батчей старше file_lifetime_days.
    Сама запись батча сохраняется, удаляется только бинарник.
    Возвращает количество обновлённых батчей."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE batches SET video_data = NULL
                    WHERE status IN ('published', 'устарел')
                      AND completed_at < now() - make_interval(days => %s)
                      AND video_data IS NOT NULL
                """, (file_lifetime_days,))
                count = cur.rowcount
            conn.commit()
        return count
    except Exception as e:
        print(f"[DB] Ошибка db_cleanup_video_data: {e}")
        return 0


def db_cleanup_batches(batch_lifetime_days: int) -> int:
    """Удаляет батчи со статусом 'published'/'устарел' старше batch_lifetime_days.
    Удаляет связанные логи и осиротевшие stories.
    Возвращает количество удалённых батчей."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id FROM batches
                    WHERE status IN ('published', 'устарел')
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

                cur.execute("""
                    DELETE FROM stories
                    WHERE id NOT IN (
                        SELECT DISTINCT story_id FROM batches WHERE story_id IS NOT NULL
                    )
                """)

            conn.commit()
        return len(batch_ids)
    except Exception as e:
        print(f"[DB] Ошибка db_cleanup_batches: {e}")
        return 0
