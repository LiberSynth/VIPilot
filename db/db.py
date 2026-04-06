import time
import json
import psycopg2
import psycopg2.extras

from .connection import get_db


# ---------------------------------------------------------------------------
# Окружение (environment)
# ---------------------------------------------------------------------------

def env_get(key, default=""):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT value FROM environment WHERE key = %s", (key,))
                row = cur.fetchone()
                return row[0] if row else default
    except Exception as e:
        print(f"[DB] Ошибка чтения env {key}: {e}")
        return default


def env_set(key, value):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO environment (key, value) VALUES (%s, %s)
                    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                    """,
                    (key, value),
                )
            conn.commit()
    except Exception as e:
        print(f"[DB] Ошибка записи env {key}: {e}")


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
                cur.execute("SELECT id, time_utc, created_at FROM schedule ORDER BY time_utc")
                rows = cur.fetchall()
        return [{"id": str(row[0]), "time_utc": row[1], "created_at": row[2]} for row in rows]
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
                    "SELECT id, name, aspect_ratio_x, aspect_ratio_y, transcode FROM targets WHERE active = TRUE"
                )
                rows = cur.fetchall()
        return [
            {"id": str(row[0]), "name": row[1], "aspect_ratio_x": row[2], "aspect_ratio_y": row[3], "transcode": bool(row[4])}
            for row in rows
        ]
    except Exception as e:
        print(f"[DB] Ошибка db_get_active_targets: {e}")
        return []


def db_update_target_transcode(target_id, value: bool):
    """Обновляет флаг транскодирования для указанного target."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE targets SET transcode = %s WHERE id = %s",
                    (value, target_id),
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_update_target_transcode: {e}")
        return False


def db_update_target_aspect_ratio(target_id, x, y):
    """Обновляет соотношение сторон для указанного target."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE targets SET aspect_ratio_x = %s, aspect_ratio_y = %s WHERE id = %s",
                    (x, y, target_id)
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_update_target_aspect_ratio: {e}")
        return False


# ---------------------------------------------------------------------------
# Батчи
# ---------------------------------------------------------------------------

def db_ensure_batch(scheduled_at, target_id):
    """Создаёт батч если нет активного (не отменённого) батча для этого слота.
    Отменённые батчи игнорируются — они остаются в БД как есть.
    Возвращает UUID нового батча или None если активный батч уже существует."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id FROM batches
                    WHERE scheduled_at = %s
                      AND target_id    = %s
                      AND status      != 'cancelled'
                    LIMIT 1
                    """,
                    (scheduled_at, target_id),
                )
                if cur.fetchone():
                    return None
                cur.execute(
                    """
                    INSERT INTO batches (scheduled_at, target_id)
                    VALUES (%s, %s)
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

def db_create_adhoc_batch(target_id):
    """Создаёт внеплановый батч с adhoc=True и scheduled_at=NULL.
    Возвращает UUID нового батча или None при ошибке."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO batches (scheduled_at, target_id, adhoc)
                    VALUES (NULL, %s, TRUE)
                    RETURNING id
                """, (target_id,))
                row = cur.fetchone()
            conn.commit()
        return str(row[0]) if row else None
    except Exception as e:
        print(f"[DB] Ошибка db_create_adhoc_batch: {e}")
        return None


def db_create_probe_batch(video_model_id):
    """Создаёт pending-батч без таргета с зафиксированной video_model_id.
    Возвращает UUID батча или None."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO batches
                        (target_id, status, adhoc, video_model_id)
                    VALUES (NULL, 'pending', TRUE, %s)
                    RETURNING id
                """, (video_model_id,))
                row = cur.fetchone()
            conn.commit()
        return str(row[0]) if row else None
    except Exception as e:
        print(f"[DB] Ошибка db_create_probe_batch: {e}")
        return None


def db_create_story_probe_batch(text_model_id):
    """Создаёт pending-батч без таргета для пробного запроса сценария.
    data-флаг story_probe=true сигнализирует пайплайну завершить батч
    в статусе story_probe (без передачи в видео-конвейер)."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO batches
                        (target_id, status, adhoc, text_model_id, data)
                    VALUES (NULL, 'pending', TRUE, %s, '{"story_probe": true}')
                    RETURNING id
                """, (text_model_id,))
                row = cur.fetchone()
            conn.commit()
        return str(row[0]) if row else None
    except Exception as e:
        print(f"[DB] Ошибка db_create_story_probe_batch: {e}")
        return None


def db_set_batch_story_probe(batch_id, story_id):
    """Переводит батч в status='story_probe', сохраняет story_id и проставляет completed_at."""
    _assert_known_status('story_probe')
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
    """Привязывает story к батчу и переводит статус в 'story_ready'."""
    _assert_known_status('story_ready')
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


def db_set_batch_text_model(batch_id, model_id):
    """Сохраняет id текстовой модели, сгенерировавшей сюжет."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE batches SET text_model_id = %s WHERE id = %s",
                    (model_id, batch_id),
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_set_batch_text_model: {e}")
        return False


def db_set_batch_video_model(batch_id, model_id):
    """Сохраняет id видео-модели, сгенерировавшей видео."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE batches SET video_model_id = %s WHERE id = %s",
                    (model_id, batch_id),
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_set_batch_video_model: {e}")
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
                      AND (m.grade IS NULL OR m.grade != 'rejected')
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

def db_is_batch_scheduled(scheduled_at, target_id):
    """Проверяет актуальность батча: слот расписания существует И таргет активен.
    Возвращает True если оба условия выполнены, False если хотя бы одно нарушено.
    Adhoc-батчи (scheduled_at=None) всегда считаются актуальными."""
    if scheduled_at is None:
        return True
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
    """Переводит батч в status='cancelled'."""
    return db_set_batch_status(batch_id, 'cancelled')


def db_cancel_obsolete_waiting_batches():
    """Отменяет batches в статусе transcode_ready/story_posted, у которых слот расписания
    больше не существует или таргет деактивирован. Возвращает список отменённых batch_id."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE batches
                    SET status = 'cancelled'
                    WHERE status IN ('transcode_ready', 'story_posted')
                      AND scheduled_at IS NOT NULL
                      AND (
                          NOT EXISTS (
                              SELECT 1 FROM schedule
                              WHERE time_utc = to_char(
                                  batches.scheduled_at AT TIME ZONE 'UTC', 'HH24:MI'
                              )
                          )
                          OR NOT EXISTS (
                              SELECT 1 FROM targets
                              WHERE id = batches.target_id AND active = TRUE
                          )
                      )
                    RETURNING id
                """)
                rows = cur.fetchall()
            conn.commit()
        return [str(r[0]) for r in rows]
    except Exception as e:
        print(f"[DB] Ошибка db_cancel_obsolete_waiting_batches: {e}")
        return []


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


def db_get_active_text_models():
    """Возвращает список всех активных text-моделей в порядке order."""
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
            result.append({
                'platform_url': row['platform_url'],
                'model_url':    row['model_url'],
                'body_tpl':     row['body'] if isinstance(row['body'], dict) else {},
                'name':         row['name'],
                'id':           str(row['id']),
            })
        return result
    except Exception as e:
        print(f"[DB] Ошибка db_get_active_text_models: {e}")
        return []


def db_get_text_model_by_id(model_id: str):
    """Возвращает данные text-модели по ID (без фильтра по active)."""
    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT p.url AS platform_url, m.url AS model_url,
                           m.body, m.name, m.id
                    FROM ai_models m
                    JOIN ai_platforms p ON p.id = m.platform_id
                    WHERE m.id = %s AND m.type = 'text'
                """, (model_id,))
                row = cur.fetchone()
        if not row:
            return None
        return {
            'platform_url': row['platform_url'],
            'model_url':    row['model_url'],
            'body_tpl':     row['body'] if isinstance(row['body'], dict) else {},
            'name':         row['name'],
            'id':           str(row['id']),
        }
    except Exception as e:
        print(f"[DB] Ошибка db_get_text_model_by_id: {e}")
        return None


def db_get_video_model_by_id(model_id: str):
    """Возвращает данные text-to-video модели по ID (без фильтра по active)."""
    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT p.url AS platform_url, m.url AS model_url,
                           m.body, m.name, m.id
                    FROM ai_models m
                    JOIN ai_platforms p ON p.id = m.platform_id
                    WHERE m.id = %s AND m.type = 'text-to-video'
                """, (model_id,))
                row = cur.fetchone()
        if not row:
            return None
        platform_url = row['platform_url']
        model_url = row['model_url']
        return {
            'platform_url': platform_url,
            'model_url':    model_url,
            'body_tpl':     row['body'] if isinstance(row['body'], dict) else {},
            'name':         row['name'],
            'id':           str(row['id']),
            'submit_url':   f"{platform_url}/{model_url}",
        }
    except Exception as e:
        print(f"[DB] Ошибка db_get_video_model_by_id: {e}")
        return None


def db_get_active_video_models():
    """Возвращает список всех активных text-to-video моделей в порядке order."""
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
        result = []
        for row in rows:
            result.append({
                'platform_url': row['platform_url'],
                'model_url':    row['model_url'],
                'body_tpl':     row['body'] if isinstance(row['body'], dict) else {},
                'name':         row['name'],
                'id':           str(row['id']),
                'submit_url':   f"{row['platform_url']}/{row['model_url']}",
            })
        return result
    except Exception as e:
        print(f"[DB] Ошибка db_get_active_video_models: {e}")
        return []


def db_set_batch_video_pending(batch_id, job_data):
    """Сохраняет данные задания и переводит батч в status='video_pending'."""
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


def db_set_batch_video_ready(batch_id, video_url):
    """Сохраняет video_url и переводит батч в status='video_ready'."""
    _assert_known_status('video_ready')
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


def db_set_batch_original_video(batch_id, video_data: bytes):
    """Сохраняет оригинальное видео (до транскодирования) в поле video_data_original."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE batches SET video_data_original = %s WHERE id = %s",
                    (psycopg2.Binary(video_data), batch_id),
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_set_batch_original_video: {e}")
        return False


def db_get_batch_original_video(batch_id) -> bytes | None:
    """Возвращает оригинальные байты видео (video_data_original) для батча, или None."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT video_data_original FROM batches WHERE id = %s", (batch_id,))
                row = cur.fetchone()
        if row and row[0] is not None:
            return bytes(row[0])
        return None
    except Exception as e:
        print(f"[DB] Ошибка db_get_batch_original_video: {e}")
        return None


def db_set_batch_transcode_skip(batch_id):
    """Переводит батч в status='transcode_ready' без изменения video_data_transcoded.
    Используется когда транскодирование отключено или завершилось некритичной ошибкой."""
    return db_set_batch_status(batch_id, 'transcode_ready')


def db_set_batch_transcode_ready(batch_id, video_data: bytes):
    """Сохраняет транскодированный файл в БД и переводит батч в status='transcode_ready'."""
    _assert_known_status('transcode_ready')
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE batches SET status = 'transcode_ready', video_data_transcoded = %s WHERE id = %s",
                    (psycopg2.Binary(video_data), batch_id),
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_set_batch_transcode_ready: {e}")
        return False


def db_get_random_real_original_video() -> bytes | None:
    """Возвращает video_data_original случайного батча с реальным (не эмулированным) видео."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT video_data_original FROM batches
                    WHERE video_data_original IS NOT NULL
                      AND (video_url IS NULL OR video_url NOT LIKE 'emulation://%')
                    ORDER BY random() LIMIT 1
                    """
                )
                row = cur.fetchone()
                return bytes(row[0]) if row else None
    except Exception as e:
        print(f"[DB] Ошибка db_get_random_real_original_video: {e}")
        return None


def db_get_batch_video_data(batch_id) -> bytes | None:
    """Возвращает байты видео для батча: транскодированное если есть, иначе оригинал."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT video_data_transcoded, video_data_original FROM batches WHERE id = %s",
                    (batch_id,),
                )
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


def db_steal_video_from_cancelled(batch_id) -> str | None:
    """Ищет самый старый батч-донор с видео:
    — отменённые батчи (status = 'отменён')
    — завершённые пробные батчи (status = 'probe', target_id IS NULL)
    Донором считается батч у которого есть video_data_transcoded или video_data_original.
    Если есть транскодированное — переносит его, статус получателя → transcode_ready.
    Если только оригинал — переносит его, статус получателя → video_ready.
    У донора обнуляет перенесённое поле. Возвращает id донора (str) или None."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, video_data_transcoded, video_data_original, video_url
                    FROM batches
                    WHERE (video_data_transcoded IS NOT NULL OR video_data_original IS NOT NULL)
                      AND (
                        status = 'cancelled'
                        OR (status = 'probe' AND target_id IS NULL)
                      )
                    ORDER BY created_at ASC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                """)
                donor = cur.fetchone()
                if not donor:
                    return None

                donor_id, video_data_transcoded, video_data_original, video_url = donor

                if video_data_transcoded is not None:
                    # Транскодированное есть — переносим его, получатель сразу в transcode_ready
                    cur.execute("""
                        UPDATE batches
                        SET status                = 'transcode_ready',
                            video_data_transcoded = %s,
                            video_url             = %s
                        WHERE id = %s
                    """, (psycopg2.Binary(bytes(video_data_transcoded)), video_url, batch_id))
                    cur.execute(
                        "UPDATE batches SET video_data_transcoded = NULL WHERE id = %s",
                        (donor_id,),
                    )
                else:
                    # Только оригинал — переносим его, получатель идёт на транскодирование
                    cur.execute("""
                        UPDATE batches
                        SET status               = 'video_ready',
                            video_data_original  = %s,
                            video_url            = %s
                        WHERE id = %s
                    """, (psycopg2.Binary(bytes(video_data_original)), video_url, batch_id))
                    cur.execute(
                        "UPDATE batches SET video_data_original = NULL WHERE id = %s",
                        (donor_id,),
                    )

            conn.commit()
        return str(donor_id)
    except Exception as e:
        print(f"[DB] Ошибка db_steal_video_from_cancelled: {e}")
        return None


def db_get_donor_count() -> int:
    """Возвращает количество батчей-доноров (отменённые или пробные без target с видео)."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*)
                    FROM batches
                    WHERE (video_data_transcoded IS NOT NULL OR video_data_original IS NOT NULL)
                      AND (
                        status = 'cancelled'
                        OR (status = 'probe' AND target_id IS NULL)
                      )
                """)
                return cur.fetchone()[0]
    except Exception as e:
        print(f"[DB] Ошибка db_get_donor_count: {e}")
        return 0


def db_set_batch_transcode_error(batch_id):
    """Переводит батч в status='transcode_error'."""
    return db_set_batch_status(batch_id, 'transcode_error')


def db_get_batch_logs(batch_id):
    """
    Возвращает один батч в формате монитора — для поллинга диалога пробы.
    Набор полей идентичен тому, что возвращает db_get_monitor для каждого батча,
    чтобы фронтенд мог использовать те же функции renderLogItem/groupLogsByPipeline.
    """
    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT
                        b.id::text                                              AS batch_id,
                        b.scheduled_at,
                        b.adhoc,
                        b.status                                                AS batch_status,
                        b.created_at,
                        t.name                                                  AS target_name,
                        b.story_id::text                                        AS story_id,
                        (b.video_data_transcoded IS NOT NULL
                         OR b.video_data_original IS NOT NULL)                  AS has_video_data,
                        tm.name                                                 AS text_model_name,
                        vm.name                                                 AS video_model_name
                    FROM batches b
                    LEFT JOIN targets   t  ON t.id  = b.target_id
                    LEFT JOIN ai_models tm ON tm.id = b.text_model_id
                    LEFT JOIN ai_models vm ON vm.id = b.video_model_id
                    WHERE b.id = %s
                """, (batch_id,))
                row = cur.fetchone()
                if not row:
                    return None

                cur.execute("""
                    SELECT l.id::text, l.pipeline, l.message, l.status,
                           l.created_at,
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
                    WHERE l.batch_id = %s
                    GROUP BY l.id
                    ORDER BY l.created_at
                """, (batch_id,))
                logs = cur.fetchall()

        return {
            'batch_id':         row['batch_id'],
            'batch_status':     row['batch_status'],
            'created_at':       row['created_at'].isoformat() if row['created_at'] else None,
            'adhoc':            row['adhoc'],
            'scheduled_at':     row['scheduled_at'].isoformat() if row['scheduled_at'] else None,
            'target_name':      row['target_name'],
            'story_id':         row['story_id'],
            'has_video_data':   bool(row['has_video_data']),
            'text_model_name':  row['text_model_name'],
            'video_model_name': row['video_model_name'],
            'logs': [
                {
                    'id':         r['id'],
                    'pipeline':   r['pipeline'],
                    'message':    r['message'],
                    'status':     r['status'],
                    'created_at': r['created_at'].isoformat() if r['created_at'] else None,
                    'entries':    r['entries'],
                }
                for r in logs
            ],
        }
    except Exception as e:
        print(f"[DB] Ошибка db_get_batch_logs: {e}")
        return None


def db_set_batch_probe(batch_id):
    """Переводит батч в status='probe' и ставит completed_at."""
    _assert_known_status('probe')
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE batches SET status = 'probe', completed_at = now() WHERE id = %s",
                    (batch_id,),
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_set_batch_probe: {e}")
        return False


def db_set_batch_published(batch_id):
    """Переводит батч в status='published' и ставит completed_at."""
    _assert_known_status('published')
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
    return db_set_batch_status(batch_id, 'publish_error')


def db_set_batch_video_error(batch_id):
    """Переводит батч в status='video_error'."""
    return db_set_batch_status(batch_id, 'video_error')


def db_set_batch_story_error(batch_id):
    """Переводит батч в status='story_error'."""
    return db_set_batch_status(batch_id, 'story_error')


def db_set_batch_story_posted(batch_id):
    """Переводит батч в status='story_posted' (история опубликована, стена ожидает)."""
    return db_set_batch_status(batch_id, 'story_posted')


def db_set_batch_story_ready_from_error(batch_id):
    """Откатывает батч в story_ready после сбоя видео, сохраняя story_id.
    Очищает data (request_id/url), но НЕ трогает story_id — сюжет не регенерируется."""
    _assert_known_status('story_ready')
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE batches SET status = 'story_ready', data = NULL WHERE id = %s",
                    (batch_id,),
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_set_batch_story_ready_from_error: {e}")
        return False


def db_set_batch_pending(batch_id):
    """Сбрасывает батч в статус 'pending', очищая story_id, video_url и data.
    Используется для перезапуска цикла генерации после фатального сбоя видео."""
    _assert_known_status('pending')
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
# Ручной сброс батча по пайплайну (для кнопки "Перезапустить" в мониторе)
# ---------------------------------------------------------------------------

PIPELINE_RESET_STATUS = {
    'story':     'pending',
    'video':     'story_ready',
    'transcode': 'video_ready',
    'publish':   'transcode_ready',
}


def db_reset_batch_pipeline(batch_id: str, pipeline: str) -> bool:
    """Сбрасывает статус батча до входного статуса для указанного пайплайна.
    Возвращает True если батч обновлён, False если pipeline неизвестен или ошибка."""
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
                           m.grade, m.price, p.name AS platform_name
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


def db_set_model_grade(model_id: str, grade: str):
    """Устанавливает grade для модели."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE ai_models SET grade = %s WHERE id = %s",
                    (grade, model_id),
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_set_model_grade: {e}")
        return False


def db_toggle_model(model_id: str):
    """Переключает active для одной модели (не затрагивает остальные)."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE ai_models SET active = NOT active WHERE id = %s",
                    (model_id,),
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_toggle_model: {e}")
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
# Cleanup — очистка отменённых данных
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
    """Удаляет записи log (вместе с оставшимися log_entries) старше short_log_lifetime_days.
    Возвращает количество удалённых строк."""
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
    """Удаляет всю историю: log_entries, log, завершённые батчи и осиротевшие сюжеты."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM log_entries")
                le = cur.rowcount
                cur.execute("DELETE FROM log")
                ll = cur.rowcount
                cur.execute("DELETE FROM batches")
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
    """Обнуляет video_data_transcoded у опубликованных/отменённых батчей старше file_lifetime_days.
    Сама запись батча сохраняется, удаляется только бинарник.
    Возвращает количество обновлённых батчей."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE batches SET video_data_transcoded = NULL
                    WHERE status IN ('published', 'cancelled')
                      AND completed_at < now() - make_interval(days => %s)
                      AND video_data_transcoded IS NOT NULL
                """, (file_lifetime_days,))
                count = cur.rowcount
            conn.commit()
        return count
    except Exception as e:
        print(f"[DB] Ошибка db_cleanup_video_data: {e}")
        return 0


def db_get_last_pipeline_run(pipeline):
    """Возвращает MAX(created_at) из таблицы log для указанного пайплайна,
    или None если записей нет."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT MAX(created_at) FROM log WHERE pipeline = %s",
                    (pipeline,),
                )
                row = cur.fetchone()
        return row[0] if row and row[0] is not None else None
    except Exception as e:
        print(f"[DB] Ошибка db_get_last_pipeline_run: {e}")
        return None


def db_get_batch_by_id(batch_id):
    """Возвращает батч по ID со всеми полями, нужными пайплайнам."""
    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT b.id, b.scheduled_at, b.target_id, b.story_id,
                           b.video_url, b.status, b.adhoc, b.data,
                           b.text_model_id, b.video_model_id,
                           t.name            AS target_name,
                           t.aspect_ratio_x,
                           t.aspect_ratio_y,
                           t.transcode       AS target_transcode
                    FROM batches b
                    LEFT JOIN targets t ON t.id = b.target_id
                    WHERE b.id = %s
                """, (batch_id,))
                row = cur.fetchone()
        return dict(row) if row else None
    except Exception as e:
        print(f"[DB] Ошибка db_get_batch_by_id: {e}")
        return None


def db_get_actionable_batches():
    """Возвращает список батчей, готовых к обработке (FIFO по created_at)."""
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
                        'transcode_ready', 'story_posted'
                    )
                    ORDER BY created_at ASC
                """)
                rows = cur.fetchall()
        return [dict(r) for r in rows]
    except Exception as e:
        print(f"[DB] Ошибка db_get_actionable_batches: {e}")
        return []


def db_get_distinct_batch_statuses():
    """Возвращает множество всех уникальных статусов батчей в БД."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT DISTINCT status FROM batches")
                rows = cur.fetchall()
        return {row[0] for row in rows}
    except Exception as e:
        print(f"[DB] Ошибка db_get_distinct_batch_statuses: {e}")
        return set()


def db_get_batches_with_unknown_status(known_statuses):
    """Возвращает словарь {batch_id: status} для батчей с неизвестным статусом."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, status FROM batches")
                rows = cur.fetchall()
        return {row[0]: row[1] for row in rows if row[1] not in known_statuses}
    except Exception as e:
        print(f"[DB] Ошибка db_get_batches_with_unknown_status: {e}")
        return {}


KNOWN_BATCH_STATUSES = frozenset({
    # story pipeline
    'pending', 'story_generating',
    # video pipeline
    'story_ready', 'video_generating', 'video_pending',
    # transcode pipeline
    'video_ready', 'transcoding',
    # publish pipeline
    'transcode_ready', 'story_posted',
    # terminal
    'cancelled', 'error', 'probe', 'story_probe', 'story_error',
    'video_error', 'transcode_error', 'publish_error', 'published',
    'fatal_error',
})


def _assert_known_status(status: str) -> None:
    """Бросает ValueError если статус не зарегистрирован в KNOWN_BATCH_STATUSES.
    Вызывай в начале каждого сеттера, чтобы поймать рассинхронизацию с константой."""
    if status not in KNOWN_BATCH_STATUSES:
        raise ValueError(
            f"[DB] Неизвестный статус '{status}' — добавь его в KNOWN_BATCH_STATUSES"
        )


def db_set_batch_status(batch_id: str, status: str) -> bool:
    """Универсальный сеттер статуса батча с валидацией.
    Используй вместо специфичных сеттеров, когда статус задаётся динамически.
    При передаче незарегистрированного статуса бросает ValueError, запись не производится.
    """
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


def db_set_batch_fatal_error(batch_id: str) -> bool:
    """Переводит батч в статус fatal_error.
    Вызывается при обнаружении ошибки логики приложения (программного бага),
    чтобы остановить бесконечный retry. Логирование — ответственность вызывающей стороны.
    """
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE batches SET status = 'fatal_error' WHERE id = %s",
                    (batch_id,),
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_set_batch_fatal_error: {e}")
        return False


def db_set_batch_story_generating_by_id(batch_id):
    """Переводит конкретный батч pending → story_generating (атомарно)."""
    _assert_known_status('story_generating')
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE batches SET status = 'story_generating'
                    WHERE id = %s AND status = 'pending'
                """, (batch_id,))
                n = cur.rowcount
            conn.commit()
        return n > 0
    except Exception as e:
        print(f"[DB] Ошибка db_set_batch_story_generating_by_id: {e}")
        return False


def db_set_batch_video_generating_by_id(batch_id):
    """Переводит конкретный батч story_ready → video_generating (атомарно)."""
    _assert_known_status('video_generating')
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE batches SET status = 'video_generating'
                    WHERE id = %s AND status = 'story_ready'
                """, (batch_id,))
                n = cur.rowcount
            conn.commit()
        return n > 0
    except Exception as e:
        print(f"[DB] Ошибка db_set_batch_video_generating_by_id: {e}")
        return False


def db_set_batch_transcoding_by_id(batch_id):
    """Переводит конкретный батч video_ready → transcoding (атомарно)."""
    _assert_known_status('transcoding')
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE batches SET status = 'transcoding'
                    WHERE id = %s AND status = 'video_ready'
                """, (batch_id,))
                n = cur.rowcount
            conn.commit()
        return n > 0
    except Exception as e:
        print(f"[DB] Ошибка db_set_batch_transcoding_by_id: {e}")
        return False


def db_cleanup_batches(batch_lifetime_days: int) -> int:
    """Удаляет батчи со статусом 'published'/'отменён' старше batch_lifetime_days.
    Удаляет связанные логи и осиротевшие stories.
    Возвращает количество удалённых батчей."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id FROM batches
                    WHERE status IN ('published', 'cancelled')
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
