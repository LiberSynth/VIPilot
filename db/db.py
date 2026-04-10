import time
import json
import psycopg2
import psycopg2.extras

from .connection import get_db
from log.log import FINAL_BATCH_STATUSES


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
                    'SELECT id, name, aspect_ratio_x, aspect_ratio_y, transcode, config, slug FROM targets WHERE active = TRUE ORDER BY "order", name'
                )
                rows = cur.fetchall()
        return [
            {"id": str(row[0]), "name": row[1], "aspect_ratio_x": row[2], "aspect_ratio_y": row[3], "transcode": bool(row[4]), "config": row[5] or {}, "slug": row[6] or ""}
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


def db_get_target_by_name(name: str) -> dict | None:
    """Возвращает таргет по имени (id, name, config, active, ...)."""
    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT id::text, name, active, aspect_ratio_x, aspect_ratio_y, transcode, config "
                    "FROM targets WHERE name = %s LIMIT 1",
                    (name,),
                )
                row = cur.fetchone()
        return dict(row) if row else None
    except Exception as e:
        print(f"[DB] Ошибка db_get_target_by_name: {e}")
        return None


def db_update_target_config(target_id: str, config: dict) -> bool:
    """Обновляет JSONB-поле config для указанного target."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE targets SET config = %s::jsonb WHERE id = %s::uuid",
                    (json.dumps(config), target_id),
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_update_target_config: {e}")
        return False


def db_update_target_publish_method_by_slug(slug: str, methods: dict) -> bool:
    """Обновляет config.publish_method у таргета по slug (JSONB merge)."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE targets SET config = config || jsonb_build_object('publish_method', %s::jsonb)"
                    " WHERE slug = %s",
                    (json.dumps(methods), slug),
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_update_target_publish_method_by_slug: {e}")
        return False


def db_get_target_session_context(target_id: str) -> dict | None:
    """Возвращает Playwright storage state (cookies) для таргета, или None."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT session_context FROM targets WHERE id = %s::uuid",
                    (target_id,),
                )
                row = cur.fetchone()
        if row and row[0]:
            return row[0]
        return None
    except Exception as e:
        print(f"[DB] Ошибка db_get_target_session_context: {e}")
        return None


def db_set_target_session_context(target_id: str, state: dict) -> bool:
    """Сохраняет Playwright storage state (cookies) для таргета."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE targets SET session_context = %s::jsonb WHERE id = %s::uuid",
                    (json.dumps(state), target_id),
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_set_target_session_context: {e}")
        return False


def db_get_target_session_context_saved_at(target_id: str) -> str | None:
    """
    Возвращает timestamp последнего сохранения session_context из поля
    session_context->'saved_at' (ISO-строку), или None.
    """
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT session_context->>'saved_at' FROM targets WHERE id = %s::uuid",
                    (target_id,),
                )
                row = cur.fetchone()
        return row[0] if row else None
    except Exception as e:
        print(f"[DB] Ошибка db_get_target_session_context_saved_at: {e}")
        return None


# ---------------------------------------------------------------------------
# Батчи
# ---------------------------------------------------------------------------

def db_ensure_batch(scheduled_at):
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


# ---------------------------------------------------------------------------
# Батчи — пайплайновые функции
# ---------------------------------------------------------------------------

def db_create_adhoc_batch():
    """Создаёт внеплановый батч с type='adhoc' и scheduled_at=NULL.
    Возвращает UUID нового батча или None при ошибке."""
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


def db_create_probe_batch(video_model_id):
    """Создаёт pending-батч с type='probe' и зафиксированной video_model_id.
    model_id сохраняется в movies при генерации видео через db_set_batch_video_model.
    Пока создаём batсh и пустую запись movies с model_id.
    Возвращает UUID батча или None."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO movies (model_id)
                    VALUES (%s)
                    RETURNING id
                """, (video_model_id,))
                movie_row = cur.fetchone()
                movie_id = movie_row[0]
                cur.execute("""
                    INSERT INTO batches
                        (status, type, movie_id)
                    VALUES ('pending', 'probe', %s)
                    RETURNING id
                """, (movie_id,))
                row = cur.fetchone()
            conn.commit()
        return str(row[0]) if row else None
    except Exception as e:
        print(f"[DB] Ошибка db_create_probe_batch: {e}")
        return None


def db_create_story_probe_batch(text_model_id):
    """Создаёт pending-батч с type='probe' для пробного запроса сценария.
    data-флаг story_probe=true сигнализирует пайплайну завершить батч
    в статусе story_probe (без передачи в видео-конвейер).
    probe_model_id хранится в data, а не в колонке text_model_id."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO batches
                        (status, type, data)
                    VALUES ('pending', 'probe', %s)
                    RETURNING id
                """, (json.dumps({"story_probe": True, "probe_model_id": str(text_model_id)}),))
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


def db_set_batch_story_id(batch_id, story_id):
    """Устанавливает story_id батча без изменения статуса."""
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


def db_set_story_model(story_id, model_id):
    """Сохраняет id текстовой модели, сгенерировавшей сюжет, в таблицу stories."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE stories SET model_id = %s WHERE id = %s",
                    (model_id, story_id),
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_set_story_model: {e}")
        return False


def db_set_batch_video_model(batch_id, model_id):
    """Сохраняет id видео-модели, сгенерировавшей видео, в запись movies.
    Если у батча нет movie_id — создаёт запись в movies."""
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


# ---------------------------------------------------------------------------
# Истории (stories)
# ---------------------------------------------------------------------------

def db_create_story(model_id, title, result):
    """Сохраняет сгенерированный сюжет в таблицу stories. Возвращает UUID или None."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO stories (model_id, title, content) VALUES (%s, %s, %s) RETURNING id",
                    (model_id, title, result),
                )
                row = cur.fetchone()
            conn.commit()
        return str(row[0]) if row else None
    except Exception as e:
        print(f"[DB] Ошибка db_create_story: {e}")
        return None


def db_upsert_story_draft(story_id, title, content):
    """Создаёт или обновляет черновик сюжета (model_id=NULL, grade='good').
    Если story_id передан и запись существует — обновляет title и content.
    Иначе создаёт новую запись через INSERT ... ON CONFLICT DO UPDATE.
    Возвращает UUID или None."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                if story_id:
                    cur.execute(
                        """
                        INSERT INTO stories (id, model_id, title, content, grade)
                        VALUES (%s::uuid, NULL, %s, %s, 'good')
                        ON CONFLICT (id) DO UPDATE
                            SET title = EXCLUDED.title,
                                content = EXCLUDED.content,
                                grade = 'good'
                            WHERE stories.model_id IS NULL
                        RETURNING id
                        """,
                        (story_id, title, content),
                    )
                    row = cur.fetchone()
                    if row:
                        conn.commit()
                        return str(row[0])
                cur.execute(
                    "INSERT INTO stories (model_id, title, content, grade) VALUES (NULL, %s, %s, 'good') RETURNING id",
                    (title, content),
                )
                row = cur.fetchone()
            conn.commit()
        return str(row[0]) if row else None
    except Exception as e:
        print(f"[DB] Ошибка db_upsert_story_draft: {e}")
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

def db_is_batch_scheduled(scheduled_at, batch_type='slot'):
    """Проверяет актуальность батча: слот расписания существует.
    Не-slot батчи (adhoc/probe) пропускаются без проверки — всегда возвращают True.
    Возвращает True если слот существует, False если удалён."""
    if batch_type != 'slot' or scheduled_at is None:
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
        return True  # безопасный fallback: не блокируем работу


def db_set_batch_cancelled(batch_id):
    """Переводит батч в status='cancelled'."""
    return db_set_batch_status(batch_id, 'cancelled')


def db_cancel_waiting_batches():
    """Отменяет slot-батчи в статусе transcode_ready или с промежуточными составными статусами
    (*.published, *.pending), у которых слот расписания больше не существует.
    Возвращает список отменённых batch_id."""
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


def db_get_story_text(story_id):
    """Возвращает text из stories.content по UUID, или None."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT content FROM stories WHERE id = %s", (story_id,))
                row = cur.fetchone()
        return row[0] if row else None
    except Exception as e:
        print(f"[DB] Ошибка db_get_story_text: {e}")
        return None


def db_get_story_title(story_id):
    """Возвращает stories.title по UUID, или None."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT title FROM stories WHERE id = %s", (story_id,))
                row = cur.fetchone()
        return row[0] if row else None
    except Exception as e:
        print(f"[DB] Ошибка db_get_story_title: {e}")
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
    """Сохраняет video_url в movies и переводит батч в status='video_ready'.
    Если у батча нет movie_id — создаёт запись в movies."""
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


def db_set_batch_original_video(batch_id, video_data: bytes):
    """Сохраняет оригинальное видео (до транскодирования) в movies.raw_data.
    Если у батча нет movie_id — создаёт запись в movies."""
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
    """Возвращает оригинальные байты видео (movies.raw_data) для батча, или None."""
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


def db_set_batch_transcode_skip(batch_id):
    """Переводит батч в status='transcode_ready' без изменения movies.transcoded_data.
    Используется когда транскодирование отключено или завершилось некритичной ошибкой."""
    return db_set_batch_status(batch_id, 'transcode_ready')


def db_set_batch_transcode_ready(batch_id, video_data: bytes):
    """Сохраняет транскодированный файл в movies.transcoded_data и переводит батч в status='transcode_ready'."""
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


def db_get_random_real_original_video() -> tuple[bytes, str, str | None] | None:
    """Возвращает (raw_data, batch_id, story_id) случайного батча с реальным видео.
    story_id может быть None, если у донора нет привязанного сюжета."""
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


def db_get_batch_video_data(batch_id) -> bytes | None:
    """Возвращает байты видео для батча: транскодированное если есть, иначе оригинал."""
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


def db_steal_video_from_cancelled(batch_id) -> str | None:
    """Ищет самый старый батч-донор с видео:
    — отменённые батчи (status = 'cancelled')
    — завершённые пробные батчи (status = 'probe', type = 'probe')
    Донором считается батч у которого есть movies.transcoded_data или movies.raw_data.
    Если есть транскодированное — переносит его, статус получателя → transcode_ready.
    Если только оригинал — переносит его, статус получателя → video_ready.
    У донора обнуляет перенесённое поле. Возвращает id донора (str) или None."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT b.id, m.transcoded_data, m.raw_data, m.url, b.story_id::text, b.movie_id, m.id AS mid
                    FROM batches b
                    JOIN movies m ON m.id = b.movie_id
                    WHERE (m.transcoded_data IS NOT NULL OR m.raw_data IS NOT NULL)
                      AND (
                        b.status = 'cancelled'
                        OR (b.status = 'probe' AND b.type = 'probe')
                      )
                    ORDER BY b.created_at ASC
                    LIMIT 1
                    FOR UPDATE OF b SKIP LOCKED
                """)
                donor = cur.fetchone()
                if not donor:
                    return None

                donor_id, video_data_transcoded, video_data_original, video_url, donor_story_id, donor_movie_id, donor_mid = donor

                if video_data_transcoded is not None:
                    # Транскодированное есть — переносим его, получатель сразу в transcode_ready
                    cur.execute("""
                        INSERT INTO movies (url, transcoded_data)
                        VALUES (%s, %s)
                        RETURNING id
                    """, (video_url, psycopg2.Binary(bytes(video_data_transcoded))))
                    new_movie_id = cur.fetchone()[0]
                    cur.execute("""
                        UPDATE batches
                        SET status   = 'transcode_ready',
                            movie_id = %s,
                            story_id = COALESCE(story_id, %s)
                        WHERE id = %s
                    """, (new_movie_id, donor_story_id, batch_id))
                    cur.execute(
                        "UPDATE movies SET transcoded_data = NULL WHERE id = %s",
                        (donor_mid,),
                    )
                else:
                    # Только оригинал — переносим его, получатель идёт на транскодирование
                    cur.execute("""
                        INSERT INTO movies (url, raw_data)
                        VALUES (%s, %s)
                        RETURNING id
                    """, (video_url, psycopg2.Binary(bytes(video_data_original))))
                    new_movie_id = cur.fetchone()[0]
                    cur.execute("""
                        UPDATE batches
                        SET status   = 'video_ready',
                            movie_id = %s,
                            story_id = COALESCE(story_id, %s)
                        WHERE id = %s
                    """, (new_movie_id, donor_story_id, batch_id))
                    cur.execute(
                        "UPDATE movies SET raw_data = NULL WHERE id = %s",
                        (donor_mid,),
                    )

            conn.commit()
        return str(donor_id)
    except Exception as e:
        print(f"[DB] Ошибка db_steal_video_from_cancelled: {e}")
        return None


def db_set_batch_story_ready_from_donor(batch_id: str, donor_batch_id: str, donor_story_id: str | None) -> bool:
    """Записывает donor_batch_id в batches.data, устанавливает story_id через COALESCE
    (не перезаписывает, если у батча уже есть свой story_id), переводит батч в story_ready.
    Если story_id взят у донора — очищает story_id у донора. Всё в одной транзакции.
    Видео не трогает."""
    _assert_known_status('story_ready')
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
                if donor_story_id and used_story_id and str(used_story_id) == str(donor_story_id):
                    cur.execute(
                        "UPDATE batches SET story_id = NULL WHERE id = %s::uuid",
                        (donor_batch_id,),
                    )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_set_batch_story_ready_from_donor: {e}")
        return False


def db_record_donor_batch_id(batch_id: str, donor_batch_id: str) -> bool:
    """Записывает donor_batch_id в batches.data без изменения story_id и статуса.
    Используется в story.py, чтобы зафиксировать донора видео ДО генерации собственного сюжета."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE batches
                    SET data = COALESCE(data, '{}'::jsonb) || jsonb_build_object('donor_batch_id', %s)
                    WHERE id = %s
                    """,
                    (donor_batch_id, batch_id),
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Ошибка db_record_donor_batch_id: {e}")
        return False


def db_find_donor_batch() -> tuple | None:
    """Ищет самый старый батч-донор с видео (SELECT + FOR UPDATE SKIP LOCKED),
    но не трогает видео и не меняет статус получателя.
    Возвращает (donor_id: str, donor_story_id: str | None) или None."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT b.id::text, b.story_id::text
                    FROM batches b
                    JOIN movies m ON m.id = b.movie_id
                    WHERE (m.transcoded_data IS NOT NULL OR m.raw_data IS NOT NULL)
                      AND (
                        b.status = 'cancelled'
                        OR (b.status = 'probe' AND b.type = 'probe')
                      )
                    ORDER BY b.created_at ASC
                    LIMIT 1
                    FOR UPDATE OF b SKIP LOCKED
                """)
                row = cur.fetchone()
                if not row:
                    return None
            conn.commit()
        return (row[0], row[1])
    except Exception as e:
        print(f"[DB] Ошибка db_find_donor_batch: {e}")
        return None


def db_steal_video_from_donor(donor_batch_id: str, batch_id: str) -> str | None:
    """Переносит movie_id от донора (donor_batch_id) в батч-получатель (batch_id).
    У донора обнуляет movie_id. Статус получателя:
      - transcode_ready если у movie есть transcoded_data
      - video_ready если только raw_data
    story_id получателя НЕ перезаписывается (проставлен на этапе story).
    Всё в одной транзакции. Возвращает id донора (str) или None."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT b.id, b.movie_id,
                           (m.transcoded_data IS NOT NULL) AS has_transcoded,
                           (m.raw_data IS NOT NULL) AS has_raw
                    FROM batches b
                    JOIN movies m ON m.id = b.movie_id
                    WHERE b.id = %s::uuid
                    FOR UPDATE OF b SKIP LOCKED
                """, (donor_batch_id,))
                donor = cur.fetchone()
                if not donor:
                    return None

                donor_id, donor_movie_id, has_transcoded, has_raw = donor

                new_status = 'transcode_ready' if has_transcoded else 'video_ready'
                cur.execute("""
                    UPDATE batches
                    SET movie_id = %s,
                        status   = %s
                    WHERE id = %s::uuid AND movie_id IS NULL
                """, (donor_movie_id, new_status, batch_id))

                cur.execute(
                    "UPDATE batches SET movie_id = NULL WHERE id = %s::uuid",
                    (donor_batch_id,),
                )

            conn.commit()
        return str(donor_id)
    except Exception as e:
        print(f"[DB] Ошибка db_steal_video_from_donor: {e}")
        return None


def db_get_donor_count() -> int:
    """Возвращает количество батчей-доноров (отменённые или пробные с видео)."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*)
                    FROM batches b
                    JOIN movies m ON m.id = b.movie_id
                    WHERE (m.transcoded_data IS NOT NULL OR m.raw_data IS NOT NULL)
                      AND (
                        b.status = 'cancelled'
                        OR (b.status = 'probe' AND b.type = 'probe')
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
                        b.type,
                        b.status                                                AS batch_status,
                        b.created_at,
                        b.story_id::text                                        AS story_id,
                        (m.transcoded_data IS NOT NULL
                         OR m.raw_data IS NOT NULL)                             AS has_video_data,
                        tm.name                                                 AS text_model_name,
                        vm.name                                                 AS video_model_name
                    FROM batches b
                    LEFT JOIN movies m ON m.id = b.movie_id
                    LEFT JOIN stories s ON s.id = b.story_id
                    LEFT JOIN ai_models tm ON tm.id = s.model_id
                    LEFT JOIN ai_models vm ON vm.id = m.model_id
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
            'type':             row['type'],
            'scheduled_at':     row['scheduled_at'].isoformat() if row['scheduled_at'] else None,
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


def db_set_batch_published_partially(batch_id):
    """Переводит батч в status='published_partially' и ставит completed_at."""
    _assert_known_status('published_partially')
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


def db_set_batch_publish_error(batch_id):
    """Переводит батч в status='publish_error'."""
    return db_set_batch_status(batch_id, 'publish_error')


def db_set_batch_video_error(batch_id):
    """Переводит батч в status='video_error'."""
    return db_set_batch_status(batch_id, 'video_error')


def db_set_batch_story_error(batch_id):
    """Переводит батч в status='story_error'."""
    return db_set_batch_status(batch_id, 'story_error')


def db_set_batch_story_ready_from_error(batch_id):
    """Откатывает батч в story_ready после сбоя видео, сохраняя story_id.
    НЕ трогает story_id и data — сюжет не регенерируется, данные пайплайна сохраняются."""
    _assert_known_status('story_ready')
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
    """Сбрасывает батч в статус 'pending', очищая story_id и movie_id.
    Используется для перезапуска цикла генерации после фатального сбоя видео.
    Поле data не очищается — сохраняется для диагностики."""
    _assert_known_status('pending')
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
    """Удаляет всю историю: log_entries, log, завершённые батчи. Истории (stories) не удаляет — они являются заделом сюжетов."""
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
    """Обнуляет transcoded_data и raw_data в movies для опубликованных/отменённых батчей
    старше file_lifetime_days. Сама запись батча и movies сохраняется.
    Возвращает количество обновлённых записей movies."""
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


def db_reset_stalled_batches() -> list[dict]:
    """
    Сбрасывает «зависшие» батчи при рестарте приложения:
      - story_generating           → pending
      - video_generating           → story_ready
      - {slug}.{method}.posting    → {slug}.{method}.pending  (динамически по таргетам)

    Возвращает список dict: [{"id": ..., "old_status": ..., "new_status": ...}, ...]
    """
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
                        affected.append({
                            "id": str(row[0]),
                            "old_status": old_status,
                            "new_status": new_status,
                        })

                cur.execute("""
                    SELECT DISTINCT status FROM batches
                    WHERE status LIKE '%.posting'
                """)
                posting_rows = cur.fetchall()
                for (posting_status,) in posting_rows:
                    pending_status = posting_status[:-len('.posting')] + '.pending'
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
                        affected.append({
                            "id": str(row[0]),
                            "old_status": posting_status,
                            "new_status": pending_status,
                        })

            conn.commit()
    except Exception as e:
        print(f"[DB] Ошибка db_reset_stalled_batches: {e}")
    return affected


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
                    SELECT b.id, b.scheduled_at, b.type, b.story_id,
                           m.url AS video_url, b.status, b.data,
                           m.model_id AS video_model_id,
                           b.movie_id
                    FROM batches b
                    LEFT JOIN movies m ON m.id = b.movie_id
                    WHERE b.id = %s
                """, (batch_id,))
                row = cur.fetchone()
        return dict(row) if row else None
    except Exception as e:
        print(f"[DB] Ошибка db_get_batch_by_id: {e}")
        return None


def db_get_actionable_batches():
    """Возвращает список батчей, готовых к обработке (FIFO по created_at).
    Включает батчи с фиксированными статусами пайплайнов story/video/transcode,
    а также батчи со статусами, оканчивающимися на .pending или .published
    (промежуточные составные статусы publish-пайплайна).
    Статусы *.posting не включаются — они являются активными и захвачены воркером."""
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
    # publish pipeline entry point
    'transcode_ready',
    # terminal
    'cancelled', 'error', 'probe', 'story_probe', 'story_error',
    'video_error', 'transcode_error', 'publish_error', 'published',
    'published_partially', 'fatal_error',
})

_COMPOSITE_STATUS_SUFFIXES = ('.posting', '.published', '.pending', '.failed')


def _get_dynamic_publish_statuses() -> set:
    """Читает все таргеты (включая неактивные) и возвращает набор допустимых составных статусов вида
    {slug}.{method}.posting / .published / .pending.
    Неактивные таргеты включаются для корректной валидации статусов батчей, которые могли
    находиться в процессе публикации в момент деактивации таргета."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT slug, config FROM targets WHERE slug IS NOT NULL AND slug != ''"
                )
                rows = cur.fetchall()
        statuses = set()
        for slug, cfg in rows:
            methods = (cfg or {}).get('publish_method', {})
            for method in methods:
                for suffix in _COMPOSITE_STATUS_SUFFIXES:
                    statuses.add(f"{slug}.{method}{suffix}")
        return statuses
    except Exception:
        return set()


def _assert_known_status(status: str) -> None:
    """Проверяет, что статус известен. Для составных статусов ({slug}.{method}.posting/published/pending)
    динамически читает разрешённые значения из таблицы targets."""
    if status in KNOWN_BATCH_STATUSES:
        return
    if any(status.endswith(sfx) for sfx in _COMPOSITE_STATUS_SUFFIXES):
        dynamic = _get_dynamic_publish_statuses()
        if status in dynamic:
            return
    raise ValueError(
        f"[DB] Неизвестный статус '{status}' — добавь его в KNOWN_BATCH_STATUSES или проверь конфиг таргета"
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


def db_claim_batch_status(batch_id: str, from_status: str, to_status: str) -> bool:
    """Атомарно переводит батч из from_status → to_status только если текущий статус равен from_status.
    Возвращает True если переход выполнен (этот процесс захватил батч),
    False если строка не найдена (другой процесс уже изменил статус)."""
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
    """Удаляет батчи со статусом 'published'/'cancelled', а также пробные батчи
    (type = 'probe') завершённые (probe/story_probe), старше batch_lifetime_days.
    Удаляет связанные логи. Истории (stories) не удаляет — они являются заделом сюжетов.
    Возвращает количество удалённых батчей."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id FROM batches
                    WHERE (
                        status IN ('published', 'cancelled')
                        OR (type = 'probe' AND status IN ('probe', 'story_probe'))
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


# ---------------------------------------------------------------------------
# Пользователи
# ---------------------------------------------------------------------------

def db_get_user_by_login(login):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT u.id, u.name, u.login, u.password,
                           r.slug, r.name AS role_name, r.module
                    FROM users u
                    LEFT JOIN user_role_links url ON url.user_id = u.id
                    LEFT JOIN user_roles r        ON r.id = url.role_id
                    WHERE u.login = %s
                    ORDER BY r.slug
                """, (login,))
                rows = cur.fetchall()
                if not rows:
                    return None
                first = rows[0]
                roles = []
                for row in rows:
                    if row[4] is not None:
                        roles.append({
                            "slug": row[4],
                            "name": row[5],
                            "module": row[6] or row[4].upper(),
                        })
                return {
                    "id": str(first[0]),
                    "name": first[1],
                    "login": first[2],
                    "password": first[3],
                    "roles": roles,
                }
    except Exception as e:
        print(f"[DB] Ошибка db_get_user_by_login: {e}")
        return None
