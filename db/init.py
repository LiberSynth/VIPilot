"""
Точка входа инициализации БД.
Последовательность: bootstrap → migrations → seed.

bootstrap() вызывается безусловно при каждом старте и создаёт полную схему
(CREATE TABLE IF NOT EXISTS + ADD COLUMN IF NOT EXISTS), что позволяет пережить
восстановление из старого бэкапа, в котором часть таблиц отсутствует.
"""

from .connection import get_db
from .migrations import run_migrations
from .seed import seed_db
from log.log import write_log_entry


def bootstrap():
    """
    Создаёт/дополняет полную схему БД. Idempotent: безопасно вызывать при каждом старте.
    Использует CREATE TABLE IF NOT EXISTS и ADD COLUMN IF NOT EXISTS.
    """
    with get_db() as conn:
        with conn.cursor() as cur:
            # ------------------------------------------------------------------
            # Базовые служебные таблицы
            # ------------------------------------------------------------------
            cur.execute("""
                CREATE TABLE IF NOT EXISTS environment (
                    key   VARCHAR(100) PRIMARY KEY,
                    value TEXT NOT NULL
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS settings (
                    key   VARCHAR(100) PRIMARY KEY,
                    value TEXT NOT NULL
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS cycle_config (
                    key   VARCHAR(100) PRIMARY KEY,
                    value TEXT NOT NULL
                )
            """)

            # ------------------------------------------------------------------
            # Расписание
            # ------------------------------------------------------------------
            cur.execute("""
                CREATE TABLE IF NOT EXISTS schedule (
                    id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
                    time_utc   VARCHAR(5)  NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)
            cur.execute("""
                ALTER TABLE schedule
                    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            """)

            # ------------------------------------------------------------------
            # ИИ-платформы и модели
            # ------------------------------------------------------------------
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ai_platforms (
                    id           UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
                    name         VARCHAR(200) NOT NULL,
                    url          VARCHAR(500) NOT NULL,
                    env_key_name VARCHAR(200)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ai_models (
                    id          UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
                    name        VARCHAR(200) NOT NULL,
                    url         VARCHAR(200) NOT NULL,
                    body        JSONB        NOT NULL DEFAULT '{}',
                    "order"     INTEGER      NOT NULL DEFAULT 0,
                    active      BOOLEAN      NOT NULL DEFAULT FALSE,
                    platform_id UUID,
                    type        VARCHAR(50)  NOT NULL,
                    grade       VARCHAR(20),
                    note        TEXT,
                    price       TEXT
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS model_durations (
                    model_id UUID    NOT NULL,
                    duration INTEGER NOT NULL,
                    PRIMARY KEY (model_id, duration)
                )
            """)

            # ------------------------------------------------------------------
            # Таргеты (публикационные платформы)
            # ------------------------------------------------------------------
            cur.execute("""
                CREATE TABLE IF NOT EXISTS targets (
                    id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
                    name            VARCHAR(200) NOT NULL,
                    aspect_ratio_x  SMALLINT     NOT NULL DEFAULT 9,
                    aspect_ratio_y  SMALLINT     NOT NULL DEFAULT 16,
                    transcode       BOOLEAN      NOT NULL DEFAULT FALSE,
                    config          JSONB        NOT NULL DEFAULT '{}',
                    slug            VARCHAR(50),
                    active          BOOLEAN      NOT NULL DEFAULT TRUE,
                    "order"         INTEGER      NOT NULL DEFAULT 0,
                    session_context JSONB
                )
            """)

            # ------------------------------------------------------------------
            # Сюжеты
            # ------------------------------------------------------------------
            cur.execute("""
                CREATE TABLE IF NOT EXISTS stories (
                    id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
                    model_id       UUID,
                    title          TEXT,
                    content        TEXT,
                    grade          TEXT,
                    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
                    manual_changed BOOLEAN     NOT NULL DEFAULT FALSE,
                    pinned         BOOLEAN     NOT NULL DEFAULT FALSE
                )
            """)

            # ------------------------------------------------------------------
            # Видео
            # ------------------------------------------------------------------
            cur.execute("""
                CREATE TABLE IF NOT EXISTS movies (
                    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
                    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
                    url             TEXT,
                    raw_data        BYTEA,
                    transcoded_data BYTEA,
                    model_id        UUID,
                    grade           TEXT
                )
            """)

            # ------------------------------------------------------------------
            # Батчи (задачи пайплайна)
            # ------------------------------------------------------------------
            cur.execute("""
                CREATE TABLE IF NOT EXISTS batches (
                    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
                    scheduled_at TIMESTAMPTZ,
                    type         VARCHAR(30) NOT NULL DEFAULT 'slot',
                    status       VARCHAR(50) NOT NULL DEFAULT 'pending',
                    story_id     UUID,
                    movie_id     UUID,
                    data         JSONB,
                    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
                    title        TEXT
                )
            """)

            # ------------------------------------------------------------------
            # Логи
            # ------------------------------------------------------------------
            cur.execute("""
                CREATE TABLE IF NOT EXISTS log (
                    id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
                    batch_id   UUID,
                    pipeline   VARCHAR(30) NOT NULL,
                    message    TEXT,
                    status     VARCHAR(20),
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS log_entries (
                    id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
                    log_id     UUID,
                    message    TEXT        NOT NULL,
                    level      VARCHAR(10) NOT NULL DEFAULT 'info',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)

            # ------------------------------------------------------------------
            # Пользователи и роли
            # ------------------------------------------------------------------
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    name     TEXT,
                    login    TEXT,
                    password TEXT
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS user_roles (
                    id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    name   TEXT,
                    slug   TEXT,
                    module TEXT
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS user_role_links (
                    user_id UUID NOT NULL,
                    role_id UUID NOT NULL,
                    PRIMARY KEY (user_id, role_id)
                )
            """)

            # ------------------------------------------------------------------
            # Индексы (идемпотентны)
            # ------------------------------------------------------------------
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_batches_status
                    ON batches (status)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_batches_created_at
                    ON batches (created_at)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_log_batch_id
                    ON log (batch_id)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_log_entries_log_id
                    ON log_entries (log_id)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_log_entries_created_at
                    ON log_entries (created_at)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_stories_created_at
                    ON stories (created_at)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_movies_created_at
                    ON movies (created_at)
            """)

            # ------------------------------------------------------------------
            # Хранимые функции для пула доноров
            # ------------------------------------------------------------------
            cur.execute("""
                CREATE OR REPLACE FUNCTION get_donor_count(p_good_only boolean)
                RETURNS bigint LANGUAGE plpgsql AS $$
                DECLARE
                    v_count bigint;
                BEGIN
                    PERFORM pg_advisory_xact_lock(hashtext('donor_claim'));
                    SELECT COUNT(*) INTO v_count
                    FROM batches b
                    JOIN movies m ON m.id = b.movie_id
                    WHERE (m.transcoded_data IS NOT NULL OR m.raw_data IS NOT NULL)
                      AND b.status IN ('cancelled', 'movie_probe')
                      AND (NOT p_good_only OR m.grade = 'good');
                    RETURN v_count;
                END;
                $$
            """)
            cur.execute("""
                CREATE OR REPLACE FUNCTION claim_donor_batch(p_batch_id uuid, p_good_only boolean)
                RETURNS void LANGUAGE plpgsql AS $$
                DECLARE
                    v_donor_id uuid;
                    v_donated_status text := 'donated';
                BEGIN
                    PERFORM pg_advisory_xact_lock(hashtext('donor_claim'));
                    SELECT b.id INTO v_donor_id
                    FROM batches b
                    JOIN movies m ON m.id = b.movie_id
                    WHERE (m.transcoded_data IS NOT NULL OR m.raw_data IS NOT NULL)
                      AND b.status IN ('cancelled', 'movie_probe')
                      AND (NOT p_good_only OR m.grade = 'good')
                    ORDER BY b.created_at ASC
                    LIMIT 1 FOR UPDATE OF b;
                    IF v_donor_id IS NULL THEN RETURN; END IF;
                    UPDATE batches SET status = v_donated_status WHERE id = v_donor_id;
                    UPDATE batches
                    SET data = COALESCE(data, '{}'::jsonb)
                            || jsonb_build_object('donor_batch_id', v_donor_id::text)
                    WHERE id = p_batch_id;
                END;
                $$
            """)

        conn.commit()
    write_log_entry(None, '[DB] bootstrap: схема проверена/создана', level='silent')


def init_db():
    try:
        bootstrap()
        run_migrations()
        seed_db()
    except Exception as e:
        write_log_entry(None, f"[DB] Ошибка инициализации: {e}", level='silent')
        raise
