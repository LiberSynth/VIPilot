"""
Точка входа инициализации БД.
Последовательность: bootstrap → migrations → seed.

bootstrap() вызывается безусловно при каждом старте и создаёт полную схему
(CREATE TABLE IF NOT EXISTS + ADD COLUMN IF NOT EXISTS), что позволяет пережить
восстановление из старого бэкапа, в котором часть таблиц отсутствует.
"""

from .connection import get_db
from .migrations import run_migrations
from log.log import write_log_entry


def bootstrap():
    """
    Создаёт/дополняет полную схему БД. Idempotent: безопасно вызывать при каждом старте.
    Использует CREATE TABLE IF NOT EXISTS и ADD COLUMN IF NOT EXISTS.
    """
    with get_db() as conn:
        with conn.cursor() as cur:
            # ------------------------------------------------------------------
            # Таблицы
            # ------------------------------------------------------------------
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ai_models (
                    id          UUID NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
                    name        TEXT NOT NULL,
                    platform_id UUID,
                    type        TEXT,
                    active      BOOLEAN NOT NULL DEFAULT FALSE,
                    url         TEXT,
                    body        JSONB NOT NULL DEFAULT '{}',
                    "order"     INTEGER NOT NULL DEFAULT 0,
                    grade       TEXT,
                    note        TEXT,
                    price       TEXT,
                    created_at  TIMESTAMPTZ  NOT NULL DEFAULT clock_timestamp()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ai_platforms (
                    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    name         TEXT NOT NULL,
                    url          TEXT,
                    env_key_name TEXT
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS batches (
                    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    type         TEXT,
                    story_id     UUID,
                    movie_id     UUID,
                    status       TEXT,
                    scheduled_at TIMESTAMPTZ,
                    data         JSONB,
                    title        TEXT,
                    created_at   TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS cycle_config (
                    key   TEXT PRIMARY KEY,
                    value TEXT
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS environment (
                    key   TEXT PRIMARY KEY,
                    value TEXT
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS log (
                    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    batch_id   UUID,
                    pipeline   TEXT,
                    message    TEXT,
                    status     TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS log_entries (
                    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    log_id     UUID,
                    message    TEXT,
                    level      TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS model_durations (
                    id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    model_id UUID NOT NULL,
                    duration INTEGER NOT NULL
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS movies (
                    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    model_id        UUID,
                    url             TEXT,
                    grade           TEXT,
                    created_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS schedule (
                    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    time_utc   TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS settings (
                    key   TEXT PRIMARY KEY,
                    value TEXT
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS stories (
                    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    title          TEXT,
                    content        TEXT,
                    model_id       UUID,
                    grade          TEXT,
                    manual_changed BOOLEAN NOT NULL DEFAULT FALSE,
                    pinned         BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at     TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS targets (
                    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    name            TEXT NOT NULL,
                    aspect_ratio_x  SMALLINT,
                    aspect_ratio_y  SMALLINT,
                    config          JSONB NOT NULL DEFAULT '{}',
                    session_context JSONB,
                    slug            TEXT,
                    active          BOOLEAN NOT NULL DEFAULT TRUE,
                    transcode       BOOLEAN NOT NULL DEFAULT FALSE,
                    "order"         INTEGER NOT NULL DEFAULT 0
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS user_role_links (
                    id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    user_id UUID NOT NULL,
                    role_id UUID NOT NULL
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
                CREATE TABLE IF NOT EXISTS users (
                    id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    name     TEXT,
                    login    TEXT,
                    password TEXT
                )
            """)

            # ------------------------------------------------------------------
            # Legacy cleanup
            # ------------------------------------------------------------------
            cur.execute("DELETE FROM cycle_config WHERE key = 'approve_stories'")

            # ------------------------------------------------------------------
            # Индексы
            # ------------------------------------------------------------------
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_ai_models_order
                    ON ai_models ("order")
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_ai_models_active
                    ON ai_models (active)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_ai_models_platform_id
                    ON ai_models (platform_id)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_ai_models_type
                    ON ai_models (type)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_ai_models_grade
                    ON ai_models (grade)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_ai_models_created_at
                    ON ai_models (created_at)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_batches_scheduled_at
                    ON batches (scheduled_at)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_batches_type
                    ON batches (type)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_batches_status
                    ON batches (status)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_batches_story_id
                    ON batches (story_id)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_batches_movie_id
                    ON batches (movie_id)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_batches_created_at
                    ON batches (created_at)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_batches_title
                    ON batches (title)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_log_batch_id
                    ON log (batch_id)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_log_pipeline
                    ON log (pipeline)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_log_status
                    ON log (status)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_log_created_at
                    ON log (created_at)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_log_entries_log_id
                    ON log_entries (log_id)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_log_entries_level
                    ON log_entries (level)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_log_entries_created_at
                    ON log_entries (created_at)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_model_durations_model_id
                    ON model_durations (model_id)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_model_durations_duration
                    ON model_durations (duration)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_movies_model_id
                    ON movies (model_id)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_movies_grade
                    ON movies (grade)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_movies_created_at
                    ON movies (created_at)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_schedule_time_utc
                    ON schedule (time_utc)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_schedule_created_at
                    ON schedule (created_at)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_stories_title
                    ON stories (title)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_stories_model_id
                    ON stories (model_id)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_stories_grade
                    ON stories (grade)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_stories_manual_changed
                    ON stories (manual_changed)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_stories_pinned
                    ON stories (pinned)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_stories_created_at
                    ON stories (created_at)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_targets_slug
                    ON targets (slug)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_targets_active
                    ON targets (active)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_targets_order
                    ON targets ("order")
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_user_role_links_user_id
                    ON user_role_links (user_id)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_user_role_links_role_id
                    ON user_role_links (role_id)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_user_roles_slug
                    ON user_roles (slug)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_users_login
                    ON users (login)
            """)

            # ------------------------------------------------------------------
            # Хранимые функции
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
                    WHERE b.movie_id IS NOT NULL
                      AND b.status IN ('cancelled', 'movie_manual')
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
                    WHERE b.movie_id IS NOT NULL
                      AND b.status IN ('cancelled', 'movie_manual')
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
    write_log_entry(None, "[DB] bootstrap: схема проверена/создана", level="silent")


def init_db():
    try:
        bootstrap()
        run_migrations()
    except Exception as e:
        write_log_entry(None, f"[DB] Ошибка инициализации: {e}", level="silent")
        raise
