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

def _consolidate_log_entries_channel(cur) -> None:
    """Одна колонка channel: переименование category или удаление дубля после ошибочного ADD."""
    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'log_entries'
          AND column_name IN ('category', 'channel')
    """)
    le_cols = {r[0] for r in cur.fetchall()}
    if 'category' in le_cols and 'channel' in le_cols:
        cur.execute("""
            UPDATE log_entries
            SET channel = category
            WHERE category IS NOT NULL
        """)
        cur.execute("ALTER TABLE log_entries DROP COLUMN category")
    elif 'category' in le_cols:
        cur.execute("ALTER TABLE log_entries RENAME COLUMN category TO channel")
    elif 'channel' not in le_cols:
        cur.execute("""
            ALTER TABLE log_entries
            ADD COLUMN channel TEXT NOT NULL DEFAULT 'system'
        """)
    cur.execute("DROP INDEX IF EXISTS idx_log_entries_category")

def _ensure_log_indexes(cur) -> None:
    """Индексы log / log_entries по фактической схеме."""
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_log_batch_id_unique
            ON log (batch_id)
        WHERE batch_id IS NOT NULL
    """)
    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'log_entries'
          AND column_name = 'channel'
    """)
    if cur.fetchone():
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_log_entries_channel
                ON log_entries (channel)
        """)

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
                    batch_id_source UUID,
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
                    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS log_entries (
                    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    log_id     UUID,
                    channel    TEXT NOT NULL DEFAULT 'system',
                    message    TEXT,
                    level      TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
                )
            """)
            _consolidate_log_entries_channel(cur)
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
                    story_id        UUID,
                    model_id        UUID,
                    url             TEXT,
                    grade           TEXT,
                    used            BIT(1) NOT NULL DEFAULT B'0',
                    transcoded      BIT(1) NOT NULL DEFAULT B'0',
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
                    config          JSONB NOT NULL DEFAULT '{}',
                    session_context JSONB,
                    slug            TEXT,
                    active          BOOLEAN NOT NULL DEFAULT TRUE,
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
            # Additive migrations (idempotent, no FK)
            # ------------------------------------------------------------------
            cur.execute("""
                ALTER TABLE batches
                ADD COLUMN IF NOT EXISTS batch_id_source UUID
            """)
            cur.execute("""
                ALTER TABLE movies
                ADD COLUMN IF NOT EXISTS story_id UUID
            """)
            cur.execute("""
                ALTER TABLE movies
                ADD COLUMN IF NOT EXISTS used BIT(1)
            """)
            cur.execute("""
                UPDATE movies m
                SET used = CASE
                    WHEN EXISTS (
                        SELECT 1
                        FROM batches b2
                        WHERE b2.movie_id = m.id
                          AND (
                              b2.status IN ('published', 'published_partially')
                              OR b2.status LIKE '%.published'
                          )
                    )
                    THEN B'1'
                    ELSE B'0'
                END
                WHERE m.used IS NULL
            """)
            cur.execute("""
                ALTER TABLE movies
                ALTER COLUMN used SET DEFAULT B'0'
            """)
            cur.execute("""
                ALTER TABLE movies
                ALTER COLUMN used SET NOT NULL
            """)

            # ------------------------------------------------------------------
            # Legacy cleanup
            # ------------------------------------------------------------------
            cur.execute("DELETE FROM cycle_config WHERE key = 'approve_stories'")
            cur.execute("DELETE FROM cycle_config WHERE key = 'approve_movies'")
            cur.execute("DELETE FROM environment WHERE key = 'use_donor'")
            cur.execute("DROP FUNCTION IF EXISTS get_donor_count(boolean)")
            cur.execute("DROP FUNCTION IF EXISTS claim_donor_batch(uuid, boolean)")

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
                CREATE INDEX IF NOT EXISTS idx_batches_batch_id_source
                    ON batches (batch_id_source)
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
            _ensure_log_indexes(cur)
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
                CREATE INDEX IF NOT EXISTS idx_movies_story_id
                    ON movies (story_id)
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
                CREATE INDEX IF NOT EXISTS idx_movies_used
                    ON movies (used)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_movies_pool_pick
                    ON movies (used, grade, created_at, id)
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

        conn.commit()
    write_log_entry(None, 'db', 'bootstrap: схема проверена/создана', level='silent')

def init_db():
    try:
        bootstrap()
        run_migrations()
    except Exception as e:
        write_log_entry(None, 'db', f'Ошибка инициализации: {e}', level='silent')
        raise
