from .init import get_db


def run_upgrades():
    """Apply incremental schema changes to an existing database.
    Add new migration functions here as the schema evolves.
    Each migration must be idempotent (safe to run multiple times).
    """
    _create_environment_table()
    _add_emulation_mode()
    _add_buffer_hours()
    _create_ai_models()
    _create_targets()
    _create_stories()
    _create_batches()
    _create_log()
    _create_log_entries()
    _add_loop_interval()
    _log_batch_id_nullable()
    _batches_unique_constraint()
    _rename_lifetime_settings()
    _add_video_data_column()
    _recover_story_generating()
    _fix_log_lifetime_keys()


def _add_emulation_mode():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO settings (key, value) VALUES ('emulation_mode', '0')
                    ON CONFLICT (key) DO NOTHING
                """)
                cur.execute("""
                    INSERT INTO settings (key, value) VALUES ('history_days', '7')
                    ON CONFLICT (key) DO NOTHING
                """)
                cur.execute("""
                    INSERT INTO settings (key, value) VALUES ('short_log_days', '365')
                    ON CONFLICT (key) DO NOTHING
                """)
            conn.commit()
    except Exception as e:
        print(f"[DB] Ошибка миграции _add_emulation_mode: {e}")


def _add_buffer_hours():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO settings (key, value) VALUES ('buffer_hours', '24')
                    ON CONFLICT (key) DO NOTHING
                """)
            conn.commit()
    except Exception as e:
        print(f"[DB] Ошибка миграции _add_buffer_hours: {e}")


def _create_ai_models():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS ai_models (
                        id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        name           VARCHAR(200) NOT NULL,
                        url            VARCHAR(200) NOT NULL,
                        body           JSONB NOT NULL DEFAULT '{}',
                        "order"        INTEGER NOT NULL DEFAULT 0,
                        active         BOOLEAN NOT NULL DEFAULT FALSE,
                        ai_platform_id UUID REFERENCES ai_platforms(id),
                        platform_id    UUID REFERENCES ai_platforms(id),
                        type           VARCHAR(50) NOT NULL,
                        time_point     TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                """)
            conn.commit()
    except Exception as e:
        print(f"[DB] Ошибка миграции _create_ai_models: {e}")


def _create_targets():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS targets (
                        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        name VARCHAR(200) NOT NULL,
                        aspect_ratio_x SMALLINT NOT NULL DEFAULT 9,
                        aspect_ratio_y SMALLINT NOT NULL DEFAULT 16,
                        active BOOLEAN NOT NULL DEFAULT TRUE
                    )
                """)
            conn.commit()
    except Exception as e:
        print(f"[DB] Ошибка миграции _create_targets: {e}")


def _create_stories():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS stories (
                        id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        time_point TIMESTAMPTZ NOT NULL DEFAULT now(),
                        result     TEXT NOT NULL,
                        model_id   UUID REFERENCES ai_models(id)
                    )
                """)
            conn.commit()
    except Exception as e:
        print(f"[DB] Ошибка миграции _create_stories: {e}")


def _create_batches():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS batches (
                        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        scheduled_at TIMESTAMPTZ NOT NULL,
                        target_id UUID NOT NULL REFERENCES targets(id),
                        status VARCHAR(30) NOT NULL DEFAULT 'pending',
                        story_id UUID REFERENCES stories(id),
                        video_url TEXT,
                        video_file TEXT,
                        data JSONB,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        completed_at TIMESTAMPTZ
                    )
                """)
            conn.commit()
    except Exception as e:
        print(f"[DB] Ошибка миграции _create_batches: {e}")


def _create_log():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS log (
                        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        batch_id UUID NOT NULL REFERENCES batches(id),
                        pipeline VARCHAR(30) NOT NULL,
                        message TEXT,
                        status VARCHAR(20),
                        time_point TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                """)
            conn.commit()
    except Exception as e:
        print(f"[DB] Ошибка миграции _create_log: {e}")


def _create_log_entries():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS log_entries (
                        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        log_id UUID NOT NULL REFERENCES log(id),
                        message TEXT NOT NULL,
                        level VARCHAR(10) NOT NULL DEFAULT 'info',
                        time_point TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                """)
            conn.commit()
    except Exception as e:
        print(f"[DB] Ошибка миграции _create_log_entries: {e}")


def _add_loop_interval():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO settings (key, value) VALUES ('loop_interval', '5')
                    ON CONFLICT (key) DO NOTHING
                """)
            conn.commit()
    except Exception as e:
        print(f"[DB] Ошибка миграции _add_loop_interval: {e}")


def _log_batch_id_nullable():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT is_nullable FROM information_schema.columns
                    WHERE table_name = 'log' AND column_name = 'batch_id'
                """)
                row = cur.fetchone()
                if row and row[0] == 'NO':
                    cur.execute("ALTER TABLE log ALTER COLUMN batch_id DROP NOT NULL")
            conn.commit()
    except Exception as e:
        print(f"[DB] Ошибка миграции _log_batch_id_nullable: {e}")


def _batches_unique_constraint():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM pg_constraint
                        WHERE conname = 'batches_scheduled_at_target_id_key'
                    )
                """)
                if not cur.fetchone()[0]:
                    cur.execute("""
                        ALTER TABLE batches
                        ADD CONSTRAINT batches_scheduled_at_target_id_key
                        UNIQUE (scheduled_at, target_id)
                    """)
            conn.commit()
    except Exception as e:
        print(f"[DB] Ошибка миграции _batches_unique_constraint: {e}")


def _rename_lifetime_settings():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE settings SET key = 'batch_lifetime'
                    WHERE key = 'history_days'
                      AND NOT EXISTS (SELECT 1 FROM settings WHERE key = 'batch_lifetime')
                """)
                cur.execute("""
                    UPDATE settings SET key = 'short_log_lifetime'
                    WHERE key = 'short_log_days'
                      AND NOT EXISTS (SELECT 1 FROM settings WHERE key = 'short_log_lifetime')
                """)
                cur.execute("""
                    INSERT INTO settings (key, value) VALUES ('log_lifetime', '30')
                    ON CONFLICT (key) DO NOTHING
                """)
                cur.execute("""
                    INSERT INTO settings (key, value) VALUES ('file_lifetime', '7')
                    ON CONFLICT (key) DO NOTHING
                """)
            conn.commit()
    except Exception as e:
        print(f"[DB] Ошибка миграции _rename_lifetime_settings: {e}")


def _add_video_data_column():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_name = 'batches' AND column_name = 'video_data'
                """)
                if not cur.fetchone():
                    cur.execute("ALTER TABLE batches ADD COLUMN video_data BYTEA")
            conn.commit()
    except Exception as e:
        print(f"[DB] Ошибка миграции _add_video_data_column: {e}")


def _fix_log_lifetime_keys():
    """Переименовывает log_lifetime→entries_lifetime, short_log_lifetime→log_lifetime,
    удаляет устаревший short_log_days."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                # Шаг 1: log_lifetime (подробные записи) → entries_lifetime
                cur.execute("""
                    UPDATE settings SET key = 'entries_lifetime'
                    WHERE key = 'log_lifetime'
                      AND NOT EXISTS (SELECT 1 FROM settings WHERE key = 'entries_lifetime')
                """)
                # Шаг 2: short_log_lifetime (краткие заголовки) → log_lifetime
                # (log_lifetime уже освободили на шаге 1)
                cur.execute("""
                    UPDATE settings SET key = 'log_lifetime'
                    WHERE key = 'short_log_lifetime'
                      AND NOT EXISTS (SELECT 1 FROM settings WHERE key = 'log_lifetime')
                """)
                # Шаг 3: вставить defaults если ключи не появились
                cur.execute("""
                    INSERT INTO settings (key, value) VALUES ('entries_lifetime', '30')
                    ON CONFLICT (key) DO NOTHING
                """)
                cur.execute("""
                    INSERT INTO settings (key, value) VALUES ('log_lifetime', '365')
                    ON CONFLICT (key) DO NOTHING
                """)
                # Шаг 4: удалить устаревший ключ-призрак
                cur.execute("DELETE FROM settings WHERE key = 'short_log_days'")
                cur.execute("DELETE FROM settings WHERE key = 'short_log_lifetime'")
            conn.commit()
    except Exception as e:
        print(f"[DB] Ошибка миграции _fix_log_lifetime_keys: {e}")


def _recover_story_generating():
    """Сбрасывает застрявшие story_generating-батчи в pending при старте приложения."""
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
    except Exception as e:
        print(f"[DB] Ошибка миграции _recover_story_generating: {e}")


def _create_environment_table():
    """Создаёт таблицу environment и добавляет начальные записи."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS environment (
                        key VARCHAR(100) PRIMARY KEY,
                        value TEXT NOT NULL
                    )
                """)
                cur.execute("""
                    INSERT INTO environment (key, value) VALUES ('workflow_state', 'running')
                    ON CONFLICT (key) DO NOTHING
                """)
            conn.commit()
    except Exception as e:
        print(f"[DB] Ошибка миграции _create_environment_table: {e}")
