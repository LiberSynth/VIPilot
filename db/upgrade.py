from .init import get_db


def run_upgrades():
    """Apply incremental schema changes to an existing database.
    Add new migration functions here as the schema evolves.
    Each migration must be idempotent (safe to run multiple times).
    """
    _add_emulation_mode()
    _rename_publish_times_to_schedule()
    _add_buffer_hours()
    _create_targets()
    _create_stories()
    _create_batches()
    _create_log()
    _create_log_entries()
    _add_loop_interval()
    _log_batch_id_nullable()
    _batches_unique_constraint()
    _batches_add_fal_fields()


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


def _rename_publish_times_to_schedule():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_name = 'publish_times'
                    )
                """)
                if cur.fetchone()[0]:
                    cur.execute("ALTER TABLE publish_times RENAME TO schedule")
            conn.commit()
    except Exception as e:
        print(f"[DB] Ошибка миграции _rename_publish_times_to_schedule: {e}")


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
                        model_id   UUID REFERENCES models(id)
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


def _batches_add_fal_fields():
    """Добавляет поля для хранения состояния fal.ai-запроса (рестарт-устойчивость)."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    ALTER TABLE batches
                        ADD COLUMN IF NOT EXISTS fal_request_id  VARCHAR(200),
                        ADD COLUMN IF NOT EXISTS fal_status_url  TEXT,
                        ADD COLUMN IF NOT EXISTS fal_response_url TEXT
                """)
            conn.commit()
    except Exception as e:
        print(f"[DB] Ошибка миграции _batches_add_fal_fields: {e}")
