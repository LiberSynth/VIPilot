from .init import get_db


def run_upgrades():
    """Apply incremental schema changes to an existing database.
    Add new migration functions here as the schema evolves.
    Each migration must be idempotent (safe to run multiple times).
    """
    _add_emulation_mode()


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
