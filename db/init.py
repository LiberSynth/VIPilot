"""
Точка входа инициализации БД.
Последовательность: bootstrap → migrations → seed.
"""

from .connection import get_db
from .migrations import run_migrations
from .seed import seed_db
from log.log import write_log_entry


def bootstrap():
    """
    Создаёт environment и settings — минимум, нужный для запуска миграций.
    Idempotent: безопасно вызывать при каждом старте.
    """
    with get_db() as conn:
        with conn.cursor() as cur:
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
        conn.commit()


def init_db():
    try:
        bootstrap()
        run_migrations()
        seed_db()
    except Exception as e:
        write_log_entry(None, f"[DB] Ошибка инициализации: {e}", level='silent')
        raise
