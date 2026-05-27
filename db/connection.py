import os
import contextlib
from psycopg2 import pool as pg_pool

_pool: pg_pool.ThreadedConnectionPool | None = None


def _get_pool() -> pg_pool.ThreadedConnectionPool:
    global _pool
    if _pool is None or _pool.closed:
        _pool = pg_pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=10,
            dsn=os.environ["DATABASE_URL"],
            connect_timeout=10,
        )
    return _pool


@contextlib.contextmanager
def get_db():
    p = _get_pool()
    conn = p.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        p.putconn(conn)


def close_pool() -> None:
    """Закрывает все соединения пула. Следующий get_db() создаст новый пул.

    Нужно перед операциями, которые ломают/пересоздают объекты схемы
    (например, восстановление БД из бэкапа): idle-коннекты в пуле могут
    блокировать DROP SCHEMA.
    """
    global _pool
    if _pool is not None and not _pool.closed:
        try:
            _pool.closeall()
        except Exception:
            pass
    _pool = None
