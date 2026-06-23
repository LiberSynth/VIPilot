import os
import platform
import shutil
import subprocess
from urllib.parse import urlparse, unquote, parse_qsl

from .connection import get_db
from .db_pipeline import build_connected_batch_components, build_pipeline_chain_map
from .db_media import db_delete_movie_video_files
from common.statuses import FINAL_BATCH_STATUSES, batch_is_active
from log.log import write_log_entry
from utils.utils import fmt_id_msg

def db_get_log_entries(log_id):
    """Возвращает список субзаписей для указанного log_id."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT message, level, created_at, channel
                FROM log_entries WHERE log_id = %s
                ORDER BY created_at DESC, id DESC
                """,
                (log_id,),
            )
            rows = cur.fetchall()
    return [
        {
            "msg": r[0],
            "level": r[1],
            "ts": r[2].isoformat() if r[2] else None,
            "channel": r[3],
        }
        for r in rows
    ]

def db_get_monitor():
    """
    Лёгкий список для монитора без log_entries и без join log/stories/movies.
    batches — поля batches; system — log-окна (batch_id IS NULL).
    """
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    b.id,
                    b.scheduled_at,
                    b.type,
                    b.status,
                    b.created_at,
                    b.story_id,
                    b.movie_id,
                    b.title,
                    b.batch_id_source,
                    (
                        SELECT l.id::text
                        FROM log l
                        WHERE l.batch_id = b.id
                        ORDER BY l.created_at ASC
                        LIMIT 1
                    ) AS log_id,
                    (
                        SELECT COUNT(*)::int
                        FROM log_entries le
                        JOIN log l ON l.id = le.log_id
                        WHERE l.batch_id = b.id
                    ) AS entry_count
                FROM batches b
                ORDER BY b.created_at DESC, b.id DESC
                """
            )
            batch_rows = cur.fetchall()

            cur.execute(
                """
                SELECT
                    l.id::text,
                    l.created_at,
                    (SELECT COUNT(*)::int FROM log_entries le WHERE le.log_id = l.id) AS entry_count
                FROM log l
                WHERE l.batch_id IS NULL
                ORDER BY l.created_at DESC, l.id DESC
                """
            )
            sys_rows = cur.fetchall()

    batches = [
        {
            "batch_id":       str(r[0]),
            "scheduled_at":   r[1].isoformat() if r[1] else None,
            "type":           r[2],
            "batch_status":   r[3],
            "created_at":     r[4].isoformat() if r[4] else None,
            "story_id":       str(r[5]) if r[5] else None,
            "movie_id":       str(r[6]) if r[6] else None,
            "title":          r[7],
            "batch_id_source": str(r[8]) if r[8] else None,
            "log_id":         r[9] if r[9] else None,
            "entry_count":    int(r[10] or 0),
        }
        for r in batch_rows
    ]
    chain_map = build_pipeline_chain_map(batches)
    for batch in batches:
        batch["pipeline_chain_ids"] = chain_map.get(batch["batch_id"], [])
    system = [
        {
            "id":           r[0],
            "created_at":   r[1].isoformat() if r[1] else None,
            "entry_count":  int(r[2] or 0),
        }
        for r in sys_rows
    ]
    return {"batches": batches, "system": system}

def db_get_system_log_entries(log_id: str) -> list:
    """Возвращает log_entries для log_id (батч или приложение)."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT le.message, le.level, le.created_at, le.channel
                FROM log_entries le
                WHERE le.log_id = %s
                ORDER BY le.created_at DESC, le.id DESC
                """,
                (log_id,),
            )
            rows = cur.fetchall()
    return [
        {
            "message":    r[0],
            "level":      r[1],
            "created_at": r[2].isoformat() if r[2] else None,
            "channel":   r[3],
        }
        for r in rows
    ]

def db_get_batch_log_entries(batch_id: str) -> list:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT le.message, le.level, le.created_at, le.channel
                FROM log_entries le
                JOIN log l ON l.id = le.log_id
                WHERE l.batch_id = %s::uuid
                ORDER BY le.created_at DESC, le.id DESC
                """,
                (batch_id,),
            )
            rows = cur.fetchall()
    return [
        {
            "message":    r[0],
            "level":      r[1],
            "created_at": r[2].isoformat() if r[2] else None,
            "channel":   r[3],
        }
        for r in rows
    ]

def db_cleanup_log_entries(log_lifetime_days: int) -> int:
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

def db_cleanup_logs(log_lifetime_days: int) -> int:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM log_entries
                WHERE log_id IN (
                    SELECT id FROM log
                    WHERE created_at < now() - make_interval(days => %s)
                )
            """, (log_lifetime_days,))
            cur.execute("""
                DELETE FROM log
                WHERE created_at < now() - make_interval(days => %s)
            """, (log_lifetime_days,))
            count = cur.rowcount
        conn.commit()
    return count

_LIBPQ_QUERY_ENV = {
    'sslmode': 'PGSSLMODE',
    'sslrootcert': 'PGSSLROOTCERT',
    'sslcert': 'PGSSLCERT',
    'sslkey': 'PGSSLKEY',
    'sslpassword': 'PGSSLPASSWORD',
    'options': 'PGOPTIONS',
    'application_name': 'PGAPPNAME',
    'connect_timeout': 'PGCONNECT_TIMEOUT',
    'target_session_attrs': 'PGTARGETSESSIONATTRS',
    'gssencmode': 'PGGSSENCMODE',
    'channel_binding': 'PGCHANNELBINDING',
}

def _parse_db_url() -> dict:
    """Парсит DATABASE_URL в параметры подключения для pg_repack CLI.

    Возвращает host/port/user/password/dbname плюс `extra_env` —
    libpq-переменные (PGSSLMODE, PGOPTIONS и т.п.), извлечённые из
    query-строки DSN. Это нужно, чтобы pg_repack подключался ровно с
    теми же опциями, что и приложение (например, когда нужен
    sslmode=require).
    """
    dsn = os.environ.get('DATABASE_URL')
    if not dsn:
        raise RuntimeError('DATABASE_URL не задан')
    p = urlparse(dsn)
    extra_env: dict = {}
    for key, val in parse_qsl(p.query, keep_blank_values=False):
        env_key = _LIBPQ_QUERY_ENV.get(key.lower())
        if env_key:
            extra_env[env_key] = val
    return {
        'host': p.hostname or 'localhost',
        'port': str(p.port or 5432),
        'user': unquote(p.username) if p.username else '',
        'password': unquote(p.password) if p.password else '',
        'dbname': (p.path or '/').lstrip('/') or '',
        'extra_env': extra_env,
    }

def _public_tables() -> list[tuple[str, str]]:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT schemaname, tablename
                FROM pg_tables
                WHERE schemaname = 'public'
                ORDER BY tablename
            """)
            return [(r[0], r[1]) for r in cur.fetchall()]

def _ensure_pg_repack_extension() -> None:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute('CREATE EXTENSION IF NOT EXISTS pg_repack')
        conn.commit()

def _db_vacuum_full_fallback(tables: list[tuple[str, str]]) -> dict:
    """Fallback на Windows: VACUUM FULL (блокирует таблицу на время операции)."""
    import psycopg2
    from psycopg2 import sql

    dsn = os.environ.get('DATABASE_URL')
    if not dsn:
        raise RuntimeError('DATABASE_URL не задан')

    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    results = []
    freed_total = 0
    ok_count = 0
    failed_count = 0
    try:
        with conn.cursor() as cur:
            for schema, table in tables:
                full = f'{schema}.{table}'
                size_before = _table_size(full)
                try:
                    cur.execute(
                        sql.SQL('VACUUM (FULL, ANALYZE) {}.{}').format(
                            sql.Identifier(schema),
                            sql.Identifier(table),
                        )
                    )
                except Exception as e:
                    results.append({
                        'table': full, 'ok': False,
                        'freed_bytes': 0, 'error': f'{type(e).__name__}: {e}'[:500],
                    })
                    failed_count += 1
                    continue

                size_after = _table_size(full)
                freed = max(0, (size_before or 0) - (size_after or 0))
                freed_total += freed
                ok_count += 1
                results.append({
                    'table': full, 'ok': True,
                    'freed_bytes': freed, 'error': None,
                })
    finally:
        conn.close()

    return {
        'mode': 'vacuum_full_fallback',
        'tables_total': len(tables),
        'tables_ok': ok_count,
        'tables_failed': failed_count,
        'freed_bytes': freed_total,
        'results': results,
    }

def db_vacuum_full() -> dict:
    """Сжимает все таблицы схемы public через pg_repack (онлайн, без блокировки).

    По каждой таблице запускает отдельный процесс `pg_repack -t schema.table`,
    собирает размер до/после из `pg_total_relation_size`. Падение по одной
    таблице не валит весь прогон. На Windows при недоступном pg_repack — VACUUM FULL.
    Возвращает сводку:
    `{tables_total, tables_ok, tables_failed, freed_bytes, results: [...]}`.
    """
    from utils.runtime_bootstrap import ensure_pg_repack_in_path, get_pg_repack_bootstrap_error

    tables = _public_tables()
    # На Windows официального pg_repack нет — только поиск уже установленного CLI.
    ensure_pg_repack_in_path(auto_install=(platform.system() != 'Windows'))
    cli = shutil.which('pg_repack')
    if not cli:
        bootstrap_error = get_pg_repack_bootstrap_error()
        if platform.system() == 'Windows':
            write_log_entry(
                None,
                "db",
                '[DB] pg_repack не найден, выполняю fallback VACUUM FULL: '
                + (bootstrap_error or 'причина неизвестна'),
                level='warn',
            )
            return _db_vacuum_full_fallback(tables)
        details = f': {bootstrap_error}' if bootstrap_error else ''
        raise RuntimeError(f'pg_repack CLI не найден в окружении{details}')

    try:
        _ensure_pg_repack_extension()
    except Exception as e:
        write_log_entry(
            None,
            "db",
            f'[DB] CREATE EXTENSION pg_repack: {type(e).__name__}: {e}',
            level='warn',
        )

    conn_params = _parse_db_url()

    env = os.environ.copy()
    if conn_params['password']:
        env['PGPASSWORD'] = conn_params['password']
    for k, v in conn_params['extra_env'].items():
        env[k] = v
    base_args = [
        cli,
        '-h', conn_params['host'],
        '-p', conn_params['port'],
        '-d', conn_params['dbname'],
    ]
    if conn_params['user']:
        base_args += ['-U', conn_params['user']]

    results = []
    freed_total = 0
    ok_count = 0
    failed_count = 0
    for schema, table in tables:
        full = f'{schema}.{table}'
        size_before = _table_size(full)
        try:
            r = subprocess.run(
                base_args + ['--no-superuser-check', '-t', full],
                env=env,
                capture_output=True,
                timeout=3600,
            )
            if r.returncode != 0:
                err = (r.stderr.decode(errors='replace').strip()
                       or r.stdout.decode(errors='replace').strip()
                       or f'код {r.returncode}')
                results.append({
                    'table': full, 'ok': False,
                    'freed_bytes': 0, 'error': err[:500],
                })
                failed_count += 1
                continue
        except subprocess.TimeoutExpired:
            results.append({
                'table': full, 'ok': False,
                'freed_bytes': 0, 'error': 'таймаут pg_repack (>1ч)',
            })
            failed_count += 1
            continue
        except Exception as e:
            results.append({
                'table': full, 'ok': False,
                'freed_bytes': 0, 'error': f'{type(e).__name__}: {e}',
            })
            failed_count += 1
            continue
        size_after = _table_size(full)
        freed = max(0, (size_before or 0) - (size_after or 0))
        freed_total += freed
        ok_count += 1
        results.append({
            'table': full, 'ok': True,
            'freed_bytes': freed, 'error': None,
        })

    summary = {
        'mode': 'pg_repack',
        'tables_total': len(tables),
        'tables_ok': ok_count,
        'tables_failed': failed_count,
        'freed_bytes': freed_total,
        'results': results,
    }
    write_log_entry(
        None,
        "db",
        f'[DB] Сжатие БД (pg_repack): всего={len(tables)}, ok={ok_count}, '
        f'fail={failed_count}, освобождено={freed_total} байт',
        level='silent',
    )
    return summary

def _table_size(qualified: str) -> int:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT pg_total_relation_size(%s::regclass)', (qualified,))
            row = cur.fetchone()
            return int(row[0]) if row and row[0] is not None else 0

def db_get_database_size_bytes() -> int:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT pg_database_size(current_database())')
            row = cur.fetchone()
            return int(row[0]) if row and row[0] is not None else 0

def db_clear_all_history():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM log_entries")
            le = cur.rowcount
            cur.execute("DELETE FROM log")
            ll = cur.rowcount
            cur.execute("DELETE FROM batches")
            bl = cur.rowcount
        conn.commit()
    write_log_entry(None, 'db', f'Очистка истории: log_entries={le}, log={ll}, batches={bl}', level='silent')
    return {"log_entries": le, "logs": ll, "batches": bl}

def _delete_story_type_batches_and_logs(cur, story_ids) -> tuple[int, int, int]:
    """Удаляет батчи type='story' для сюжетов и их log / log_entries."""
    if not story_ids:
        return 0, 0, 0
    fmt = ','.join(['%s'] * len(story_ids))
    cur.execute(
        f"SELECT id FROM batches WHERE story_id IN ({fmt}) AND type = 'story'",
        story_ids,
    )
    batch_ids = [r[0] for r in cur.fetchall()]
    if not batch_ids:
        return 0, 0, 0
    bfmt = ','.join(['%s'] * len(batch_ids))
    cur.execute(f"""
        DELETE FROM log_entries
        WHERE log_id IN (
            SELECT id FROM log WHERE batch_id IN ({bfmt})
        )
    """, batch_ids)
    le_count = cur.rowcount
    cur.execute(f"DELETE FROM log WHERE batch_id IN ({bfmt})", batch_ids)
    ll_count = cur.rowcount
    cur.execute(f"DELETE FROM batches WHERE id IN ({bfmt})", batch_ids)
    bl_count = cur.rowcount
    return bl_count, ll_count, le_count

def db_purge_unused_stories() -> dict:
    """Удаление stories — только UI «Сценарист → Удалить лишние»."""
    with get_db() as conn:
        with conn.cursor() as cur:
            final_statuses_sql = ', '.join(f"'{s}'" for s in FINAL_BATCH_STATUSES)
            cur.execute(f"""
                SELECT s.id
                FROM stories s
                WHERE (s.grade IS NULL OR s.grade = 'bad')
                  AND NOT s.pinned
                  AND NOT EXISTS (
                      SELECT 1 FROM movies m WHERE m.story_id = s.id
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM batches b
                      WHERE b.story_id = s.id
                        AND b.status NOT IN ({final_statuses_sql})
                  )
            """)
            story_ids = [r[0] for r in cur.fetchall()]

            if not story_ids:
                conn.commit()
                write_log_entry(None, 'db', 'Очистка сюжетов: stories=0, batches=0, log=0, log_entries=0', level='silent')
                return {"stories": 0, "batches": 0, "logs": 0, "log_entries": 0}

            bl_count, ll_count, le_count = _delete_story_type_batches_and_logs(cur, story_ids)

            fmt = ','.join(['%s'] * len(story_ids))
            cur.execute(f"DELETE FROM stories WHERE id IN ({fmt})", story_ids)
            sl_count = cur.rowcount

        conn.commit()

    write_log_entry(None, 'db', f'Очистка сюжетов: stories={sl_count}, batches={bl_count}, log={ll_count}, log_entries={le_count}', level='silent')
    return {"stories": sl_count, "batches": bl_count, "logs": ll_count, "log_entries": le_count}

def db_delete_bad_movies() -> dict:
    """Удаление movies и видеофайлов — только UI «Режиссёр → Удалить неудачные»."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM movies WHERE grade = 'bad'")
            movie_ids = [r[0] for r in cur.fetchall()]
            deleted_movie_ids = [str(mid) for mid in movie_ids]

            if not movie_ids:
                conn.commit()
                write_log_entry(None, 'db', 'Удалены неудачные видео: movies=0, batches=0, log=0, log_entries=0', level='silent')
                return {"movies": 0, "batches": 0, "logs": 0, "log_entries": 0}

            mfmt = ','.join(['%s'] * len(movie_ids))

            cur.execute(f"SELECT id FROM batches WHERE movie_id IN ({mfmt})", movie_ids)
            batch_ids = [r[0] for r in cur.fetchall()]

            le_count = 0
            ll_count = 0
            bl_count = 0

            if batch_ids:
                bfmt = ','.join(['%s'] * len(batch_ids))
                cur.execute(f"""
                    DELETE FROM log_entries
                    WHERE log_id IN (
                        SELECT id FROM log WHERE batch_id IN ({bfmt})
                    )
                """, batch_ids)
                le_count = cur.rowcount

                cur.execute(f"DELETE FROM log WHERE batch_id IN ({bfmt})", batch_ids)
                ll_count = cur.rowcount

                cur.execute(f"DELETE FROM batches WHERE id IN ({bfmt})", batch_ids)
                bl_count = cur.rowcount

            cur.execute(f"DELETE FROM movies WHERE id IN ({mfmt})", movie_ids)
            ml_count = cur.rowcount

        conn.commit()

    for movie_id in deleted_movie_ids:
        db_delete_movie_video_files(movie_id)

    write_log_entry(None, 'db', f'Удалены неудачные видео: movies={ml_count}, batches={bl_count}, log={ll_count}, log_entries={le_count}', level='silent')
    return {"movies": ml_count, "batches": bl_count, "logs": ll_count, "log_entries": le_count}

def db_get_batch_status(batch_id: str) -> str | None:
    """Возвращает текущий статус батча или None если не найден."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT status FROM batches WHERE id = %s", (batch_id,))
            row = cur.fetchone()
            return row[0] if row else None

def db_delete_batch(batch_id: str) -> bool:
    """Удаляет батч и его лог. movies, stories и видеофайлы не затрагивает."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM log_entries
                WHERE log_id IN (
                    SELECT id FROM log WHERE batch_id = %s
                )
                """,
                (batch_id,),
            )
            cur.execute("DELETE FROM log WHERE batch_id = %s", (batch_id,))
            cur.execute("DELETE FROM batches WHERE id = %s", (batch_id,))
            deleted = cur.rowcount
        conn.commit()
    return deleted > 0

def db_delete_story(story_id: str) -> dict:
    """Удаление stories — только UI «Сценарист → Удалить»."""
    with get_db() as conn:
        with conn.cursor() as cur:
            bl_count, ll_count, le_count = _delete_story_type_batches_and_logs(cur, [story_id])

            cur.execute("DELETE FROM stories WHERE id = %s", (story_id,))
            sl_count = cur.rowcount

        conn.commit()

    write_log_entry(None, 'db', fmt_id_msg('Удалён сюжет {}: batches=' + str(bl_count) + ', log=' + str(ll_count) + ', log_entries=' + str(le_count), story_id), level='silent')
    return {"stories": sl_count, "batches": bl_count, "logs": ll_count, "log_entries": le_count}

def db_delete_movie(movie_id: str) -> dict:
    """Удаление movies и видеофайлов — только UI «Режиссёр → Удалить»."""
    deleted_movie_id = None
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM movies WHERE id = %s", (movie_id,))
            if not cur.fetchone():
                return {}

            cur.execute("SELECT id FROM batches WHERE movie_id = %s", (movie_id,))
            batch_ids = [r[0] for r in cur.fetchall()]

            le_count = 0
            ll_count = 0
            bl_count = 0

            if batch_ids:
                bfmt = ','.join(['%s'] * len(batch_ids))
                cur.execute(f"""
                    DELETE FROM log_entries
                    WHERE log_id IN (
                        SELECT id FROM log WHERE batch_id IN ({bfmt})
                    )
                """, batch_ids)
                le_count = cur.rowcount

                cur.execute(f"DELETE FROM log WHERE batch_id IN ({bfmt})", batch_ids)
                ll_count = cur.rowcount

                cur.execute(f"DELETE FROM batches WHERE id IN ({bfmt})", batch_ids)
                bl_count = cur.rowcount

            cur.execute("DELETE FROM movies WHERE id = %s", (movie_id,))
            ml_count = cur.rowcount
            if ml_count:
                deleted_movie_id = str(movie_id)

        conn.commit()

    if deleted_movie_id:
        db_delete_movie_video_files(deleted_movie_id)

    write_log_entry(None, 'db', fmt_id_msg('Удалено видео {}: batches=' + str(bl_count) + ', log=' + str(ll_count) + ', log_entries=' + str(le_count), movie_id), level='silent')
    return {"movies": ml_count, "batches": bl_count, "logs": ll_count, "log_entries": le_count}

def db_cleanup_batches(batch_lifetime_days: int) -> int:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, status, created_at, batch_id_source FROM batches"
            )
            rows = cur.fetchall()
            if not rows:
                return 0

            by_id: dict[str, dict] = {}
            for batch_id, status, created_at, batch_id_source in rows:
                bid = str(batch_id)
                by_id[bid] = {
                    "status": status,
                    "created_at": created_at,
                    "source": str(batch_id_source) if batch_id_source else None,
                }

            cur.execute(
                "SELECT now() - make_interval(days => %s) AS cutoff",
                (batch_lifetime_days,),
            )
            cutoff = cur.fetchone()[0]

            to_delete: list[str] = []
            for component in build_connected_batch_components(by_id):
                if any(batch_is_active(by_id[bid]["status"]) for bid in component):
                    continue
                for bid in component:
                    if by_id[bid]["created_at"] < cutoff:
                        to_delete.append(bid)

            if not to_delete:
                return 0

            fmt = ','.join(['%s'] * len(to_delete))

            cur.execute(f"""
                DELETE FROM log_entries
                WHERE log_id IN (
                    SELECT id FROM log WHERE batch_id IN ({fmt})
                )
            """, to_delete)

            cur.execute(f"DELETE FROM log WHERE batch_id IN ({fmt})", to_delete)

            cur.execute(f"DELETE FROM batches WHERE id IN ({fmt})", to_delete)

        conn.commit()
    return len(to_delete)

