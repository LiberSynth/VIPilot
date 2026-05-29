import os
import platform
import shutil
import subprocess
from urllib.parse import urlparse, unquote, parse_qsl

from .connection import get_db
from .db_media import db_delete_movie_video_files
from common.statuses import FINAL_BATCH_STATUSES
from log.log import write_log_entry
from utils.utils import fmt_id_msg


def db_interrupt_stale_logs():
    """При старте приложения переводит все 'running'-записи лога в 'interrupted'.

    Вызывать один раз при инициализации, до запуска пайплайнов.
    Предотвращает «висячие» синие значки в мониторе после рестарта сервера.
    """
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE log SET status = 'interrupted' WHERE status = 'running'")
        conn.commit()


def db_log_update(log_id, message, status):
    """Обновляет message и status существующей записи лога."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE log SET message = %s, status = %s WHERE id = %s",
                (message, status, log_id),
            )
        conn.commit()


def db_get_log_entries(log_id):
    """Возвращает список субзаписей для указанного log_id."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT message, level, created_at FROM log_entries WHERE log_id = %s ORDER BY created_at DESC, id DESC",
                (log_id,),
            )
            rows = cur.fetchall()
    return [
        {"msg": r[0], "level": r[1], "ts": r[2].isoformat() if r[2] else None}
        for r in rows
    ]


def db_get_monitor():
    """
    Возвращает структурированные данные для монитора.
    Первичная таблица — log. Батч появляется только если есть хотя бы одна запись в log.
    - batches: батчи с вложенными log-записями, сортировка по последнему событию DESC
    - system:  системные события без батча (без log_entries — они грузятся лениво)
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
                    MAX(l.created_at) AS last_event_at,
                    b.story_id,
                    m.has_video_data,
                    COALESCE(
                        json_agg(
                            json_build_object(
                                'id',         l.id::text,
                                'pipeline',   l.pipeline,
                                'message',    l.message,
                                'status',     l.status,
                                'created_at', l.created_at
                            ) ORDER BY l.created_at, l.id
                        ) FILTER (WHERE l.id IS NOT NULL),
                        '[]'::json
                    ) AS logs,
                    tm.name AS text_model_name,
                    vm.name AS video_model_name,
                    b.title
                FROM batches b
                LEFT JOIN (
                    SELECT id, model_id,
                           TRUE AS has_video_data
                    FROM movies
                ) m ON m.id = b.movie_id
                LEFT JOIN log l ON l.batch_id = b.id
                LEFT JOIN stories s ON s.id = b.story_id
                LEFT JOIN ai_models tm ON tm.id = s.model_id
                LEFT JOIN ai_models vm ON vm.id = m.model_id
                GROUP BY b.id, b.scheduled_at, b.type, b.status, b.created_at,
                         b.story_id, m.has_video_data,
                         tm.name, vm.name, b.title
                ORDER BY COALESCE(MAX(l.created_at), b.created_at) DESC, b.id DESC
                """
            )
            batch_rows = cur.fetchall()

            cur.execute(
                """
                SELECT l.id, l.pipeline, l.message, l.status, l.created_at
                FROM log l
                WHERE l.batch_id IS NULL
                ORDER BY l.created_at DESC, l.id DESC
                """
            )
            sys_rows = cur.fetchall()

            cur.execute(
                """
                SELECT
                    b.id AS batch_id,
                    (COUNT(le.id) > 0) AS has_system_before,
                    COUNT(le.id) AS orphan_count,
                    MIN(le.created_at) AS orphan_min_at
                FROM (
                    SELECT id,
                           created_at AS date_begin,
                           LEAD(created_at) OVER (ORDER BY created_at DESC, id DESC) AS date_end
                    FROM batches
                    UNION ALL
                    SELECT NULL::uuid, 'infinity'::timestamptz, MAX(created_at)
                    FROM batches
                ) b
                LEFT JOIN log_entries le
                    ON  le.log_id IS NULL
                    AND le.created_at <  b.date_begin
                    AND le.created_at >= COALESCE(b.date_end, '-infinity'::timestamptz)
                GROUP BY b.id, b.date_begin, b.date_end
                ORDER BY b.date_begin DESC, b.id DESC
                """
            )
            sys_flag_rows = cur.fetchall()

            cur.execute(
                """
                SELECT b.id AS batch_id, le.message, le.level, le.created_at
                FROM (
                    SELECT id,
                           created_at AS date_begin,
                           LEAD(created_at) OVER (ORDER BY created_at DESC, id DESC) AS date_end
                    FROM batches
                    UNION ALL
                    SELECT NULL::uuid, 'infinity'::timestamptz, MAX(created_at)
                    FROM batches
                ) b
                JOIN log_entries le
                    ON  le.log_id IS NULL
                    AND le.created_at <  b.date_begin
                    AND le.created_at >= COALESCE(b.date_end, '-infinity'::timestamptz)
                ORDER BY le.created_at DESC, le.id DESC
                """
            )
            orphan_rows = cur.fetchall()

    orphans_by_batch: dict = {}
    orphans_top: list = []
    for r in orphan_rows:
        bid_o, msg_o, lvl_o, at_o = r[0], r[1], r[2], r[3]
        entry_o = {
            "message":    msg_o,
            "level":      lvl_o,
            "created_at": at_o.isoformat() if at_o else None,
        }
        if bid_o is None:
            orphans_top.append(entry_o)
        else:
            orphans_by_batch.setdefault(str(bid_o), []).append(entry_o)

    has_system_map = {}
    has_system_top = None
    for r in sys_flag_rows:
        bid, has_before, count, min_at = r[0], bool(r[1]), int(r[2] or 0), r[3]
        entry = {
            "has_system_before": has_before,
            "orphan_count":      count,
            "orphan_min_at":     min_at.isoformat() if min_at else None,
            "orphan_entries":    orphans_by_batch.get(str(bid) if bid else "", []),
        }
        if bid is None:
            if has_before:
                entry["orphan_entries"] = orphans_top
                has_system_top = entry
        else:
            has_system_map[str(bid)] = entry

    batches = [
        {
            "batch_id":       str(r[0]),
            "scheduled_at":   r[1].isoformat() if r[1] else None,
            "type":           r[2],
            "batch_status":   r[3],
            "created_at":     r[4].isoformat() if r[4] else None,
            "last_event_at":  r[5].isoformat() if r[5] else None,
            "story_id":       str(r[6]) if r[6] else None,
            "has_video_data": bool(r[7]),
            "logs":           r[8],
            "text_model_name":  r[9],
            "video_model_name": r[10],
            "title":            r[11],
            **has_system_map.get(str(r[0]), {
                "has_system_before": False,
                "orphan_count":      0,
                "orphan_min_at":     None,
                "orphan_entries":    [],
            }),
        }
        for r in batch_rows
    ]
    system = [
        {
            "id": str(r[0]),
            "pipeline": r[1],
            "message": r[2],
            "status": r[3],
            "created_at": r[4].isoformat() if r[4] else None,
        }
        for r in sys_rows
    ]
    return {"batches": batches, "system": system, "has_system_top": has_system_top}


def db_get_system_log_entries(log_id: str) -> list:
    """Возвращает log_entries для системного лога (без batch_id) по log_id."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT le.message, le.level, le.created_at
                FROM log_entries le
                JOIN log l ON l.id = le.log_id
                WHERE le.log_id = %s AND l.batch_id IS NULL
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
        }
        for r in rows
    ]


def db_get_system_window_orphans(
    before_iso: str | None = None,
    after_iso:  str | None = None,
) -> list:
    """Orphan log_entries (log_id IS NULL, level != 'silent') в заданном временном окне.

    before_iso — верхняя граница (исключительная), after_iso — нижняя (включительная).
    Если граница не указана — окно открыто с этой стороны.
    """
    if not before_iso and not after_iso:
        return []
    with get_db() as conn:
        with conn.cursor() as cur:
            if before_iso and after_iso:
                cur.execute(
                    """
                    SELECT message, level, created_at FROM log_entries
                    WHERE log_id IS NULL
                      AND created_at <  %s::timestamptz
                      AND created_at >= %s::timestamptz
                    ORDER BY created_at DESC, id DESC
                    """,
                    (before_iso, after_iso),
                )
            elif before_iso:
                cur.execute(
                    """
                    SELECT message, level, created_at FROM log_entries
                    WHERE log_id IS NULL
                      AND created_at < %s::timestamptz
                    ORDER BY created_at DESC, id DESC
                    """,
                    (before_iso,),
                )
            else:
                cur.execute(
                    """
                    SELECT message, level, created_at FROM log_entries
                    WHERE log_id IS NULL
                      AND created_at >= %s::timestamptz
                    ORDER BY created_at DESC, id DESC
                    """,
                    (after_iso,),
                )
            rows = cur.fetchall()
    return [
        {
            "message":    r[0],
            "level":      r[1],
            "created_at": r[2].isoformat() if r[2] else None,
        }
        for r in rows
    ]


def db_get_batch_log_entries(batch_id: str) -> list:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    l.id::text,
                    COALESCE(
                        json_agg(
                            json_build_object(
                                'message',    le.message,
                                'level',      le.level,
                                'created_at', le.created_at
                            ) ORDER BY le.created_at DESC, le.id DESC
                        ) FILTER (WHERE le.id IS NOT NULL),
                        '[]'::json
                    ) AS entries
                FROM log l
                LEFT JOIN log_entries le ON le.log_id = l.id
                WHERE l.batch_id = %s
                GROUP BY l.id
                ORDER BY l.created_at, l.id
                """,
                (batch_id,),
            )
            rows = cur.fetchall()
    return [{"id": r[0], "entries": r[1]} for r in rows]


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
    from utils.pkg_bootstrap import ensure_pg_repack_in_path, get_pg_repack_bootstrap_error

    tables = _public_tables()
    # На Windows официального pg_repack нет — только поиск уже установленного CLI.
    ensure_pg_repack_in_path(auto_install=(platform.system() != 'Windows'))
    cli = shutil.which('pg_repack')
    if not cli:
        bootstrap_error = get_pg_repack_bootstrap_error()
        if platform.system() == 'Windows':
            write_log_entry(
                None,
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
    write_log_entry(None, f"[DB] Очистка истории: log_entries={le}, log={ll}, batches={bl}", level='silent')
    return {"log_entries": le, "logs": ll, "batches": bl}


def db_purge_unused_stories() -> dict:
    with get_db() as conn:
        with conn.cursor() as cur:
            final_statuses_sql = ', '.join(f"'{s}'" for s in FINAL_BATCH_STATUSES)
            cur.execute(f"""
                SELECT DISTINCT s.id
                FROM stories s
                LEFT JOIN batches b
                       ON b.story_id = s.id
                      AND (b.movie_id IS NOT NULL OR b.status NOT IN ({final_statuses_sql}))
                WHERE (s.grade IS NULL OR s.grade = 'bad')
                  AND NOT s.pinned
                  AND b.id IS NULL
            """)
            story_ids = [r[0] for r in cur.fetchall()]

            if not story_ids:
                conn.commit()
                write_log_entry(None, "[DB] Очистка сюжетов: stories=0, batches=0, log=0, log_entries=0", level='silent')
                return {"stories": 0, "batches": 0, "logs": 0, "log_entries": 0}

            fmt = ','.join(['%s'] * len(story_ids))

            cur.execute(f"SELECT id FROM batches WHERE story_id IN ({fmt})", story_ids)
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

            cur.execute(f"DELETE FROM stories WHERE id IN ({fmt})", story_ids)
            sl_count = cur.rowcount

        conn.commit()

    write_log_entry(None, f"[DB] Очистка сюжетов: stories={sl_count}, batches={bl_count}, log={ll_count}, log_entries={le_count}", level='silent')
    return {"stories": sl_count, "batches": bl_count, "logs": ll_count, "log_entries": le_count}


def db_delete_bad_movies() -> dict:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM movies WHERE grade = 'bad'")
            movie_ids = [r[0] for r in cur.fetchall()]
            deleted_movie_ids = [str(mid) for mid in movie_ids]

            if not movie_ids:
                conn.commit()
                write_log_entry(None, "[DB] Удалены неудачные видео: movies=0, batches=0, log=0, log_entries=0", level='silent')
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

    write_log_entry(None, f"[DB] Удалены неудачные видео: movies={ml_count}, batches={bl_count}, log={ll_count}, log_entries={le_count}", level='silent')
    return {"movies": ml_count, "batches": bl_count, "logs": ll_count, "log_entries": le_count}


def db_get_batch_status(batch_id: str) -> str | None:
    """Возвращает текущий статус батча или None если не найден."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT status FROM batches WHERE id = %s", (batch_id,))
            row = cur.fetchone()
            return row[0] if row else None


def db_delete_batch(batch_id: str) -> bool:
    deleted_movie_id = None
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
            cur.execute(
                "SELECT movie_id FROM batches WHERE id = %s AND movie_id IS NOT NULL",
                (batch_id,),
            )
            row = cur.fetchone()
            movie_id = row[0] if row else None
            cur.execute("DELETE FROM batches WHERE id = %s", (batch_id,))
            deleted = cur.rowcount
            if movie_id:
                cur.execute("DELETE FROM movies WHERE id = %s", (movie_id,))
                if cur.rowcount:
                    deleted_movie_id = str(movie_id)
        conn.commit()
    if deleted_movie_id:
        db_delete_movie_video_files(deleted_movie_id)
    return deleted > 0


def db_delete_story(story_id: str) -> dict:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM batches WHERE story_id = %s", (story_id,))
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

            cur.execute("DELETE FROM stories WHERE id = %s", (story_id,))
            sl_count = cur.rowcount

        conn.commit()

    write_log_entry(None, fmt_id_msg("[DB] Удалён сюжет {}: batches=" + str(bl_count) + ", log=" + str(ll_count) + ", log_entries=" + str(le_count), story_id), level='silent')
    return {"stories": sl_count, "batches": bl_count, "logs": ll_count, "log_entries": le_count}


def db_delete_movie(movie_id: str) -> dict:
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

    write_log_entry(None, fmt_id_msg("[DB] Удалено видео {}: batches=" + str(bl_count) + ", log=" + str(ll_count) + ", log_entries=" + str(le_count), movie_id), level='silent')
    return {"movies": ml_count, "batches": bl_count, "logs": ll_count, "log_entries": le_count}


def db_cleanup_batches(batch_lifetime_days: int) -> int:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id FROM batches
                WHERE (
                    status IN ('published', 'cancelled')
                    OR (type = 'movie_manual' AND status = 'movie_manual')
                    OR (type = 'movie' AND status = 'ready')
                    OR (type = 'planning' AND status = 'ready')
                    OR (type = 'story' AND status = 'ready')
                )
                  AND created_at < now() - make_interval(days => %s)
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


