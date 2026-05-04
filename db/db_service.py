import glob
import json
import os
import shutil
import subprocess
import threading
import time
from datetime import datetime, timezone
from urllib.parse import urlparse, unquote, parse_qsl

from .connection import get_db
from common.statuses import FINAL_BATCH_STATUSES
from log.log import write_log_entry
from utils.utils import fmt_id_msg


_db_op_lock = threading.Lock()
_backup_proc_holder: dict = {'proc': None}
_restore_proc_holder: dict = {'psql': None, 'rest': None}
_backup_cancel_event = threading.Event()
_restore_cancel_event = threading.Event()

_BACKUP_TMP_GLOB = '/tmp/vipilot-backup-*.dump'
_RESTORE_TMP_GLOB = '/tmp/vipilot-restore-*.dump'

ENV_BACKUP_STATE = 'db_backup_state'
ENV_RESTORE_STATE = 'db_restore_state'


def _drain_to_buffer(stream, buf: list) -> threading.Thread:
    """Запускает фоновый поток, который читает stream до EOF в buf.

    Нужно для дренирования stderr дочерних процессов: иначе при заполнении
    буфера трубы процесс может зависнуть в write().
    """
    def _run():
        try:
            while True:
                chunk = stream.read(65536)
                if not chunk:
                    break
                buf.append(chunk)
        except Exception:
            pass
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return t


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_state(env_key: str) -> dict:
    from db.db_simple import env_get
    raw = env_get(env_key, '')
    if not raw:
        return {'state': 'idle'}
    try:
        d = json.loads(raw)
        if not isinstance(d, dict) or 'state' not in d:
            return {'state': 'idle'}
        return d
    except Exception:
        return {'state': 'idle'}


def _set_state(env_key: str, state: dict) -> None:
    from db.db_simple import env_set
    env_set(env_key, json.dumps(state, ensure_ascii=False))


def _is_pid_alive(pid) -> bool:
    if not pid:
        return False
    try:
        os.kill(int(pid), 0)
        return True
    except (OSError, ValueError, TypeError):
        return False


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
                           (transcoded_data IS NOT NULL OR raw_data IS NOT NULL) AS has_video_data
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
    теми же опциями, что и приложение (в Replit/Neon обычно требуется
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


def db_vacuum_full() -> dict:
    """Сжимает все таблицы схемы public через pg_repack (онлайн, без блокировки).

    По каждой таблице запускает отдельный процесс `pg_repack -t schema.table`,
    собирает размер до/после из `pg_total_relation_size`. Падение по одной
    таблице не валит весь прогон. Возвращает сводку:
    `{tables_total, tables_ok, tables_failed, freed_bytes, results: [...]}`.
    """
    cli = shutil.which('pg_repack')
    if not cli:
        raise RuntimeError('pg_repack CLI не найден в окружении')
    conn_params = _parse_db_url()

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT schemaname, tablename
                FROM pg_tables
                WHERE schemaname = 'public'
                ORDER BY tablename
            """)
            tables = [(r[0], r[1]) for r in cur.fetchall()]

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
        'tables_total': len(tables),
        'tables_ok': ok_count,
        'tables_failed': failed_count,
        'freed_bytes': freed_total,
        'results': results,
    }
    write_log_entry(
        None,
        f'[DB] Сжатие БД: всего={len(tables)}, ok={ok_count}, '
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


def db_cleanup_video_data(file_lifetime_days: int) -> int:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE movies
                   SET transcoded_data = NULL,
                       raw_data        = NULL
                WHERE id IN (
                    SELECT movie_id FROM batches
                    WHERE status = ANY(%s)
                      AND created_at < now() - make_interval(days => %s)
                      AND movie_id IS NOT NULL
                )
                  AND COALESCE(transcoded_data, raw_data) IS NOT NULL
            """, (list(FINAL_BATCH_STATUSES), file_lifetime_days))
            count = cur.rowcount
        conn.commit()
    return count


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
        conn.commit()
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

        conn.commit()

    write_log_entry(None, fmt_id_msg("[DB] Удалено видео {}: batches=" + str(bl_count) + ", log=" + str(ll_count) + ", log_entries=" + str(le_count), movie_id), level='silent')
    return {"movies": ml_count, "batches": bl_count, "logs": ll_count, "log_entries": le_count}


def db_cleanup_batches(batch_lifetime_days: int) -> int:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id FROM batches
                WHERE (
                    status IN ('published', 'cancelled')
                    OR (type = 'movie_probe' AND status = 'movie_probe')
                    OR (type = 'story_probe' AND status = 'story_probe')
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


def db_backup_check_available() -> tuple[bool, str]:
    """Проверяет доступность pg_dump CLI и его базовую работоспособность."""
    cli = shutil.which('pg_dump')
    if not cli:
        return False, 'pg_dump CLI не найден в окружении (нужен PostgreSQL 16; добавь pkgs.postgresql_16 в replit.nix)'
    try:
        r = subprocess.run([cli, '--version'], capture_output=True, timeout=5)
        if r.returncode != 0:
            return False, f'pg_dump --version вернул код {r.returncode}'
    except Exception as e:
        return False, f'pg_dump: ошибка вызова CLI — {e}'
    return True, ''


def db_restore_check_available() -> tuple[bool, str]:
    """Проверяет доступность psql и pg_restore CLI."""
    for name in ('pg_restore', 'psql'):
        if not shutil.which(name):
            return False, f'{name} CLI не найден в окружении (нужен PostgreSQL 16; добавь pkgs.postgresql_16 в replit.nix)'
    return True, ''


def get_db_backup_state() -> dict:
    """Возвращает текущее состояние бэкапа из env с актуальным bytes_written и проверкой PID.

    PID-liveness проверяется ТОЛЬКО если PID уже установлен; до этого state считается
    валидным running ('starting' окно — воркер ещё не успел заспавнить процесс).
    """
    s = _get_state(ENV_BACKUP_STATE)
    if s.get('state') == 'running':
        pid = s.get('pid')
        if pid and not _is_pid_alive(pid):
            s = {
                'state': 'failed',
                'error': 'Процесс прерван (рестарт сервера или внешнее завершение).',
                'finished_at': _now_iso(),
            }
            _set_state(ENV_BACKUP_STATE, s)
    if s.get('state') in ('running', 'ready'):
        fn = s.get('filename')
        if fn:
            try:
                s['bytes_written'] = os.path.getsize(fn)
            except Exception:
                s['bytes_written'] = 0
    return s


def get_db_restore_state() -> dict:
    s = _get_state(ENV_RESTORE_STATE)
    if s.get('state') == 'running':
        pid = s.get('pid')
        if pid and not _is_pid_alive(pid):
            s = {
                'state': 'failed',
                'error': 'Процесс прерван (рестарт сервера или внешнее завершение).',
                'finished_at': _now_iso(),
            }
            _set_state(ENV_RESTORE_STATE, s)
    return s


def _estimate_db_size_bytes() -> int:
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_database_size(current_database())")
                row = cur.fetchone()
                return int(row[0]) if row else 0
    except Exception:
        return 0


def start_db_backup() -> dict:
    """Запускает фоновое создание бэкапа в файл /tmp/vipilot-backup-*.dump.

    Возвращает текущее состояние. Если бэкап уже идёт или готов — возвращает его.
    Если идёт восстановление — бросает RuntimeError.
    """
    with _db_op_lock:
        b = get_db_backup_state()
        r = get_db_restore_state()
        if b.get('state') in ('running', 'ready'):
            return b
        if r.get('state') == 'running':
            raise RuntimeError('Идёт восстановление БД')

        ok, err = db_backup_check_available()
        if not ok:
            state = {'state': 'failed', 'error': err, 'finished_at': _now_iso()}
            _set_state(ENV_BACKUP_STATE, state)
            write_log_entry(None, f'[DB] Бэкап БД: pg_dump недоступен: {err}', level='silent')
            write_log_entry(None, 'Бэкап БД: ошибка создания', level='error')
            return state

        for old in glob.glob(_BACKUP_TMP_GLOB):
            try: os.unlink(old)
            except Exception: pass

        _backup_cancel_event.clear()
        ts = datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')
        filename = f'/tmp/vipilot-backup-{ts}.dump'
        bytes_total_est = _estimate_db_size_bytes()
        state = {
            'state': 'running',
            'pid': None,
            'filename': filename,
            'started_at': _now_iso(),
            'bytes_total_est': bytes_total_est,
        }
        _set_state(ENV_BACKUP_STATE, state)
        write_log_entry(None, 'Бэкап БД: запущено создание', level='info')
        write_log_entry(
            None,
            f'[DB] Бэкап БД: запущено (файл {filename}, оценка несжатого размера {bytes_total_est} байт)',
            level='silent',
        )
        t = threading.Thread(target=_db_backup_worker, args=(filename,), daemon=True)
        t.start()
        return get_db_backup_state()


def _db_backup_worker(filename: str):
    cli = shutil.which('pg_dump')
    conn_params = _parse_db_url()
    env = os.environ.copy()
    if conn_params['password']:
        env['PGPASSWORD'] = conn_params['password']
    for k, v in conn_params['extra_env'].items():
        env[k] = v
    args = [
        cli,
        '-h', conn_params['host'], '-p', conn_params['port'],
        '-d', conn_params['dbname'],
        '-Fc', '-Z', '6',
        '--no-owner', '--no-privileges',
        '-f', filename,
    ]
    if conn_params['user']:
        args += ['-U', conn_params['user']]

    started = time.monotonic()
    proc = None
    err_buf: list = []
    try:
        if _backup_cancel_event.is_set():
            try: os.unlink(filename)
            except Exception: pass
            _set_state(ENV_BACKUP_STATE, {'state': 'idle'})
            write_log_entry(None, '[DB] Бэкап БД отменён до запуска pg_dump', level='silent')
            write_log_entry(None, 'Бэкап БД: отменён', level='info')
            return
        proc = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, env=env)
        _backup_proc_holder['proc'] = proc
        if _backup_cancel_event.is_set():
            try: proc.kill()
            except Exception: pass

        s = _get_state(ENV_BACKUP_STATE)
        s['pid'] = proc.pid
        _set_state(ENV_BACKUP_STATE, s)

        err_t = _drain_to_buffer(proc.stderr, err_buf)
        rc = proc.wait()
        err_t.join(timeout=5)
        elapsed = time.monotonic() - started

        if _backup_cancel_event.is_set() or _get_state(ENV_BACKUP_STATE).get('state') == 'cancelled':
            try: os.unlink(filename)
            except Exception: pass
            _set_state(ENV_BACKUP_STATE, {'state': 'idle'})
            write_log_entry(None, '[DB] Бэкап БД отменён пользователем', level='silent')
            write_log_entry(None, 'Бэкап БД: отменён', level='info')
            return

        if rc != 0:
            err = b''.join(err_buf).decode(errors='replace').strip()[:500]
            _set_state(ENV_BACKUP_STATE, {
                'state': 'failed',
                'error': f'pg_dump rc={rc}: {err}',
                'finished_at': _now_iso(),
            })
            try: os.unlink(filename)
            except Exception: pass
            write_log_entry(None, f'[DB] Бэкап БД: ошибка pg_dump rc={rc}: {err}', level='silent')
            write_log_entry(None, 'Бэкап БД: ошибка создания', level='error')
            return

        try:
            size = os.path.getsize(filename)
        except Exception:
            size = 0

        prest = shutil.which('pg_restore')
        if prest:
            try:
                v = subprocess.run([prest, '--list', filename], capture_output=True, timeout=30)
                if v.returncode != 0:
                    raise RuntimeError(
                        f'pg_restore --list rc={v.returncode}: '
                        f'{v.stderr.decode(errors="replace")[:300]}'
                    )
            except Exception as e:
                _set_state(ENV_BACKUP_STATE, {
                    'state': 'failed',
                    'error': f'Файл бэкапа не прошёл валидацию: {e}',
                    'finished_at': _now_iso(),
                })
                try: os.unlink(filename)
                except Exception: pass
                write_log_entry(None, f'[DB] Бэкап БД: валидация провалилась: {e}', level='silent')
                write_log_entry(None, 'Бэкап БД: ошибка создания', level='error')
                return

        _set_state(ENV_BACKUP_STATE, {
            'state': 'ready',
            'filename': filename,
            'bytes_total': size,
            'finished_at': _now_iso(),
            'elapsed_sec': round(elapsed, 1),
        })
        write_log_entry(
            None,
            f'[DB] Бэкап БД создан: {size} байт за {elapsed:.1f}c, ожидает скачивания',
            level='silent',
        )
        write_log_entry(None, 'Бэкап БД: создан, готов к скачиванию', level='info')
    except Exception as e:
        _set_state(ENV_BACKUP_STATE, {
            'state': 'failed',
            'error': str(e)[:500],
            'finished_at': _now_iso(),
        })
        try: os.unlink(filename)
        except Exception: pass
        write_log_entry(None, f'[DB] Бэкап БД: исключение в воркере: {e}', level='silent')
        write_log_entry(None, 'Бэкап БД: ошибка создания', level='error')
    finally:
        _backup_proc_holder['proc'] = None
        if proc is not None and proc.poll() is None:
            try: proc.kill()
            except Exception: pass


def cancel_db_backup() -> dict:
    """Отменяет бэкап (running) или сбрасывает финальный state (ready/failed) в idle."""
    with _db_op_lock:
        s = _get_state(ENV_BACKUP_STATE)
        st = s.get('state')
    if st == 'running':
        _backup_cancel_event.set()
        s['state'] = 'cancelled'
        _set_state(ENV_BACKUP_STATE, s)
        proc = _backup_proc_holder.get('proc')
        if proc is not None:
            try: proc.kill()
            except Exception: pass
        write_log_entry(None, '[DB] Бэкап БД: запрошена отмена пользователем', level='silent')
    elif st == 'ready':
        fn = s.get('filename')
        if fn:
            try: os.unlink(fn)
            except Exception: pass
        _set_state(ENV_BACKUP_STATE, {'state': 'idle'})
        write_log_entry(None, '[DB] Бэкап БД: готовый файл удалён по запросу пользователя', level='silent')
    elif st in ('failed', 'cancelled'):
        _set_state(ENV_BACKUP_STATE, {'state': 'idle'})
    return _get_state(ENV_BACKUP_STATE)


def get_backup_download_filename() -> str:
    s = _get_state(ENV_BACKUP_STATE)
    fn = s.get('filename') or ''
    return os.path.basename(fn) or 'vipilot-backup.dump'


def stream_db_backup_for_download(chunk_size: int = 65536):
    """Стримит готовый файл бэкапа клиенту. После завершения (нормального или при обрыве)
    удаляет файл и сбрасывает state в idle.
    """
    s = _get_state(ENV_BACKUP_STATE)
    if s.get('state') != 'ready':
        raise RuntimeError(f'Файл бэкапа не готов (state={s.get("state")})')
    filename = s.get('filename')
    if not filename or not os.path.exists(filename):
        _set_state(ENV_BACKUP_STATE, {
            'state': 'failed',
            'error': 'Файл бэкапа отсутствует на диске',
            'finished_at': _now_iso(),
        })
        raise RuntimeError('Файл бэкапа отсутствует на диске')

    sent = 0
    completed = False
    try:
        with open(filename, 'rb') as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    completed = True
                    break
                sent += len(chunk)
                yield chunk
    finally:
        try: os.unlink(filename)
        except Exception: pass
        _set_state(ENV_BACKUP_STATE, {'state': 'idle'})
        if completed:
            write_log_entry(None, f'[DB] Бэкап БД скачан клиентом: {sent} байт', level='silent')
            write_log_entry(None, 'Бэкап БД: скачан', level='info')
        else:
            write_log_entry(
                None,
                f'[DB] Бэкап БД: скачивание прервано на {sent} байт, файл удалён',
                level='silent',
            )


# ====================== RESTORE ======================

def start_db_restore_from_path(tmp_path: str) -> dict:
    """Запускает фоновое восстановление из указанного файла.

    Восстановление атомарно (psql --single-transaction): при ошибке БД остаётся
    в исходном состоянии. Движок приостанавливается, в finally возобновляется
    только если до старта он был running.
    """
    with _db_op_lock:
        b = get_db_backup_state()
        r = get_db_restore_state()
        if r.get('state') == 'running':
            try: os.unlink(tmp_path)
            except Exception: pass
            return r
        if b.get('state') in ('running', 'ready'):
            try: os.unlink(tmp_path)
            except Exception: pass
            raise RuntimeError('Идёт операция бэкапа')

        ok, err = db_restore_check_available()
        if not ok:
            try: os.unlink(tmp_path)
            except Exception: pass
            state = {'state': 'failed', 'error': err, 'finished_at': _now_iso()}
            _set_state(ENV_RESTORE_STATE, state)
            write_log_entry(None, f'[DB] Восстановление БД: psql/pg_restore недоступны: {err}', level='silent')
            write_log_entry(None, 'Восстановление БД: не удалось', level='error')
            return state

        from db.db_simple import env_get as _env_get, env_set as _env_set
        import common.environment as environment
        was_running = _env_get('workflow_state', 'running') != 'pause'
        if was_running:
            _env_set('workflow_state', 'pause')
            environment.set_paused()
            write_log_entry(None, '[DB] Восстановление БД: движок приостановлен', level='silent')

        try:
            dump_size = os.path.getsize(tmp_path)
        except Exception:
            dump_size = 0
        _restore_cancel_event.clear()
        state = {
            'state': 'running',
            'pid': None,
            'phase': 'preparing',
            'tmp_path': tmp_path,
            'started_at': _now_iso(),
            'was_running': was_running,
            'bytes_total': dump_size,
        }
        _set_state(ENV_RESTORE_STATE, state)
        write_log_entry(
            None,
            f'[DB] Восстановление БД: запущено (файл {tmp_path}, {dump_size} байт)',
            level='silent',
        )
        write_log_entry(None, 'Восстановление БД: запущено', level='info')
        t = threading.Thread(target=_db_restore_worker, args=(tmp_path, was_running), daemon=True)
        t.start()
        return get_db_restore_state()


def _db_restore_worker(tmp_path: str, was_running: bool):
    psql_bin = shutil.which('psql')
    prest_bin = shutil.which('pg_restore')
    conn_params = _parse_db_url()
    env = os.environ.copy()
    if conn_params['password']:
        env['PGPASSWORD'] = conn_params['password']
    for k, v in conn_params['extra_env'].items():
        env[k] = v

    psql_proc = None
    rest_proc = None
    started = time.monotonic()
    try:
        if _restore_cancel_event.is_set():
            _set_state(ENV_RESTORE_STATE, {'state': 'idle'})
            write_log_entry(None, '[DB] Восстановление БД отменено до запуска psql', level='silent')
            write_log_entry(None, 'Восстановление БД: отменено', level='info')
            return

        from db.connection import close_pool
        close_pool()

        psql_args = [
            psql_bin,
            '-h', conn_params['host'], '-p', conn_params['port'],
            '-d', conn_params['dbname'],
            '-v', 'ON_ERROR_STOP=1',
            '--single-transaction',
            '-q', '-X',
        ]
        if conn_params['user']:
            psql_args += ['-U', conn_params['user']]
        rest_args = [prest_bin, '--no-owner', '--no-privileges', '-f', '-', tmp_path]

        psql_proc = subprocess.Popen(
            psql_args, stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env,
        )
        _restore_proc_holder['psql'] = psql_proc
        if _restore_cancel_event.is_set():
            try: psql_proc.kill()
            except Exception: pass

        s = _get_state(ENV_RESTORE_STATE)
        s['pid'] = psql_proc.pid
        s['phase'] = 'restoring'
        _set_state(ENV_RESTORE_STATE, s)

        psql_err_buf: list = []
        psql_out_buf: list = []
        rest_err_buf: list = []
        psql_err_t = _drain_to_buffer(psql_proc.stderr, psql_err_buf)
        psql_out_t = _drain_to_buffer(psql_proc.stdout, psql_out_buf)

        if _restore_cancel_event.is_set():
            try: psql_proc.kill()
            except Exception: pass
            _set_state(ENV_RESTORE_STATE, {'state': 'idle'})
            write_log_entry(None, '[DB] Восстановление БД отменено до destructive preamble', level='silent')
            write_log_entry(None, 'Восстановление БД: отменено', level='info')
            return

        preamble = (
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE pid <> pg_backend_pid() AND datname = current_database();\n"
            "DROP SCHEMA IF EXISTS public CASCADE;\n"
            "CREATE SCHEMA public;\n"
        )
        try:
            psql_proc.stdin.write(preamble.encode())
            psql_proc.stdin.flush()
        except BrokenPipeError:
            pass

        rest_proc = subprocess.Popen(
            rest_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env,
        )
        _restore_proc_holder['rest'] = rest_proc
        rest_err_t = _drain_to_buffer(rest_proc.stderr, rest_err_buf)

        try:
            shutil.copyfileobj(rest_proc.stdout, psql_proc.stdin)
        except BrokenPipeError:
            pass
        finally:
            try: rest_proc.stdout.close()
            except Exception: pass

        rest_rc = rest_proc.wait()
        rest_err_t.join(timeout=5)

        try: psql_proc.stdin.close()
        except Exception: pass
        psql_rc = psql_proc.wait()
        psql_err_t.join(timeout=5)
        psql_out_t.join(timeout=5)

        rest_err = b''.join(rest_err_buf).decode(errors='replace').strip()[:500]
        psql_err = b''.join(psql_err_buf).decode(errors='replace').strip()[:500]

        cur_state = _get_state(ENV_RESTORE_STATE)
        if cur_state.get('state') == 'cancelled':
            _set_state(ENV_RESTORE_STATE, {'state': 'idle'})
            write_log_entry(None, '[DB] Восстановление БД отменено пользователем', level='silent')
            write_log_entry(None, 'Восстановление БД: отменено', level='info')
            return

        if rest_rc != 0:
            raise RuntimeError(f'pg_restore: rc={rest_rc}; {rest_err}')
        if psql_rc != 0:
            raise RuntimeError(f'psql: rc={psql_rc}; {psql_err}')

        elapsed = time.monotonic() - started
        _set_state(ENV_RESTORE_STATE, {'state': 'idle'})
        write_log_entry(
            None,
            f'[DB] Восстановление БД из бэкапа завершено успешно за {elapsed:.1f}c',
            level='silent',
        )
        write_log_entry(None, 'Восстановление БД: завершено успешно', level='info')
    except Exception as e:
        _set_state(ENV_RESTORE_STATE, {
            'state': 'failed',
            'error': str(e)[:500],
            'finished_at': _now_iso(),
        })
        write_log_entry(None, f'[DB] Восстановление БД: ошибка: {e}', level='silent')
        write_log_entry(None, 'Восстановление БД: не удалось', level='error')
    finally:
        try: os.unlink(tmp_path)
        except Exception: pass
        _restore_proc_holder['psql'] = None
        _restore_proc_holder['rest'] = None
        if psql_proc and psql_proc.poll() is None:
            try: psql_proc.kill()
            except Exception: pass
        if rest_proc and rest_proc.poll() is None:
            try: rest_proc.kill()
            except Exception: pass
        try:
            from db.db_simple import env_set as _env_set2
            import common.environment as environment2
            if was_running:
                _env_set2('workflow_state', 'running')
                environment2.set_running()
                write_log_entry(None, '[DB] Восстановление БД: движок возобновлён', level='silent')
        except Exception:
            pass
        try:
            from db.connection import close_pool as _close_pool2
            _close_pool2()
        except Exception:
            pass


def cancel_db_restore() -> dict:
    """Отменяет восстановление (running) или сбрасывает финальный state (failed) в idle.

    При отмене running: выставляет cancel-event ДО любого kill (воркер проверяет event
    в нескольких контрольных точках, в т.ч. перед destructive preamble), затем убивает
    psql/pg_restore если они уже стартовали. Postgres откатит транзакцию по обрыву
    соединения — БД останется в исходном состоянии.
    """
    with _db_op_lock:
        s = _get_state(ENV_RESTORE_STATE)
        st = s.get('state')
    if st == 'running':
        _restore_cancel_event.set()
        s['state'] = 'cancelled'
        _set_state(ENV_RESTORE_STATE, s)
        for k in ('rest', 'psql'):
            p = _restore_proc_holder.get(k)
            if p is not None:
                try: p.kill()
                except Exception: pass
        write_log_entry(None, '[DB] Восстановление БД: запрошена отмена пользователем', level='silent')
    elif st in ('failed', 'cancelled'):
        _set_state(ENV_RESTORE_STATE, {'state': 'idle'})
    return _get_state(ENV_RESTORE_STATE)


def cleanup_db_op_state_on_startup():
    """Очищает зависшие state бэкапа/восстановления и tmp-файлы. Вызывать при старте Flask.

    После SIGKILL рестарта Replit рабочие потоки не успевают записать финальный state.
    Любой не-idle state на старте — заведомо мусор.
    """
    for env_key, label in ((ENV_BACKUP_STATE, 'бэкапа'), (ENV_RESTORE_STATE, 'восстановления')):
        s = _get_state(env_key)
        st = s.get('state')
        if st and st != 'idle':
            write_log_entry(
                None,
                f'[DB] Сброс зависшего state {label} (state={st}): процесс не пережил рестарт сервера',
                level='silent',
            )
            _set_state(env_key, {'state': 'idle'})
    for pat in (_BACKUP_TMP_GLOB, _RESTORE_TMP_GLOB):
        for f in glob.glob(pat):
            try:
                os.unlink(f)
                write_log_entry(None, f'[DB] Удалён остаточный tmp-файл операции БД: {f}', level='silent')
            except Exception:
                pass
