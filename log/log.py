"""
Все функции логирования приложения.
"""
from db.init import get_db


def db_log_pipeline(pipeline, message, status='info', batch_id=None):
    """Записывает событие пайплайна в таблицу log. Возвращает log_id или None."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO log (batch_id, pipeline, message, status)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id
                    """,
                    (batch_id, pipeline, message, status),
                )
                log_id = cur.fetchone()[0]
            conn.commit()
        return str(log_id)
    except Exception as e:
        print(f"[DB] Ошибка db_log_pipeline: {e}")
        return None


def db_log_entry(log_id, message, level='info'):
    """Добавляет субзапись к существующей записи лога."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO log_entries (log_id, message, level) VALUES (%s, %s, %s)",
                    (log_id, message, level),
                )
            conn.commit()
    except Exception as e:
        print(f"[DB] Ошибка db_log_entry: {e}")


def db_log_interrupt_running(pipeline):
    """Переводит все незавершённые записи пайплайна из 'running' в 'прервана'."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE log SET status = 'прервана' WHERE pipeline = %s AND status = 'running'",
                    (pipeline,),
                )
                count = cur.rowcount
            conn.commit()
        if count:
            print(f"[log] {pipeline}: {count} незавершённых записей → прервана")
    except Exception as e:
        print(f"[DB] Ошибка db_log_interrupt_running: {e}")


def db_log_update(log_id, message, status):
    """Обновляет message и status существующей записи лога."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE log SET message = %s, status = %s WHERE id = %s",
                    (message, status, log_id),
                )
            conn.commit()
    except Exception as e:
        print(f"[DB] Ошибка db_log_update: {e}")


def db_log_root(message, status='info'):
    """Системная запись уровня приложения (pipeline='root', без батча)."""
    return db_log_pipeline('root', message, status=status, batch_id=None)


def db_get_log_entries(log_id):
    """Возвращает список субзаписей для указанного log_id."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT message, level, created_at FROM log_entries WHERE log_id = %s ORDER BY created_at",
                    (log_id,),
                )
                rows = cur.fetchall()
        return [
            {"msg": r[0], "level": r[1], "ts": r[2].isoformat() if r[2] else None}
            for r in rows
        ]
    except Exception as e:
        print(f"[DB] Ошибка db_get_log_entries: {e}")
        return []


def db_get_log(limit=200):
    """Плоский список записей лога (устаревший формат, для обратной совместимости)."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        l.id,
                        l.batch_id,
                        l.pipeline,
                        l.message,
                        l.status,
                        l.created_at,
                        COALESCE(
                            json_agg(
                                json_build_object(
                                    'message',    le.message,
                                    'level',      le.level,
                                    'created_at', le.created_at
                                ) ORDER BY le.created_at
                            ) FILTER (WHERE le.id IS NOT NULL),
                            '[]'
                        ) AS entries
                    FROM log l
                    LEFT JOIN log_entries le ON le.log_id = l.id
                    GROUP BY l.id
                    ORDER BY l.created_at DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = cur.fetchall()
        return [
            {
                "id":         str(row[0]),
                "batch_id":   str(row[1]) if row[1] else None,
                "pipeline":   row[2],
                "message":    row[3],
                "status":     row[4],
                "created_at": row[5].isoformat() if row[5] else None,
                "entries":    row[6],
            }
            for row in rows
        ]
    except Exception as e:
        print(f"[DB] Ошибка db_get_log: {e}")
        return []


def db_get_monitor(batch_limit=50):
    """
    Возвращает структурированные данные для монитора.
    Первичная таблица — log. Батч появляется только если есть хотя бы одна запись в log.
    - batches: батчи с вложенными log-записями, сортировка по последнему событию DESC
    - system:  системные события без батча
    """
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                # Батчи — ведущая таблица batches, LEFT JOIN log/targets
                cur.execute(
                    """
                    SELECT
                        b.id,
                        b.scheduled_at,
                        b.adhoc,
                        b.status,
                        b.created_at,
                        t.name,
                        t.aspect_ratio_x,
                        t.aspect_ratio_y,
                        MAX(l.created_at) AS last_event_at,
                        COALESCE(
                            json_agg(
                                json_build_object(
                                    'id',         l.id::text,
                                    'pipeline',   l.pipeline,
                                    'message',    l.message,
                                    'status',     l.status,
                                    'created_at', l.created_at,
                                    'entries', (
                                        SELECT COALESCE(
                                            json_agg(
                                                json_build_object(
                                                    'message',    le.message,
                                                    'level',      le.level,
                                                    'created_at', le.created_at
                                                ) ORDER BY le.created_at
                                            ),
                                            '[]'::json
                                        )
                                        FROM log_entries le WHERE le.log_id = l.id
                                    )
                                ) ORDER BY l.created_at
                            ) FILTER (WHERE l.id IS NOT NULL),
                            '[]'::json
                        ) AS logs
                    FROM batches b
                    LEFT JOIN log l ON l.batch_id = b.id
                    LEFT JOIN targets t ON t.id = b.target_id
                    GROUP BY b.id, b.scheduled_at, b.adhoc, b.status, b.created_at,
                             t.name, t.aspect_ratio_x, t.aspect_ratio_y
                    ORDER BY COALESCE(MAX(l.created_at), b.created_at) DESC
                    LIMIT %s
                    """,
                    (batch_limit,),
                )
                batch_rows = cur.fetchall()

                # Системные события (без батча)
                cur.execute(
                    """
                    SELECT
                        l.id, l.pipeline, l.message, l.status, l.created_at,
                        COALESCE(
                            json_agg(
                                json_build_object(
                                    'message',    le.message,
                                    'level',      le.level,
                                    'created_at', le.created_at
                                ) ORDER BY le.created_at
                            ) FILTER (WHERE le.id IS NOT NULL),
                            '[]'::json
                        ) AS entries
                    FROM log l
                    LEFT JOIN log_entries le ON le.log_id = l.id
                    WHERE l.batch_id IS NULL
                    GROUP BY l.id
                    ORDER BY l.created_at DESC
                    LIMIT 100
                    """
                )
                sys_rows = cur.fetchall()

        batches = [
            {
                "batch_id":       str(r[0]),
                "scheduled_at":   r[1].isoformat() if r[1] else None,
                "adhoc":          r[2],
                "batch_status":   r[3],
                "created_at":     r[4].isoformat() if r[4] else None,
                "target_name":    r[5],
                "aspect_ratio_x": r[6],
                "aspect_ratio_y": r[7],
                "last_event_at":  r[8].isoformat() if r[8] else None,
                "logs":           r[9],
            }
            for r in batch_rows
        ]
        system = [
            {
                "id":         str(r[0]),
                "pipeline":   r[1],
                "message":    r[2],
                "status":     r[3],
                "created_at": r[4].isoformat() if r[4] else None,
                "entries":    r[5],
            }
            for r in sys_rows
        ]
        return {"batches": batches, "system": system}
    except Exception as e:
        print(f"[DB] Ошибка db_get_monitor: {e}")
        return {"batches": [], "system": []}
