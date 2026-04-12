import psycopg2.extras

from .connection import get_db


def db_get_batch_logs(batch_id):
    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT
                        b.id::text                                              AS batch_id,
                        b.scheduled_at,
                        b.type,
                        b.status                                                AS batch_status,
                        b.created_at,
                        b.story_id::text                                        AS story_id,
                        (m.transcoded_data IS NOT NULL
                         OR m.raw_data IS NOT NULL)                             AS has_video_data,
                        tm.name                                                 AS text_model_name,
                        vm.name                                                 AS video_model_name
                    FROM batches b
                    LEFT JOIN movies m ON m.id = b.movie_id
                    LEFT JOIN stories s ON s.id = b.story_id
                    LEFT JOIN ai_models tm ON tm.id = s.model_id
                    LEFT JOIN ai_models vm ON vm.id = m.model_id
                    WHERE b.id = %s
                """, (batch_id,))
                row = cur.fetchone()
                if not row:
                    return None

                cur.execute("""
                    SELECT l.id::text, l.pipeline, l.message, l.status,
                           l.created_at,
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
                    WHERE l.batch_id = %s
                    GROUP BY l.id
                    ORDER BY l.created_at
                """, (batch_id,))
                logs = cur.fetchall()

        return {
            'batch_id':         row['batch_id'],
            'batch_status':     row['batch_status'],
            'created_at':       row['created_at'].isoformat() if row['created_at'] else None,
            'type':             row['type'],
            'scheduled_at':     row['scheduled_at'].isoformat() if row['scheduled_at'] else None,
            'story_id':         row['story_id'],
            'has_video_data':   bool(row['has_video_data']),
            'text_model_name':  row['text_model_name'],
            'video_model_name': row['video_model_name'],
            'logs': [
                {
                    'id':         r['id'],
                    'pipeline':   r['pipeline'],
                    'message':    r['message'],
                    'status':     r['status'],
                    'created_at': r['created_at'].isoformat() if r['created_at'] else None,
                    'entries':    r['entries'],
                }
                for r in logs
            ],
        }
    except Exception as e:
        print(f"[DB] Ошибка db_get_batch_logs: {e}")
        return None


def db_get_stories_list(show_used=True, show_bad=True):
    conditions = []
    if not show_used:
        conditions.append("NOT EXISTS (SELECT 1 FROM batches b WHERE b.story_id = s.id AND b.movie_id IS NOT NULL)")
    if not show_bad:
        conditions.append("(s.grade IS DISTINCT FROM 'bad')")
    where_clause = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT
                        s.id::text,
                        s.title,
                        s.content,
                        s.grade,
                        s.manual_changed,
                        s.model_id IS NOT NULL AS ai_generated,
                        EXISTS (
                            SELECT 1 FROM batches b
                            WHERE b.story_id = s.id AND b.movie_id IS NOT NULL
                        ) AS used,
                        am.name AS model_name
                    FROM stories s
                    LEFT JOIN ai_models am ON am.id = s.model_id
                    {where_clause}
                    ORDER BY used ASC, s.created_at DESC
                """)
                rows = cur.fetchall()
        return [
            {
                "id": row[0],
                "title": row[1] or "",
                "content": row[2] or "",
                "grade": row[3],
                "manual_changed": bool(row[4]),
                "ai_generated": bool(row[5]),
                "used": bool(row[6]),
                "model_name": row[7] or "",
            }
            for row in rows
        ]
    except Exception as e:
        print(f"[DB] Ошибка db_get_stories_list: {e}")
        return []


def db_get_stories_pool(grade_required: bool = True) -> list:
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                if grade_required:
                    grade_clause = "grade = 'good'"
                else:
                    grade_clause = "(grade IS NULL OR grade != 'bad')"
                cur.execute(f"""
                    SELECT id::text, title, content
                    FROM stories
                    WHERE {grade_clause}
                      AND NOT EXISTS (
                          SELECT 1 FROM batches b
                          WHERE b.story_id = stories.id AND b.movie_id IS NOT NULL
                      )
                    ORDER BY created_at DESC
                """)
                rows = cur.fetchall()
        return [{"id": row[0], "title": row[1] or "", "content": row[2] or ""} for row in rows]
    except Exception as e:
        print(f"[DB] Ошибка db_get_stories_pool: {e}")
        return []


def db_get_models(model_type: str):
    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT m.id, m.name, m.url, m.body, m."order", m.active,
                           m.grade, m.price, p.name AS platform_name
                    FROM ai_models m
                    LEFT JOIN ai_platforms p ON p.id = m.platform_id
                    WHERE m.type = %s
                    ORDER BY m."order" ASC
                """, (model_type,))
                rows = cur.fetchall()
        return [dict(r) for r in rows]
    except Exception as e:
        print(f"[DB] Ошибка db_get_models({model_type}): {e}")
        return []


def db_get_user_by_login(login):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT u.id, u.name, u.login, u.password,
                           r.slug, r.name AS role_name, r.module
                    FROM users u
                    LEFT JOIN user_role_links url ON url.user_id = u.id
                    LEFT JOIN user_roles r        ON r.id = url.role_id
                    WHERE u.login = %s
                    ORDER BY r.slug
                """, (login,))
                rows = cur.fetchall()
                if not rows:
                    return None
                first = rows[0]
                roles = []
                for row in rows:
                    if row[4] is not None:
                        roles.append({
                            "slug": row[4],
                            "name": row[5],
                            "module": row[6] or row[4].upper(),
                        })
                return {
                    "id": str(first[0]),
                    "name": first[1],
                    "login": first[2],
                    "password": first[3],
                    "roles": roles,
                }
    except Exception as e:
        print(f"[DB] Ошибка db_get_user_by_login: {e}")
        return None
