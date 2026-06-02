import psycopg2.extras

from .connection import get_db


def db_get_batch_logs(batch_id):
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
                    (m.id IS NOT NULL)                                      AS has_video_data,
                    b.movie_id::text                                        AS movie_id,
                    tm.name                                                 AS text_model_name,
                    vm.name                                                 AS video_model_name,
                    b.title
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
                SELECT le.message, le.level, le.created_at, le.channel
                FROM log_entries le
                JOIN log l ON l.id = le.log_id
                WHERE l.batch_id = %s::uuid
                ORDER BY le.created_at, le.id
            """, (batch_id,))
            entries = cur.fetchall()

    return {
        'batch_id':         row['batch_id'],
        'batch_status':     row['batch_status'],
        'created_at':       row['created_at'].isoformat() if row['created_at'] else None,
        'type':             row['type'],
        'scheduled_at':     row['scheduled_at'].isoformat() if row['scheduled_at'] else None,
        'story_id':         row['story_id'],
        'has_video_data':   bool(row['has_video_data']),
        'movie_id':         row['movie_id'],
        'text_model_name':  row['text_model_name'],
        'video_model_name': row['video_model_name'],
        'title':            row['title'],
        'entries': [
            {
                'message':    r[0],
                'level':      r[1],
                'created_at': r[2].isoformat() if r[2] else None,
                'channel':   r[3],
            }
            for r in entries
        ],
    }


def _build_stories_used_expr() -> str:
    return """EXISTS (
                    SELECT 1 FROM movies m
                    WHERE m.story_id = s.id
                      AND m.grade = 'good'
                )"""


def _build_stories_where(used_expr: str, show_used: bool, show_bad: bool, for_approval: bool, only_pinned: bool, only_bad: bool, pin_id=None):
    filter_conditions = []
    if for_approval:
        filter_conditions.append("s.grade IS NULL")
        filter_conditions.append(f"NOT ({used_expr})")
    else:
        if not show_used:
            filter_conditions.append(f"NOT ({used_expr})")
        if only_bad and not show_bad:
            filter_conditions.append("(s.grade = 'good' OR s.grade = 'bad')")
        elif only_bad:
            filter_conditions.append("s.grade = 'bad'")
        elif not show_bad:
            filter_conditions.append("s.grade = 'good'")
        if only_pinned:
            filter_conditions.append("s.pinned = TRUE")
    params = []
    if pin_id and filter_conditions:
        base_cond = " AND ".join(filter_conditions)
        where_clause = f"WHERE (s.id::text = %s) OR ({base_cond})"
        params = [str(pin_id)]
    elif filter_conditions:
        where_clause = "WHERE " + " AND ".join(filter_conditions)
    else:
        where_clause = ""
    return where_clause, params


def db_get_story_ids_by_filter(show_used=True, show_bad=True, for_approval=False, only_pinned=False, only_bad=False) -> list:
    used_expr = _build_stories_used_expr()
    where_clause, params = _build_stories_where(used_expr, show_used, show_bad, for_approval, only_pinned, only_bad)
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT s.id::text FROM stories s {where_clause} ORDER BY s.created_at DESC, s.id DESC",
                params or None,
            )
            return [row[0] for row in cur.fetchall()]


def db_get_stories_list(show_used=True, show_bad=True, for_approval=False, pin_id=None, only_pinned: bool = False, only_bad: bool = False):
    from common.statuses import FINAL_BATCH_STATUSES
    final_statuses_sql = ', '.join(f"'{s}'" for s in FINAL_BATCH_STATUSES)
    used_expr = _build_stories_used_expr()
    where_clause, params = _build_stories_where(used_expr, show_used, show_bad, for_approval, only_pinned, only_bad, pin_id)
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
                    {used_expr} AS used,
                    am.name AS model_name,
                    s.pinned,
                    EXISTS (
                        SELECT 1 FROM batches b
                        WHERE b.story_id = s.id
                          AND b.status NOT IN ({final_statuses_sql})
                    ) AS has_active_batch,
                    EXISTS (
                        SELECT 1 FROM movies m
                        WHERE m.story_id = s.id
                    ) AS has_movie
                FROM stories s
                LEFT JOIN ai_models am ON am.id = s.model_id
                {where_clause}
                ORDER BY s.created_at DESC, s.id DESC
            """, params or None)
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
            "pinned": bool(row[8]),
            "has_active_batch": bool(row[9]),
            "has_movie": bool(row[10]),
        }
        for row in rows
    ]


_MOVIES_PUBLISHED_CHECK = """EXISTS (
        SELECT 1 FROM batches b2
        WHERE b2.movie_id = m.id
          AND (b2.status IN ('published', 'published_partially')
               OR b2.status LIKE '%%.published')
    )"""


def _build_movies_where(show_published: bool, show_bad: bool, for_approval: bool, pin_id=None):
    filter_conditions = []
    if for_approval:
        filter_conditions.append("m.grade IS NULL")
        filter_conditions.append(f"NOT ({_MOVIES_PUBLISHED_CHECK})")
    else:
        if not show_published:
            filter_conditions.append(f"NOT ({_MOVIES_PUBLISHED_CHECK})")
        if not show_bad:
            filter_conditions.append("m.grade = 'good'")
    params = []
    if pin_id and filter_conditions:
        base_cond = " AND ".join(filter_conditions)
        where_clause = f"WHERE (m.id::text = %s) OR ({base_cond})"
        params = [str(pin_id)]
    elif filter_conditions:
        where_clause = "WHERE " + " AND ".join(filter_conditions)
    else:
        where_clause = ""
    return where_clause, params


def db_get_movie_ids_by_filter(show_published=True, show_bad=True, for_approval=False) -> list:
    where_clause, params = _build_movies_where(show_published, show_bad, for_approval)
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT m.id::text FROM movies m {where_clause} ORDER BY m.created_at DESC, m.id DESC",
                params or None,
            )
            return [row[0] for row in cur.fetchall()]


def db_get_movies_list(show_published=True, show_bad=True, for_approval=False, pin_id=None):
    where_clause, params = _build_movies_where(show_published, show_bad, for_approval, pin_id)
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT
                    m.id::text,
                    m.grade,
                    m.created_at,
                    s.title AS story_title,
                    vm.name AS model_name,
                    (p.movie_id IS NOT NULL) AS published,
                    ab.id::text AS active_batch_id,
                    s.id::text AS story_id
                FROM movies m
                LEFT JOIN ai_models vm ON vm.id = m.model_id
                LEFT JOIN stories s ON s.id = m.story_id
                LEFT JOIN LATERAL (
                    SELECT bp.movie_id
                    FROM batches bp
                    WHERE bp.movie_id = m.id AND bp.status LIKE '%%published%%'
                    LIMIT 1
                ) p ON TRUE
                LEFT JOIN LATERAL (
                    SELECT b2.id
                    FROM batches b2
                    WHERE b2.story_id = m.story_id
                      AND b2.type = 'movie'
                      AND b2.status IN (
                          'pending', 'generating', 'generated'
                      )
                    ORDER BY b2.created_at DESC, b2.id DESC
                    LIMIT 1
                ) ab ON TRUE
                {where_clause}
                ORDER BY m.created_at DESC, m.id DESC
            """, params or None)
            rows = cur.fetchall()
    return [
        {
            "id": row[0],
            "grade": row[1],
            "created_at": row[2].isoformat() if row[2] else None,
            "story_title": row[3] or "",
            "model_name": row[4] or "",
            "published": bool(row[5]),
            "active_batch_id": row[6],
            "story_id": row[7],
        }
        for row in rows
    ]


def db_get_stories_pool() -> list:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT id::text, title, content
                FROM stories
                WHERE grade = 'good'
                  AND NOT EXISTS (
                      SELECT 1 FROM movies m
                      WHERE m.story_id = stories.id
                        AND m.grade = 'good'
                  )
                ORDER BY created_at DESC, id DESC
            """)
            rows = cur.fetchall()
    return [{"id": row[0], "title": row[1] or "", "content": row[2] or ""} for row in rows]


def db_count_good_pool() -> int:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT COUNT(*) FROM stories
                WHERE grade = 'good'
                  AND
                  NOT EXISTS (
                      SELECT 1 FROM movies m
                      WHERE m.story_id = stories.id
                        AND m.grade = 'good'
                  )
            """)
            row = cur.fetchone()
    return row[0] if row else 0


def db_get_models(model_type: str):
    with get_db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT m.id, m.name, m.url, m.body, m."order", m.active,
                       m.grade, m.price, m.note, p.name AS platform_name
                FROM ai_models m
                LEFT JOIN ai_platforms p ON p.id = m.platform_id
                WHERE m.type = %s
                ORDER BY m."order" ASC
            """, (model_type,))
            rows = cur.fetchall()
            models = [dict(r) for r in rows]

            if model_type == 'text-to-video' and models:
                model_ids = [str(m['id']) for m in models]
                cur.execute("""
                    SELECT model_id::text, ARRAY_AGG(duration ORDER BY duration ASC) AS durations
                    FROM model_durations
                    WHERE model_id::text = ANY(%s)
                    GROUP BY model_id
                """, (model_ids,))
                durations_map = {row['model_id']: list(row['durations']) for row in cur.fetchall()}
                for m in models:
                    m['allowed_durations'] = durations_map.get(str(m['id']), [0])
            else:
                for m in models:
                    m['allowed_durations'] = [0]

    return models


def db_get_role_modules():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT slug, module FROM user_roles")
            return {row[0]: row[1] for row in cur.fetchall()}


def db_get_user_by_login(login):
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
