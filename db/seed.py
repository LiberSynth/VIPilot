"""
Начальные данные: вставляются один раз при первом запуске (ON CONFLICT DO NOTHING).
Не содержит DDL — только данные, которые могут быть изменены пользователем.
Модели ИИ управляются через db/migrations.py (_m004_seed_ai_models).
"""

from .connection import get_db
from log.log import write_log_entry


def seed_db():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO environment (key, value)
                    VALUES
                        ('workflow_state', 'running'),
                        ('deep_debugging', '0'),
                        ('producer_autoplay_movie', '0')
                    ON CONFLICT (key) DO NOTHING
                """)

                cur.execute("""
                    INSERT INTO settings (key, value) VALUES
                        ('text_prompt',        ''),
                        ('format_prompt',     ''),
                        ('notify_email',      ''),
                        ('notify_phone',      ''),
                        ('video_duration',    '6'),
                        ('buffer_hours',      '24'),
                        ('loop_interval',     '15'),
                        ('max_batch_threads', '5'),
                        ('max_model_passes',  '5'),
                        ('approve_stories',   '0')
                    ON CONFLICT (key) DO NOTHING
                """)

                cur.execute("""
                    INSERT INTO ai_platforms (name, url)
                    SELECT v.name, v.url FROM (VALUES
                        ('OpenRouter', 'https://openrouter.ai/api/v1/chat/completions'),
                        ('fal.ai',     'https://queue.fal.run/fal-ai')
                    ) AS v(name, url)
                    WHERE NOT EXISTS (
                        SELECT 1 FROM ai_platforms WHERE name = v.name
                    )
                """)

                cur.execute("""
                    INSERT INTO targets (name, aspect_ratio_x, aspect_ratio_y, active)
                    SELECT * FROM (VALUES
                        ('VKontakte', 9::SMALLINT,  16::SMALLINT, TRUE),
                        ('Дзен',     16::SMALLINT,  9::SMALLINT,  FALSE)
                    ) AS v(name, aspect_ratio_x, aspect_ratio_y, active)
                    WHERE NOT EXISTS (
                        SELECT 1 FROM targets WHERE name = v.name
                    )
                """)

                cur.execute("""
                    INSERT INTO user_roles (name, slug, module)
                    SELECT v.name, v.slug, v.module FROM (VALUES
                        ('root',     'root',     'ROOT'),
                        ('producer', 'producer', 'PRODUCTION'),
                        ('operator', 'operator', 'OPERATOR')
                    ) AS v(name, slug, module)
                    WHERE NOT EXISTS (
                        SELECT 1 FROM user_roles WHERE slug = v.slug
                    )
                """)

                cur.execute("""
                    INSERT INTO users (name, login, password)
                    SELECT v.name, v.login, v.password
                    FROM (VALUES
                        ('root',     'root',     '0000'),
                        ('producer', 'producer', '0000'),
                        ('operator', 'operator', '0000')
                    ) AS v(name, login, password)
                    WHERE NOT EXISTS (
                        SELECT 1 FROM users WHERE login = v.login
                    )
                """)

                cur.execute("""
                    INSERT INTO user_role_links (user_id, role_id)
                    SELECT u.id, r.id
                    FROM (VALUES
                        ('operator', 'operator'),
                        ('producer', 'producer'),
                        ('producer', 'operator'),
                        ('root',     'root'),
                        ('root',     'producer'),
                        ('root',     'operator')
                    ) AS v(user_login, role_slug)
                    JOIN users      u ON u.login = v.user_login
                    JOIN user_roles r ON r.slug  = v.role_slug
                    ON CONFLICT DO NOTHING
                """)

            conn.commit()
        write_log_entry(None, "[DB] Данные инициализированы", level='silent')
    except Exception as e:
        write_log_entry(None, f"[DB] Ошибка seed_db: {e}", level='silent')
        raise
