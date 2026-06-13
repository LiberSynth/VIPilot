"""
Начальные данные: вставляются один раз при первом запуске.
Только users-семейство: user_roles, users, user_role_links.
Вызывается из check_upgrade() только если build_number отсутствует в environment.
"""

from .connection import get_db
from log.log import write_log_entry

def seed_db():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO user_roles (name, slug, module) VALUES
                        ('root',     'root',     'ROOT'),
                        ('producer', 'producer', 'PRODUCTION'),
                        ('operator', 'operator', 'OPERATOR')
                """)

                cur.execute("""
                    INSERT INTO users (name, login, password) VALUES
                        ('root',     'root',     '0000'),
                        ('producer', 'producer', '0000'),
                        ('operator', 'operator', '0000')
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
                """)

                cur.execute("""
                    INSERT INTO settings (key, value) VALUES ('buffer_minutes', '60')
                    ON CONFLICT (key) DO NOTHING
                """)

            conn.commit()
        write_log_entry(None, 'db', 'Данные инициализированы', level='silent')
    except Exception as e:
        write_log_entry(None, 'db', f'Ошибка seed_db: {e}', level='silent')
        raise
