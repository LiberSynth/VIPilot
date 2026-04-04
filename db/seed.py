"""
Начальные данные: вставляются один раз при первом запуске (ON CONFLICT DO NOTHING).
Не содержит DDL — только данные, которые могут быть изменены пользователем.
"""

import json
from .connection import get_db


def seed_db():
    _text_body = json.dumps({
        "messages": [
            {"role": "system", "content": "{}"},
            {"role": "user",   "content": "{}"},
        ],
        "temperature": 0.9,
        "max_tokens":  300,
    })

    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO environment (key, value)
                    VALUES ('workflow_state', 'running')
                    ON CONFLICT (key) DO NOTHING
                """)

                cur.execute("""
                    INSERT INTO settings (key, value) VALUES
                        ('metaprompt',        ''),
                        ('system_prompt',     ''),
                        ('notify_email',      ''),
                        ('notify_phone',      ''),
                        ('vk_publish_story',  '1'),
                        ('vk_publish_wall',   '1'),
                        ('video_duration',    '6'),
                        ('buffer_hours',      '24'),
                        ('loop_interval',     '5')
                    ON CONFLICT (key) DO NOTHING
                """)

                cur.execute("""
                    INSERT INTO ai_platforms (name, url)
                    SELECT * FROM (VALUES
                        ('OpenRouter', 'https://openrouter.ai/api/v1/chat/completions'),
                        ('fal',        'https://queue.fal.run/fal-ai')
                    ) AS v(name, url)
                    WHERE NOT EXISTS (
                        SELECT 1 FROM ai_platforms WHERE name = v.name
                    )
                """)

                cur.execute("""
                    INSERT INTO ai_models
                        (name, url, body, "order", active, ai_platform_id, platform_id, type)
                    SELECT
                        v.name, v.url, v.body::jsonb,
                        v.ord, v.active, p.id, p.id, v.type
                    FROM (VALUES
                        ('sora-2',
                         'sora-2/text-to-video',
                         '{"prompt":"{}","duration":"{int}","aspect_ratio":"{:d}:{:d}"}',
                         1, TRUE, 'fal', 'text-to-video'),
                        ('veo2',
                         'veo2',
                         '{"prompt":"{}","duration":"{:d}s","aspect_ratio":"{:d}:{:d}"}',
                         2, FALSE, 'fal', 'text-to-video'),
                        ('minimax/video-01',
                         'minimax/video-01',
                         '{"prompt":"{}","duration":"{:d}s","aspect_ratio":"{:d}:{:d}"}',
                         3, FALSE, 'fal', 'text-to-video'),
                        ('kling-video/v1.6/standard',
                         'kling-video/v1.6/standard/text-to-video',
                         '{"prompt":"{}","duration":"{:d}","aspect_ratio":"{:d}:{:d}"}',
                         4, FALSE, 'fal', 'text-to-video'),
                        ('openrouter/free',
                         'openrouter/free',
                         %s,
                         1, TRUE, 'OpenRouter', 'text'),
                        ('mistral-7b-instruct',
                         'mistralai/mistral-7b-instruct:free',
                         %s,
                         2, TRUE, 'OpenRouter', 'text'),
                        ('llama-3.1-8b-instruct',
                         'meta-llama/llama-3.1-8b-instruct:free',
                         %s,
                         3, TRUE, 'OpenRouter', 'text')
                    ) AS v(name, url, body, ord, active, platform_name, type)
                    JOIN ai_platforms p ON p.name = v.platform_name
                    WHERE NOT EXISTS (
                        SELECT 1 FROM ai_models WHERE name = v.name
                    )
                """, (_text_body, _text_body, _text_body))

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

            conn.commit()
        print("[DB] Данные инициализированы")
    except Exception as e:
        print(f"[DB] Ошибка seed_db: {e}")
