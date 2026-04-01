import os
import json
import psycopg2


def get_db():
    return psycopg2.connect(os.environ["DATABASE_URL"])


def init_db():
    _fal_body = json.dumps({"prompt": "{}", "duration": "{:d}s", "aspect_ratio": "{:d}:{:d}"})
    _kling_body = json.dumps({"prompt": "{}", "duration": "{:d}", "aspect_ratio": "{:d}:{:d}"})
    _sora_body = json.dumps({"prompt": "{}", "duration": "{int}", "aspect_ratio": "{:d}:{:d}"})
    _text_body = json.dumps({
        "messages": [
            {"role": "system", "content": "{}"},
            {"role": "user", "content": "{}"},
        ],
        "temperature": 0.9,
        "max_tokens": 300,
    })

    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS settings (
                        key VARCHAR(100) PRIMARY KEY,
                        value TEXT NOT NULL
                    )
                """)
                cur.execute("""
                    INSERT INTO settings (key, value) VALUES
                        ('metaprompt', ''),
                        ('system_prompt', ''),
                        ('publish_time', '03:00'),
                        ('lead_time_mins', '60'),
                        ('notify_email', ''),
                        ('notify_phone', ''),
                        ('vk_publish_story', '1'),
                        ('vk_publish_wall', '1'),
                        ('aspect_ratio_x', '9'),
                        ('aspect_ratio_y', '16'),
                        ('video_duration', '6')
                    ON CONFLICT (key) DO NOTHING
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS publish_times (
                        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        time_utc VARCHAR(5) NOT NULL
                    )
                """)
                cur.execute("SELECT COUNT(*) FROM publish_times")
                if cur.fetchone()[0] == 0:
                    cur.execute("SELECT value FROM settings WHERE key = 'publish_time'")
                    _pt_row = cur.fetchone()
                    _default_pt = _pt_row[0] if _pt_row else "03:00"
                    cur.execute("INSERT INTO publish_times (time_utc) VALUES (%s)", (_default_pt,))

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS video_urls (
                        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        url TEXT NOT NULL UNIQUE,
                        created_at FLOAT NOT NULL
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS generated_stories (
                        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        created_at FLOAT NOT NULL,
                        model_id UUID NOT NULL REFERENCES models(id),
                        result TEXT NOT NULL
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS cycles (
                        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        started TEXT NOT NULL,
                        started_ts FLOAT NOT NULL,
                        status TEXT NOT NULL,
                        entries JSONB NOT NULL DEFAULT '[]',
                        summary JSONB NOT NULL DEFAULT '{}'
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS ai_platforms (
                        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        name VARCHAR(200) NOT NULL,
                        url VARCHAR(500) NOT NULL
                    )
                """)
                cur.execute("""
                    INSERT INTO ai_platforms (name, url)
                    SELECT * FROM (VALUES
                        ('OpenRouter', 'https://openrouter.ai/api/v1/chat/completions'),
                        ('fal', 'https://queue.fal.run/fal-ai')
                    ) AS v(name, url)
                    WHERE NOT EXISTS (SELECT 1 FROM ai_platforms WHERE name = v.name)
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS models (
                        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        name VARCHAR(200) NOT NULL,
                        url VARCHAR(500) NOT NULL,
                        body JSONB NOT NULL DEFAULT '{}',
                        "order" INTEGER NOT NULL DEFAULT 0,
                        active BOOLEAN NOT NULL DEFAULT FALSE,
                        type SMALLINT NOT NULL DEFAULT 0,
                        ai_platform_id UUID REFERENCES ai_platforms(id)
                    )
                """)
                cur.execute("SELECT COUNT(*) FROM models WHERE type = 0")
                if cur.fetchone()[0] == 0:
                    cur.executemany(
                        'INSERT INTO models (name, url, body, "order", active, type, ai_platform_id) '
                        "VALUES (%s, %s, %s::jsonb, %s, %s, 0, "
                        "(SELECT id FROM ai_platforms WHERE name = 'fal'))",
                        [
                            ("veo2", "veo2", _fal_body, 1, True),
                            ("minimax/video-01", "minimax/video-01", _fal_body, 2, False),
                            ("kling-video/v1.6/standard", "kling-video/v1.6/standard/text-to-video", _kling_body, 3, False),
                            ("sora-2", "sora-2/text-to-video", _sora_body, 4, False),
                        ],
                    )

                for _name, _url, _order, _active in [
                    ("qwen3.6-plus-preview", "qwen/qwen3.6-plus-preview:free", 1, True),
                    ("llama-3.1-8b-instruct", "meta-llama/llama-3.1-8b-instruct:free", 2, False),
                    ("mistral-7b-instruct", "mistralai/mistral-7b-instruct:free", 3, False),
                ]:
                    cur.execute("SELECT COUNT(*) FROM models WHERE name = %s", (_name,))
                    if cur.fetchone()[0] == 0:
                        cur.execute(
                            'INSERT INTO models (name, url, body, "order", active, type, ai_platform_id) '
                            "VALUES (%s, %s, %s::jsonb, %s, %s, 1, "
                            "(SELECT id FROM ai_platforms WHERE name = 'OpenRouter'))",
                            (_name, _url, _text_body, _order, _active),
                        )

            conn.commit()
        print("[DB] Инициализация выполнена")
    except Exception as e:
        print(f"[DB] Ошибка инициализации: {e}")
