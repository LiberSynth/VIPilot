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
                        ('notify_email', ''),
                        ('notify_phone', ''),
                        ('vk_publish_story', '1'),
                        ('vk_publish_wall', '1'),
                        ('video_duration', '6'),
                        ('buffer_hours', '24'),
                        ('loop_interval', '5')
                    ON CONFLICT (key) DO NOTHING
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS schedule (
                        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        time_utc VARCHAR(5) NOT NULL
                    )
                """)
                cur.execute("SELECT COUNT(*) FROM schedule")
                if cur.fetchone()[0] == 0:
                    cur.execute("INSERT INTO schedule (time_utc) VALUES ('03:00')")

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS video_urls (
                        id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        url        TEXT NOT NULL UNIQUE,
                        time_point FLOAT NOT NULL
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
                    CREATE TABLE IF NOT EXISTS ai_models (
                        id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        name           VARCHAR(200) NOT NULL,
                        url            VARCHAR(200) NOT NULL,
                        body           JSONB NOT NULL DEFAULT '{}',
                        "order"        INTEGER NOT NULL DEFAULT 0,
                        active         BOOLEAN NOT NULL DEFAULT FALSE,
                        ai_platform_id UUID REFERENCES ai_platforms(id),
                        platform_id    UUID REFERENCES ai_platforms(id),
                        type           VARCHAR(50) NOT NULL,
                        time_point     TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS targets (
                        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        name VARCHAR(200) NOT NULL,
                        aspect_ratio_x SMALLINT NOT NULL DEFAULT 9,
                        aspect_ratio_y SMALLINT NOT NULL DEFAULT 16,
                        active BOOLEAN NOT NULL DEFAULT TRUE
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS stories (
                        id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        time_point TIMESTAMPTZ NOT NULL DEFAULT now(),
                        result     TEXT NOT NULL,
                        model_id   UUID REFERENCES ai_models(id)
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS batches (
                        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        scheduled_at TIMESTAMPTZ NOT NULL,
                        target_id UUID NOT NULL REFERENCES targets(id),
                        status VARCHAR(30) NOT NULL DEFAULT 'pending',
                        story_id UUID REFERENCES stories(id),
                        video_url TEXT,
                        video_file TEXT,
                        data JSONB,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        completed_at TIMESTAMPTZ
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS log (
                        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        batch_id UUID REFERENCES batches(id),
                        pipeline VARCHAR(30) NOT NULL,
                        message TEXT,
                        status VARCHAR(20),
                        time_point TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS log_entries (
                        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        log_id UUID NOT NULL REFERENCES log(id),
                        message TEXT NOT NULL,
                        level VARCHAR(10) NOT NULL DEFAULT 'info',
                        time_point TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                """)

            conn.commit()
        print("[DB] Инициализация выполнена")
    except Exception as e:
        print(f"[DB] Ошибка инициализации: {e}")
