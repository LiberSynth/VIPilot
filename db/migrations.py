"""
Версионированные миграции схемы БД.

ПРАВИЛА УДАЛЕНИЯ ЗАДЕПЛОЕННЫХ МИГРАЦИЙ
══════════════════════════════════════════════════════════════════════════
Миграция считается «задеплоенной», когда она подтверждённо применена на
всех окружениях (dev + prod). Убедиться можно запросом:
    SELECT value FROM settings WHERE key = 'db_version';

Чтобы безопасно удалить миграцию из кода:
  1. Проверьте, что db_version в prod >= номера удаляемой миграции.
  2. Удалите функцию и её запись из MIGRATIONS.
  3. НЕ переиспользуйте освободившийся номер версии — только вперёд.
  4. Ориентир: удалять не раньше двух недель после деплоя на prod.
     Дата деплоя указана в docstring каждой миграции (поле «Deployed:»).

Пример: _m001 задеплоена 2026-01-01, db_version в prod = 3.
  → Через две недели удаляем функцию и строку (1, _m001) из MIGRATIONS.
  → MIGRATIONS начинается с (2, _m002) — это нормально, пробел в нумерации
    не влияет на работу.
══════════════════════════════════════════════════════════════════════════
"""

from .connection import get_db
from log.log import write_log_entry


# ---------------------------------------------------------------------------
# Функции миграций
# ---------------------------------------------------------------------------

# Миграции 1–70 удалены: задеплоены на prod 2026-04-20, db_version = 70.
# Следующая миграция: _m084_...


def _m088_batches_title(cur):
    """Добавляет колонку title в таблицу batches для хранения заголовка публикации.
    Deployed: —
    """
    cur.execute("""
        ALTER TABLE batches ADD COLUMN IF NOT EXISTS title TEXT
    """)


def _m087_publication_counter(cur):
    """Добавляет начальное значение счётчика публикаций в таблицу environment.
    Deployed: —
    """
    cur.execute("""
        INSERT INTO environment (key, value)
        VALUES ('publication_counter', '0')
        ON CONFLICT (key) DO NOTHING
    """)


def _m086_user_role_links_pk(cur):
    """Заменяет составной PK (user_id, role_id) на суррогатный id UUID.
    Добавляет UNIQUE-ограничение на (user_id, role_id) и отдельные индексы.
    Deployed: —
    """
    cur.execute("""
        ALTER TABLE user_role_links ADD COLUMN IF NOT EXISTS id UUID NOT NULL DEFAULT gen_random_uuid()
    """)
    cur.execute("""
        DO $$
        DECLARE
            v_pk_name TEXT;
        BEGIN
            SELECT tc.constraint_name INTO v_pk_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON kcu.constraint_name = tc.constraint_name
               AND kcu.table_name = tc.table_name
            WHERE tc.table_name = 'user_role_links'
              AND tc.constraint_type = 'PRIMARY KEY'
              AND kcu.column_name = 'user_id';

            IF v_pk_name IS NOT NULL THEN
                EXECUTE format('ALTER TABLE user_role_links DROP CONSTRAINT %I', v_pk_name);
                ALTER TABLE user_role_links ADD PRIMARY KEY (id);
            END IF;
        END $$
    """)
    cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.table_constraints
                WHERE table_name = 'user_role_links'
                  AND constraint_type = 'UNIQUE'
                  AND constraint_name = 'user_role_links_user_id_role_id_key'
            ) THEN
                ALTER TABLE user_role_links
                    ADD CONSTRAINT user_role_links_user_id_role_id_key UNIQUE (user_id, role_id);
            END IF;
        END $$
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_user_role_links_user_id ON user_role_links (user_id)
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_user_role_links_role_id ON user_role_links (role_id)
    """)


def _m085_cycle_config_to_kv(cur):
    """Пересоздаёт cycle_config как key-value (key VARCHAR PK, value TEXT).
    Переносит все 7 значений из старых типизированных колонок.
    Deployed: —
    """
    cur.execute("""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'cycle_config' AND column_name = 'id'
            ) THEN
                CREATE TABLE cycle_config_kv (
                    key   VARCHAR(100) PRIMARY KEY,
                    value TEXT NOT NULL
                );

                INSERT INTO cycle_config_kv (key, value)
                SELECT 'text_prompt', COALESCE(text_prompt, '')
                FROM cycle_config WHERE id = 1;

                INSERT INTO cycle_config_kv (key, value)
                SELECT 'format_prompt', COALESCE(format_prompt, '')
                FROM cycle_config WHERE id = 1;

                INSERT INTO cycle_config_kv (key, value)
                SELECT 'video_post_prompt', COALESCE(video_post_prompt, '')
                FROM cycle_config WHERE id = 1;

                INSERT INTO cycle_config_kv (key, value)
                SELECT 'video_duration', CAST(video_duration AS TEXT)
                FROM cycle_config WHERE id = 1;

                INSERT INTO cycle_config_kv (key, value)
                SELECT 'approve_stories', CASE WHEN approve_stories THEN '1' ELSE '0' END
                FROM cycle_config WHERE id = 1;

                INSERT INTO cycle_config_kv (key, value)
                SELECT 'approve_movies', CASE WHEN approve_movies THEN '1' ELSE '0' END
                FROM cycle_config WHERE id = 1;

                INSERT INTO cycle_config_kv (key, value)
                SELECT 'words_per_second', CAST(words_per_second AS TEXT)
                FROM cycle_config WHERE id = 1;

                DROP TABLE cycle_config;
                ALTER TABLE cycle_config_kv RENAME TO cycle_config;
            END IF;
        END $$
    """)


def _m084_add_words_per_second(cur):
    """Добавляет колонку words_per_second в таблицу cycle_config.
    Deployed: —
    """
    cur.execute("""
        ALTER TABLE cycle_config ADD COLUMN IF NOT EXISTS words_per_second NUMERIC(5,2) NOT NULL DEFAULT 8.0
    """)


def _m083_create_cycle_config(cur):
    """Создаёт таблицу cycle_config и переносит в неё 6 ключей из settings.
    Deployed: —
    """
    cur.execute("""
        CREATE TABLE IF NOT EXISTS cycle_config (
            id               INTEGER PRIMARY KEY DEFAULT 1,
            text_prompt      TEXT NOT NULL DEFAULT '',
            format_prompt    TEXT NOT NULL DEFAULT '',
            video_post_prompt TEXT NOT NULL DEFAULT '',
            video_duration   INTEGER NOT NULL DEFAULT 6,
            approve_stories  BOOLEAN NOT NULL DEFAULT FALSE,
            approve_movies   BOOLEAN NOT NULL DEFAULT FALSE,
            CONSTRAINT cycle_config_single_row CHECK (id = 1)
        )
    """)
    cur.execute("""
        INSERT INTO cycle_config (
            id, text_prompt, format_prompt, video_post_prompt,
            video_duration, approve_stories, approve_movies
        )
        SELECT
            1,
            COALESCE((SELECT value FROM settings WHERE key = 'text_prompt'), ''),
            COALESCE((SELECT value FROM settings WHERE key = 'format_prompt'), ''),
            COALESCE((SELECT value FROM settings WHERE key = 'video_post_prompt'), ''),
            COALESCE(
                NULLIF(
                    regexp_replace(
                        COALESCE((SELECT value FROM settings WHERE key = 'video_duration'), ''),
                        '[^0-9]', '', 'g'
                    ), ''
                )::INTEGER,
                6
            ),
            COALESCE((SELECT value FROM settings WHERE key = 'approve_stories'), '0') = '1',
            COALESCE((SELECT value FROM settings WHERE key = 'approve_movies'), '0') = '1'
        WHERE NOT EXISTS (SELECT 1 FROM cycle_config WHERE id = 1)
    """)
    cur.execute("""
        DELETE FROM settings
        WHERE key IN (
            'text_prompt', 'format_prompt', 'video_post_prompt',
            'video_duration', 'approve_stories', 'approve_movies'
        )
    """)


def _m082_redistribute_settings_env(cur):
    """Перемещает producer_autoplay_movie и deep_debugging из settings в environment.
    Удаляет orphan-ключи screenwriter_show_bad и screenwriter_top_quality из environment.
    Deployed: —
    """
    cur.execute("""
        INSERT INTO environment (key, value)
        SELECT key, value FROM settings
        WHERE key IN ('producer_autoplay_movie', 'deep_debugging')
        ON CONFLICT (key) DO NOTHING
    """)
    cur.execute("""
        DELETE FROM settings WHERE key IN ('producer_autoplay_movie', 'deep_debugging')
    """)
    cur.execute("""
        DELETE FROM environment WHERE key IN ('screenwriter_show_bad', 'screenwriter_top_quality')
    """)


def _m081_add_max_model_passes_setting(cur):
    """Добавляет запись max_model_passes в таблицу settings.
    Ключ объявлен в seed.py, но в БД мог отсутствовать — форма ROOT не могла его сохранить.
    Deployed: —
    """
    cur.execute("""
        INSERT INTO settings (key, value)
        VALUES ('max_model_passes', '5')
        ON CONFLICT (key) DO NOTHING
    """)


def _m080_drop_stories_top_quality(cur):
    """Удаляет колонку stories.top_quality — поле не использовалось нигде в коде.
    Deployed: —
    """
    cur.execute("ALTER TABLE stories DROP COLUMN IF EXISTS top_quality")


def _m079_created_at_clock_timestamp(cur):
    """Меняет DEFAULT created_at с now() на clock_timestamp() во всех таблицах.

    now() = TRANSACTION_TIMESTAMP() — одно значение на всю транзакцию.
    clock_timestamp() — реальное время в момент выполнения каждого INSERT.
    Это гарантирует уникальность created_at даже при пакетных вставках
    внутри одной транзакции.
    Deployed: —
    """
    tables = [
        'movies', 'batches', 'stories', 'log',
        'log_entries', 'schedule', 'ai_models',
    ]
    for table in tables:
        cur.execute(
            f"ALTER TABLE {table} ALTER COLUMN created_at"
            f" SET DEFAULT clock_timestamp()"
        )


def _m078_skyreels_durations(cur):
    """Добавляет допустимые длительности для моделей SkyReels V4 (5–15 сек)."""
    cur.execute("""
        INSERT INTO model_durations (model_id, duration)
        SELECT m.id, d.duration
        FROM ai_models m
        CROSS JOIN (VALUES (5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15)) AS d(duration)
        WHERE m.name IN ('SkyReels V4 Fast', 'SkyReels V4 Std')
        ON CONFLICT DO NOTHING
    """)


def _m077_skyreels_sound_false(cur):
    """Устанавливает sound: false в body обеих моделей SkyReels V4."""
    cur.execute("""
        UPDATE ai_models
        SET body = body || '{"sound": false}'::jsonb
        WHERE name IN ('SkyReels V4 Fast', 'SkyReels V4 Std')
    """)


def _m076_skyreels_body_update(cur):
    """Обновляет body моделей SkyReels V4: добавляет sound, prompt_optimizer, resolution, mode."""
    cur.execute("""
        UPDATE ai_models
        SET body = '{"model_name": "SkyReels-V4-Fast", "prompt": "{}", "duration": "{:int}", "aspect_ratio": "{:d}:{:d}", "resolution": "720p", "sound": true, "prompt_optimizer": true, "mode": "fast"}'::jsonb
        WHERE name = 'SkyReels V4 Fast'
    """)
    cur.execute("""
        UPDATE ai_models
        SET body = '{"model_name": "SkyReels-V4-Std", "prompt": "{}", "duration": "{:int}", "aspect_ratio": "{:d}:{:d}", "resolution": "720p", "sound": true, "prompt_optimizer": true, "mode": "std"}'::jsonb
        WHERE name = 'SkyReels V4 Std'
    """)


def _m075_add_skyreels(cur):
    """Добавляет платформу SkyReels и модели V4 Fast / V4 Std."""
    cur.execute("""
        INSERT INTO ai_platforms (name, url, env_key_name)
        SELECT 'SkyReels', 'https://api-gateway.skyreels.ai', 'SKYREELS_API_KEY'
        WHERE NOT EXISTS (SELECT 1 FROM ai_platforms WHERE name = 'SkyReels')
    """)
    cur.execute("""
        INSERT INTO ai_models (name, url, body, "order", active, grade, type, price, platform_id)
        SELECT
            m.name, m.url, m.body::jsonb, m.ord, FALSE, 'good', 'text-to-video', m.price,
            p.id
        FROM ai_platforms p
        CROSS JOIN (VALUES
            (
                'SkyReels V4 Fast',
                '/api/v1/video/text2video/submit',
                '{"model_name": "SkyReels-V4-Fast", "prompt": "{}", "duration": "{:int}", "aspect_ratio": "{:d}:{:d}"}',
                30,
                '1.100 $/10сек'
            ),
            (
                'SkyReels V4 Std',
                '/api/v1/video/text2video/submit',
                '{"model_name": "SkyReels-V4-Std", "prompt": "{}", "duration": "{:int}", "aspect_ratio": "{:d}:{:d}"}',
                31,
                '1.400 $/10сек'
            )
        ) AS m(name, url, body, ord, price)
        WHERE p.name = 'SkyReels'
        ON CONFLICT DO NOTHING
    """)


def _m074_rename_key_env(cur):
    """Переименовывает колонку key_env → env_key_name в таблице ai_platforms."""
    cur.execute("""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'ai_platforms' AND column_name = 'key_env'
            ) THEN
                ALTER TABLE ai_platforms RENAME COLUMN key_env TO env_key_name;
            END IF;
        END $$
    """)


def _m073_add_wan27(cur):
    """Добавляет модель Wan 2.7 в ai_models и её допустимые длительности."""
    cur.execute("""
        INSERT INTO ai_platforms (name, url)
        SELECT 'fal.ai', 'https://queue.fal.run/fal-ai'
        WHERE NOT EXISTS (SELECT 1 FROM ai_platforms WHERE name = 'fal.ai')
    """)
    cur.execute("""
        INSERT INTO ai_models (name, url, body, "order", active, grade, type, price, platform_id)
        SELECT
            'Wan 2.7',
            'fal-ai/wan/v2.7/text-to-video',
            '{"prompt": "{}", "duration": "{:int}", "aspect_ratio": "{:d}:{:d}", "resolution": "720p"}'::jsonb,
            17,
            TRUE,
            'good',
            'text-to-video',
            '1.000 $/10сек',
            p.id
        FROM ai_platforms p
        WHERE p.name = 'fal.ai'
        ON CONFLICT DO NOTHING
    """)
    cur.execute("""
        INSERT INTO model_durations (model_id, duration)
        SELECT m.id, d.duration
        FROM ai_models m
        CROSS JOIN (VALUES (2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15)) AS d(duration)
        WHERE m.name = 'Wan 2.7'
        ON CONFLICT DO NOTHING
    """)


def _m072_stories_pinned(cur):
    """Добавляет поле pinned в таблицу stories."""
    cur.execute("ALTER TABLE stories ADD COLUMN IF NOT EXISTS pinned BOOL NOT NULL DEFAULT FALSE")


def _m071_donor_good_only(cur):
    """
    Добавляет параметр p_good_only к функциям get_donor_count и claim_donor_batch.
    Позволяет считать и выбирать только доноров с grade = 'good'.
    Старые функции (без параметра) удаляются во избежание конфликта перегрузок.
    """
    cur.execute("DROP FUNCTION IF EXISTS get_donor_count()")
    cur.execute("""
        CREATE FUNCTION get_donor_count(p_good_only boolean)
        RETURNS bigint LANGUAGE plpgsql AS $$
        DECLARE
            v_count bigint;
        BEGIN
            PERFORM pg_advisory_xact_lock(hashtext('donor_claim'));
            SELECT COUNT(*) INTO v_count
            FROM batches b
            JOIN movies m ON m.id = b.movie_id
            WHERE (m.transcoded_data IS NOT NULL OR m.raw_data IS NOT NULL)
              AND b.status IN ('cancelled', 'movie_probe')
              AND (NOT p_good_only OR m.grade = 'good');
            RETURN v_count;
        END;
        $$
    """)
    cur.execute("DROP FUNCTION IF EXISTS claim_donor_batch(uuid)")
    cur.execute("""
        CREATE FUNCTION claim_donor_batch(p_batch_id uuid, p_good_only boolean)
        RETURNS void LANGUAGE plpgsql AS $$
        DECLARE
            v_donor_id uuid;
            v_donated_status text := 'donated';
        BEGIN
            PERFORM pg_advisory_xact_lock(hashtext('donor_claim'));
            SELECT b.id INTO v_donor_id
            FROM batches b
            JOIN movies m ON m.id = b.movie_id
            WHERE (m.transcoded_data IS NOT NULL OR m.raw_data IS NOT NULL)
              AND b.status IN ('cancelled', 'movie_probe')
              AND (NOT p_good_only OR m.grade = 'good')
            ORDER BY b.created_at ASC, b.id ASC
            LIMIT 1 FOR UPDATE OF b;
            IF v_donor_id IS NULL THEN RETURN; END IF;
            UPDATE batches SET status = v_donated_status WHERE id = v_donor_id;
            UPDATE batches
            SET data = COALESCE(data, '{}'::jsonb) || jsonb_build_object('donor_batch_id', v_donor_id::text)
            WHERE id = p_batch_id;
        END;
        $$
    """)


# ---------------------------------------------------------------------------
# Реестр миграций — добавляйте только в конец, никогда не переиспользуйте номера
# ---------------------------------------------------------------------------

def _m089_replace_ai_models(cur):
    """Полная замена ai_models и model_durations: удалить всё, залить заново все поля включая id.
    Deployed: —
    """
    import json

    cur.execute("DELETE FROM model_durations")
    cur.execute("DELETE FROM ai_models")

    _ins_m = (
        'INSERT INTO ai_models'
        ' (id, platform_id, name, url, body, type, price, "order", active, grade, note)'
        ' VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s)'
        ' ON CONFLICT (id) DO UPDATE SET'
        '  platform_id=EXCLUDED.platform_id, name=EXCLUDED.name, url=EXCLUDED.url,'
        '  body=EXCLUDED.body, type=EXCLUDED.type, price=EXCLUDED.price,'
        '  "order"=EXCLUDED."order", active=EXCLUDED.active,'
        '  grade=EXCLUDED.grade, note=EXCLUDED.note'
    )
    _ins_d = (
        'INSERT INTO model_durations (id, model_id, duration)'
        ' VALUES (%s, %s, %s)'
        ' ON CONFLICT (id) DO NOTHING'
    )

    _SU  = {"messages": [{"role": "system", "content": "{}"}, {"role": "user", "content": "{}"}], "max_tokens": 300, "temperature": 0.9}
    _U   = {"messages": [{"role": "user",   "content": "{}"}], "max_tokens": 300, "temperature": 0.9}
    _DS  = {"top_p": 0.9, "messages": [{"role": "system", "content": "{}"}, {"role": "user", "content": "{}"}], "max_tokens": 300, "temperature": 0.85, "presence_penalty": 0.5, "frequency_penalty": 0.3}
    _VK3 = {"prompt": "{}", "duration": "{:d}", "aspect_ratio": "{:d}:{:d}"}
    _W27 = {"prompt": "{}", "duration": "{:int}", "resolution": "720p", "aspect_ratio": "{:d}:{:d}"}
    _SD2 = {"prompt": "{}", "duration": "{}", "resolution": "720p", "aspect_ratio": "{0}:{1}"}
    _VEO = {"prompt": "{}", "duration": "{:d}s", "resolution": "1080p", "aspect_ratio": "{:d}:{:d}", "generate_audio": True}
    _SKY = lambda mode, name: {"mode": mode, "sound": False, "prompt": "{}", "duration": "{:int}", "model_name": name, "resolution": "720p", "aspect_ratio": "{:d}:{:d}", "prompt_optimizer": True}

    _MODELS = [
        ('6f908a00-5251-4471-8793-751cbc3fe11e','a19ca46d-0ddf-4735-b400-0c8c18ff5485','Grok Video','videos/generations',{"fps": 24, "model": "grok-imagine-video", "prompt": "{}", "duration": "{:int}", "resolution": "720p", "aspect_ratio": "{:d}:{:d}"},'text-to-video','0.700 $/10сек',1,True,'good','Качество 21%.\nЕсть возможность задавать seed (int, для воспроизводимости результата) и negative_prompt (str, описание нежелательных элементов в кадре).'),
        ('167a3dd2-b2e9-43b4-8e08-2b4df13ba7b5','8be57991-bf7a-4594-b436-a0805e163256','deepseek-chat','deepseek-chat',_DS,'text',None,1,True,'good',None),
        ('b3c5e604-99f0-4669-8e31-b38cf48dda68','1b696238-1fdd-4b95-bff0-86a37be13c78','Qwen3.6 Plus','qwen/qwen3.6-plus',_SU,'text',None,2,True,'good',None),
        ('8c23daf4-1a70-4e13-ada3-4f7d0f1822ef','0c8d1e1c-fe65-45d3-a1c3-be69e7941e17','Sora 2','fal-ai/sora-2/text-to-video',{"prompt": "{}", "duration": "{:int}", "aspect_ratio": "{:d}:{:d}"},'text-to-video','1.000 $/10сек',2,True,'good','Качество 50%.'),
        ('d3e4f5a6-b7c8-4d91-ae2f-1b2c3d4e5f6a','0c8d1e1c-fe65-45d3-a1c3-be69e7941e17','LTX Video 2.3','fal-ai/ltx-2.3/text-to-video',{"prompt": "{}", "duration": "{:int}", "resolution": "1080p", "aspect_ratio": "{}:{}"},'text-to-video','0.800 $/10сек',3,True,'good','Качество 33%\nХорошо получаются русские.'),
        ('e0a39f39-2dec-4673-abb9-c0fdf575481e','1b696238-1fdd-4b95-bff0-86a37be13c78','gpt-oss-120b','openai/gpt-oss-120b',_SU,'text',None,3,False,'good',None),
        ('4571d5cb-487e-4f73-8a13-87dae6d684c6','1b696238-1fdd-4b95-bff0-86a37be13c78','gpt-oss-20b','openai/gpt-oss-20b',_SU,'text',None,4,False,'rejected',None),
        ('4ab3ffe0-2eee-43e5-9d4d-77a576a6e17c','0c8d1e1c-fe65-45d3-a1c3-be69e7941e17','Wan 2.7','fal-ai/wan/v2.7/text-to-video',_W27,'text-to-video','1.000 $/10сек',4,True,'good','Качество 42%.'),
        ('aacc47c3-a8c6-41ce-9237-79373d9712d8','0c8d1e1c-fe65-45d3-a1c3-be69e7941e17','Kling Video v3.0 Standard','fal-ai/kling-video/v3/standard/text-to-video',_VK3,'text-to-video','1.260 $/10сек',5,True,'good','Качество 20%.'),
        ('21b6f2c6-c01c-4dfa-9af6-41d2be962400','1b696238-1fdd-4b95-bff0-86a37be13c78','Gemma 3 27B IT','google/gemma-3-27b-it',_U,'text',None,5,False,'good',None),
        ('28d29f45-151a-4836-aff8-53ed29c950f0','0c8d1e1c-fe65-45d3-a1c3-be69e7941e17','Kling Video v3.0 Pro','fal-ai/kling-video/v3/pro/text-to-video',_VK3,'text-to-video','1.680 $/10сек',6,False,'good',None),
        ('b4a32f01-04f1-4771-aab4-342b97979663','1b696238-1fdd-4b95-bff0-86a37be13c78','Gemma 3n E2B IT','google/gemma-3n-e2b-it:free',_U,'text',None,6,False,'good',None),
        ('f3c864fe-959f-4997-9685-9e4b6d868a60','1b696238-1fdd-4b95-bff0-86a37be13c78','Gemma 3 12B IT','google/gemma-3-12b-it',_U,'text',None,7,False,'good',None),
        ('0b497f50-6509-456f-82bc-f8b77d41026a','0c8d1e1c-fe65-45d3-a1c3-be69e7941e17','Veo 3.1 Fast','fal-ai/veo3.1/fast',_VEO,'text-to-video','3.075 $/10сек',7,False,'good',None),
        ('0378c0c8-da94-4f8d-8a72-a417223b0fb0','0c8d1e1c-fe65-45d3-a1c3-be69e7941e17','Veo 3.1','fal-ai/veo3.1',_VEO,'text-to-video','4.000 $/10сек',8,False,'good',None),
        ('414a0e24-7c07-4857-924f-3f5fb1aac549','1b696238-1fdd-4b95-bff0-86a37be13c78','Gemma 3n E4B IT','google/gemma-3n-e4b-it',_U,'text',None,8,False,'poor',None),
        ('9829c9d4-ed6c-4f8d-b562-b430a3c9de80','1b696238-1fdd-4b95-bff0-86a37be13c78','Gemma 3 4B IT','google/gemma-3-4b-it',_U,'text',None,9,False,'poor',None),
        ('f0293755-8bf2-4654-8d7c-eb08dbd919c6','0c8d1e1c-fe65-45d3-a1c3-be69e7941e17','Veo 2','fal-ai/veo2',{"prompt": "{}", "duration": "{:d}s", "aspect_ratio": "{:d}:{:d}"},'text-to-video','5.000 $/10сек',9,False,'good',None),
        ('ce3dbfd4-efed-479d-9c0a-d02b9923b32a','1b696238-1fdd-4b95-bff0-86a37be13c78','Llama 3.3 70B Instruct','meta-llama/llama-3.3-70b-instruct',_SU,'text',None,10,False,'good',None),
        ('4bc53f61-6e14-4951-a8d8-3bfac31d7dbd','0c8d1e1c-fe65-45d3-a1c3-be69e7941e17','Seedance 2.0','bytedance/seedance-2.0/text-to-video',_SD2,'text-to-video','3.040 $/10сек',10,False,'good',None),
        ('8ed3d2a3-4dc3-4720-a585-09fd1438fea6','1b696238-1fdd-4b95-bff0-86a37be13c78','Llama 3.2 3B Instruct','meta-llama/llama-3.2-3b-instruct',_SU,'text',None,11,False,'good',None),
        ('4f59c728-a7f1-45f7-b9d0-8b8657c7db15','0c8d1e1c-fe65-45d3-a1c3-be69e7941e17','Seedance 2.0 Fast','bytedance/seedance-2.0/fast/text-to-video',_SD2,'text-to-video','2.430 $/10сек',11,False,'good',None),
        ('b5e3f891-2c4a-4d67-9e8f-1a2b3c4d5e6f','0c8d1e1c-fe65-45d3-a1c3-be69e7941e17','Seedance v1.5 Pro','fal-ai/bytedance/seedance/v1.5/pro/text-to-video',_SD2,'text-to-video','0.520 $/10сек',12,False,'poor',None),
        ('f692a8c7-e6c0-4ddf-a52d-8977f10b7e9c','1b696238-1fdd-4b95-bff0-86a37be13c78','Llama 3.1 8B Instruct','meta-llama/llama-3.1-8b-instruct',_SU,'text',None,12,False,'poor',None),
        ('7997a651-3522-4bb4-b057-3e3ccef47f81','1b696238-1fdd-4b95-bff0-86a37be13c78','Hermes 3 405B Instruct','nousresearch/hermes-3-llama-3.1-405b',_SU,'text',None,13,False,'good',None),
        ('08bc220d-a490-4f18-ba93-2ce55ce88ee0','0c8d1e1c-fe65-45d3-a1c3-be69e7941e17','PixVerse C1','fal-ai/pixverse/c1/text-to-video',{"prompt": "{}", "duration": "{:int}", "resolution": "1080p", "aspect_ratio": "{:d}:{:d}", "generate_audio": True},'text-to-video','0.950 $/10сек',13,False,'poor','Без звука.'),
        ('d55d6064-d716-4efa-8b2e-f205d01222af','1b696238-1fdd-4b95-bff0-86a37be13c78','openrouter/free','openrouter/free',_SU,'text',None,14,True,'fallback',None),
        ('31d3c708-acd9-4a90-9efb-909219814b30','0c8d1e1c-fe65-45d3-a1c3-be69e7941e17','Kling Video v1.6 Standard','fal-ai/kling-video/v1.6/standard/text-to-video',_VK3,'text-to-video','0.450 $/10сек',14,False,'poor',None),
        ('2b0ef68a-52e8-4d37-8b29-716c4fb7d0dc','0c8d1e1c-fe65-45d3-a1c3-be69e7941e17','MiniMax Video-01','fal-ai/minimax/video-01',{"prompt": "{}", "duration": "{:d}s", "aspect_ratio": "{:d}:{:d}"},'text-to-video','0.830 $/10сек',15,False,'poor',None),
        ('b9fcf3fb-56cc-4f28-be56-0953768c68b7','1b696238-1fdd-4b95-bff0-86a37be13c78','Qwen3 Next 80B A3B Instruct','qwen/qwen3-next-80b-a3b-instruct',_SU,'text',None,15,False,'poor',None),
        ('f8419279-f7f4-43cf-816a-36ef57f3222c','1b696238-1fdd-4b95-bff0-86a37be13c78','Qwen3 Coder 480B A35B','qwen/qwen3-coder',_SU,'text',None,16,False,'poor',None),
        ('e268ac99-2a94-47c1-9e37-b019e6d024be','0c8d1e1c-fe65-45d3-a1c3-be69e7941e17','LTX Video','fal-ai/ltx-video',{"width": "{:int}", "height": "{:int}", "prompt": "{}", "num_frames": "{:int}"},'text-to-video','0.02 $/видео',16,False,'poor',None),
        ('3f17aefe-7f11-4ee7-a974-5dc3662041ed','1b696238-1fdd-4b95-bff0-86a37be13c78','Nemotron 3 Super 120B A12B','nvidia/nemotron-3-super-120b-a12b',_SU,'text',None,17,False,'rejected',None),
        ('224e7552-a2aa-49fe-9d5c-5cb274491cef','0c8d1e1c-fe65-45d3-a1c3-be69e7941e17','Wan 2.6','wan/v2.6/text-to-video',{"prompt": "{}", "duration": "{}", "resolution": "1080p", "aspect_ratio": "{0}:{1}"},'text-to-video','1.500 $/10сек',17,False,'poor',None),
        ('b61cebee-8493-4af2-935c-8a9ecc398825','38df0351-44cb-4249-be63-a067444dc66f','SkyReels V4 Fast','/api/v1/video/text2video/submit',_SKY('fast','SkyReels-V4-Fast'),'text-to-video','1.100 $/10сек',18,False,'poor','Без звука.\nСтабильно коверкает детали.'),
        ('87bce48f-a82d-4bd2-b1b0-927c99d6efd9','1b696238-1fdd-4b95-bff0-86a37be13c78','Nemotron 3 Nano 30B A3B','nvidia/nemotron-3-nano-30b-a3b',_SU,'text',None,18,False,'rejected',None),
        ('8893a98c-f27f-4bfd-ba3e-596679b36956','38df0351-44cb-4249-be63-a067444dc66f','SkyReels V4 Std','/api/v1/video/text2video/submit',_SKY('std','SkyReels-V4-Std'),'text-to-video','1.400 $/10сек',19,False,'poor','Без звука.\nСтабильно коверкает детали.'),
        ('daef6745-eb65-4991-9cf5-c2b205970ab1','1b696238-1fdd-4b95-bff0-86a37be13c78','Trinity Large Preview','arcee-ai/trinity-large-preview',_SU,'text',None,19,False,'rejected',None),
        ('b1451084-3409-4763-9023-184f2f9e8d97','1b696238-1fdd-4b95-bff0-86a37be13c78','Trinity Mini','arcee-ai/trinity-mini',_SU,'text',None,20,False,'poor',None),
        ('c56b929b-784a-4482-af9c-49c0f462afbe','1b696238-1fdd-4b95-bff0-86a37be13c78','LFM2.5-1.2B-Instruct','liquid/lfm-2.5-1.2b-instruct:free',_SU,'text',None,21,False,'poor',None),
        ('5862ec1e-1dd4-41fa-9482-cb4e93b61b19','1b696238-1fdd-4b95-bff0-86a37be13c78','LFM2.5-1.2B-Thinking','liquid/lfm-2.5-1.2b-thinking',_SU,'text',None,22,False,'poor',None),
        ('37237097-eee5-49e2-b6eb-954aaf878db4','1b696238-1fdd-4b95-bff0-86a37be13c78','GLM 4.5 Air','z-ai/glm-4.5-air:free',_SU,'text',None,23,False,'rejected',None),
        ('4cd1d9cd-4a18-4dd8-9672-9130dc42fc56','1b696238-1fdd-4b95-bff0-86a37be13c78','Venice: Uncensored','cognitivecomputations/dolphin-mistral-24b-venice-edition:free',_SU,'text',None,24,False,'limited',None),
        ('7cc24019-5519-4436-9edc-8545887f7373','1b696238-1fdd-4b95-bff0-86a37be13c78','DeepSeek R1 Distill Llama 70B','deepseek/deepseek-r1-distill-llama-70b',_SU,'text',None,25,False,'poor',None),
        ('32ee9e71-1cee-4a72-80dd-91a26db2547c','1b696238-1fdd-4b95-bff0-86a37be13c78','Phi-4','microsoft/phi-4',_SU,'text',None,26,False,'rejected',None),
        ('6345fd09-349f-4bcf-9b07-37f20fe6bed3','1b696238-1fdd-4b95-bff0-86a37be13c78','Mistral 7B Instruct v0.1','mistralai/mistral-7b-instruct-v0.1',_SU,'text',None,27,False,'rejected',None),
        ('20911225-23ca-42e6-9a62-40d87b8b3bb0','1b696238-1fdd-4b95-bff0-86a37be13c78','MiniMax M2.5','minimax/minimax-m2.5',_SU,'text',None,28,False,'rejected',None),
        ('89b3324c-1f32-46a2-ad48-550a8b6fe831','1b696238-1fdd-4b95-bff0-86a37be13c78','Step 3.5 Flash','stepfun/step-3.5-flash',_SU,'text',None,29,False,'rejected',None),
    ]

    for m in _MODELS:
        cur.execute(_ins_m, (m[0], m[1], m[2], m[3], json.dumps(m[4], ensure_ascii=False),
                              m[5], m[6], m[7], m[8], m[9], m[10]))

    _DURATIONS = [
        ('ad64dcc3-a722-4792-816d-210975786930','0378c0c8-da94-4f8d-8a72-a417223b0fb0',5),
        ('1d9ec9cf-a0a2-49a0-a811-51086f2aff18','0378c0c8-da94-4f8d-8a72-a417223b0fb0',8),
        ('b4c5abbf-b66d-4ec4-8c5c-50cc01493cd5','08bc220d-a490-4f18-ba93-2ce55ce88ee0',4),
        ('72bb32dd-1990-4df4-8e9e-924b105fadd1','08bc220d-a490-4f18-ba93-2ce55ce88ee0',5),
        ('53eed87e-1bb7-4f28-ae8b-13b2dc49b0ea','08bc220d-a490-4f18-ba93-2ce55ce88ee0',6),
        ('3e3c5a26-8325-48ef-a118-3ce254abe8c0','08bc220d-a490-4f18-ba93-2ce55ce88ee0',7),
        ('6868745e-243f-4e9c-935d-28b52105557d','08bc220d-a490-4f18-ba93-2ce55ce88ee0',8),
        ('e419bb7f-57c2-4bed-a8fc-f4cd66639e9e','08bc220d-a490-4f18-ba93-2ce55ce88ee0',9),
        ('e8173b5d-4322-47b9-bc44-ed2d61ed3553','08bc220d-a490-4f18-ba93-2ce55ce88ee0',10),
        ('d7dacb46-daa8-4e0a-9b4c-d031cced5a86','08bc220d-a490-4f18-ba93-2ce55ce88ee0',11),
        ('3b173f1a-8bd3-4cad-8cf2-e489d139de62','08bc220d-a490-4f18-ba93-2ce55ce88ee0',12),
        ('8ba15c86-d8e5-4bd6-924c-1546c858ea83','08bc220d-a490-4f18-ba93-2ce55ce88ee0',13),
        ('b95d03d1-5801-42ba-b40c-4574dd532fa1','08bc220d-a490-4f18-ba93-2ce55ce88ee0',14),
        ('892c74b5-377b-45fb-9f93-7947e687fbe8','08bc220d-a490-4f18-ba93-2ce55ce88ee0',15),
        ('e0123a0e-866d-4eeb-aad1-72ef1e0aa504','0b497f50-6509-456f-82bc-f8b77d41026a',5),
        ('27e10eb1-7f0d-4ef0-9cd4-7698409d8396','0b497f50-6509-456f-82bc-f8b77d41026a',8),
        ('82801def-8d7c-497f-a9bf-fcf472470113','224e7552-a2aa-49fe-9d5c-5cb274491cef',5),
        ('b8c568a4-bf4e-470b-b875-05a77634b090','224e7552-a2aa-49fe-9d5c-5cb274491cef',10),
        ('343c1f93-6fa0-4caf-abc6-3ee6c4e7e324','224e7552-a2aa-49fe-9d5c-5cb274491cef',15),
        ('f47491ac-c8f5-4049-b9f5-66a017948a83','28d29f45-151a-4836-aff8-53ed29c950f0',3),
        ('31cdf7b2-b7dd-4df4-9d6f-916a4e2d6291','28d29f45-151a-4836-aff8-53ed29c950f0',4),
        ('3972e9ad-c267-4ab0-ae1f-5dca832a5170','28d29f45-151a-4836-aff8-53ed29c950f0',5),
        ('efb1d70e-615f-44d3-96e1-93e0827d4fac','28d29f45-151a-4836-aff8-53ed29c950f0',6),
        ('43fbe3b8-128c-425c-bcd1-8c9889d81567','28d29f45-151a-4836-aff8-53ed29c950f0',7),
        ('31b146dc-a518-4be6-8988-959ddf4976c8','28d29f45-151a-4836-aff8-53ed29c950f0',8),
        ('036f1a01-fb13-4b83-83d2-333e01da0b39','28d29f45-151a-4836-aff8-53ed29c950f0',9),
        ('028a83ee-7073-424e-9db2-83a3b64617c6','28d29f45-151a-4836-aff8-53ed29c950f0',10),
        ('621c3c50-0564-4ecf-a00e-f11326b089aa','28d29f45-151a-4836-aff8-53ed29c950f0',11),
        ('29a6de75-ca5d-4d5d-86a6-105009e03e6a','28d29f45-151a-4836-aff8-53ed29c950f0',12),
        ('7abf0efd-afe9-4113-8b8c-9bce826840a1','28d29f45-151a-4836-aff8-53ed29c950f0',13),
        ('5d28bf85-d5d7-4bde-b3b5-ac61762bec2f','28d29f45-151a-4836-aff8-53ed29c950f0',14),
        ('536d9def-697a-4bfe-b4d3-d091f4031571','28d29f45-151a-4836-aff8-53ed29c950f0',15),
        ('81d5bbb7-837b-4354-83e3-512f1beda731','2b0ef68a-52e8-4d37-8b29-716c4fb7d0dc',0),
        ('cf79294f-0280-45d5-94db-33a1a6bba2dd','31d3c708-acd9-4a90-9efb-909219814b30',5),
        ('6234ec25-7523-4074-81c4-eabe1b2bf7ec','31d3c708-acd9-4a90-9efb-909219814b30',10),
        ('85bb6ea5-e20d-447c-8038-a2d192aea4de','4ab3ffe0-2eee-43e5-9d4d-77a576a6e17c',2),
        ('da42c386-c5bf-4d92-bfd4-e5d6ad2d43ba','4ab3ffe0-2eee-43e5-9d4d-77a576a6e17c',3),
        ('1083faa2-3311-4cef-a8a9-a04f6a1cd3ec','4ab3ffe0-2eee-43e5-9d4d-77a576a6e17c',4),
        ('da29ec0f-26ea-4a25-b0c5-857aeaf9de23','4ab3ffe0-2eee-43e5-9d4d-77a576a6e17c',5),
        ('0fccda90-f82c-4cc6-9cea-d93d5ed6ef97','4ab3ffe0-2eee-43e5-9d4d-77a576a6e17c',6),
        ('04125100-0ec5-4b22-9039-0b787a175617','4ab3ffe0-2eee-43e5-9d4d-77a576a6e17c',7),
        ('d5c06487-fabc-4ee9-b08e-ce3f0abfdaf9','4ab3ffe0-2eee-43e5-9d4d-77a576a6e17c',8),
        ('105c2b60-f997-4194-9611-88b37a064641','4ab3ffe0-2eee-43e5-9d4d-77a576a6e17c',9),
        ('b7db6b49-2525-459c-92c2-99d1293415f8','4ab3ffe0-2eee-43e5-9d4d-77a576a6e17c',10),
        ('2ab5c70a-3d67-471c-a5ae-6866d87bb5f3','4ab3ffe0-2eee-43e5-9d4d-77a576a6e17c',11),
        ('80b6f974-9aa7-47a1-b06a-a3f549f8e1a9','4ab3ffe0-2eee-43e5-9d4d-77a576a6e17c',12),
        ('f0b294b6-f8fa-4298-9621-72ecbf9f232e','4ab3ffe0-2eee-43e5-9d4d-77a576a6e17c',13),
        ('694de0be-fd48-480f-8d25-1787e43f2ae4','4ab3ffe0-2eee-43e5-9d4d-77a576a6e17c',14),
        ('12b45a6b-8fb6-4838-a1bc-2a343e082544','4ab3ffe0-2eee-43e5-9d4d-77a576a6e17c',15),
        ('956e16b6-fb16-45ce-a909-fd86191b9133','4bc53f61-6e14-4951-a8d8-3bfac31d7dbd',4),
        ('448890a0-8616-4414-80fb-52a799b8dda9','4bc53f61-6e14-4951-a8d8-3bfac31d7dbd',5),
        ('921de8e1-a121-47d3-af1c-5aca2f419c63','4bc53f61-6e14-4951-a8d8-3bfac31d7dbd',6),
        ('fe350b62-b6bc-41ab-ac10-b2c184026a2e','4bc53f61-6e14-4951-a8d8-3bfac31d7dbd',7),
        ('2a78ee30-1fac-46c2-b413-c6b2243b725e','4bc53f61-6e14-4951-a8d8-3bfac31d7dbd',8),
        ('7f31b86f-0d3c-4ef2-a784-96290c35d9f6','4bc53f61-6e14-4951-a8d8-3bfac31d7dbd',9),
        ('1582785f-b935-46e2-80f2-d2e0984fcae6','4bc53f61-6e14-4951-a8d8-3bfac31d7dbd',10),
        ('46121f28-d9c5-4ea1-91da-6303271a38b2','4bc53f61-6e14-4951-a8d8-3bfac31d7dbd',11),
        ('a231c9e6-6331-4740-843f-74806dca2f34','4bc53f61-6e14-4951-a8d8-3bfac31d7dbd',12),
        ('f66cd129-9c8d-4291-90b2-fbf46783fc00','4bc53f61-6e14-4951-a8d8-3bfac31d7dbd',13),
        ('03d67733-a0ac-4e8e-a123-2fa59373ae32','4bc53f61-6e14-4951-a8d8-3bfac31d7dbd',14),
        ('dde81ce0-4148-4b94-a25f-655a40410450','4bc53f61-6e14-4951-a8d8-3bfac31d7dbd',15),
        ('28283b31-c692-4ba2-96eb-a3e02f241763','4f59c728-a7f1-45f7-b9d0-8b8657c7db15',4),
        ('42af6849-95f7-4bed-a97a-3a3b14b04465','4f59c728-a7f1-45f7-b9d0-8b8657c7db15',5),
        ('48c05f6a-099a-401c-b5ed-1f02bbb0307d','4f59c728-a7f1-45f7-b9d0-8b8657c7db15',6),
        ('579d182b-cad4-48ed-b95e-11854161bab1','4f59c728-a7f1-45f7-b9d0-8b8657c7db15',7),
        ('b71ada76-1c6d-4f79-9ad2-d95ad79669f2','4f59c728-a7f1-45f7-b9d0-8b8657c7db15',8),
        ('65a4eac5-0655-45fc-b95e-713a339cfbe3','4f59c728-a7f1-45f7-b9d0-8b8657c7db15',9),
        ('1819ea4e-c5d4-42ac-b2e5-439608b23a6d','4f59c728-a7f1-45f7-b9d0-8b8657c7db15',10),
        ('b8ba0e68-a085-48d7-a174-48bdfe97c602','4f59c728-a7f1-45f7-b9d0-8b8657c7db15',11),
        ('3b65f116-1810-4e96-b556-fe48f28a572e','4f59c728-a7f1-45f7-b9d0-8b8657c7db15',12),
        ('4f3c13a8-41bf-4532-abe4-a5056ef3f425','4f59c728-a7f1-45f7-b9d0-8b8657c7db15',13),
        ('7c706c69-65e6-4bb0-8008-69d49257c0d7','4f59c728-a7f1-45f7-b9d0-8b8657c7db15',14),
        ('f588f5c6-96bb-4ca7-a6ca-856c407c8c86','4f59c728-a7f1-45f7-b9d0-8b8657c7db15',15),
        ('5205af06-138f-4aa9-b019-a6effb69862b','6f908a00-5251-4471-8793-751cbc3fe11e',1),
        ('65646b26-6a85-4f04-9357-7a9755ed941c','6f908a00-5251-4471-8793-751cbc3fe11e',2),
        ('7c299392-a839-4834-81c9-a786ab2b76c2','6f908a00-5251-4471-8793-751cbc3fe11e',3),
        ('0223800d-143f-4eb3-8c07-e1f734da4ff4','6f908a00-5251-4471-8793-751cbc3fe11e',4),
        ('6ca25741-006c-452e-8148-efa92278ff47','6f908a00-5251-4471-8793-751cbc3fe11e',5),
        ('9fec6c8a-b2cc-4b60-9612-057a93c51c1c','6f908a00-5251-4471-8793-751cbc3fe11e',6),
        ('3991b98f-779e-475c-b36a-6b94db165495','6f908a00-5251-4471-8793-751cbc3fe11e',7),
        ('f3a1ef7d-1529-4d87-b72b-667e226c5e59','6f908a00-5251-4471-8793-751cbc3fe11e',8),
        ('fefc64a0-0a41-4413-b0d4-0a2e3a77be5b','6f908a00-5251-4471-8793-751cbc3fe11e',9),
        ('49b63acd-a9ce-496a-8c8c-802c3e53a595','6f908a00-5251-4471-8793-751cbc3fe11e',10),
        ('251a54ec-ff19-4a8c-b64a-34cb4d6198f4','6f908a00-5251-4471-8793-751cbc3fe11e',11),
        ('dfe45c65-8a8b-4a40-8e0c-c2e280bc6e55','6f908a00-5251-4471-8793-751cbc3fe11e',12),
        ('32f398ca-71a2-43b4-8f54-8705baf759d0','6f908a00-5251-4471-8793-751cbc3fe11e',13),
        ('c91158c3-2ef1-4a99-b82b-798e46dc77d1','6f908a00-5251-4471-8793-751cbc3fe11e',14),
        ('f5a199a4-173e-4659-9057-f6762b02a082','6f908a00-5251-4471-8793-751cbc3fe11e',15),
        ('925cdfa4-212c-445c-b1fa-a97a22692afa','8893a98c-f27f-4bfd-ba3e-596679b36956',5),
        ('b60cd950-25e6-4c94-a2d4-115645e98336','8893a98c-f27f-4bfd-ba3e-596679b36956',6),
        ('5f799986-209e-4bd4-a283-4769563e6eb1','8893a98c-f27f-4bfd-ba3e-596679b36956',7),
        ('eb33c375-72b3-4c87-8553-355aa3904852','8893a98c-f27f-4bfd-ba3e-596679b36956',8),
        ('45a2f50f-5ebe-4ba1-8226-64ab61856c96','8893a98c-f27f-4bfd-ba3e-596679b36956',9),
        ('815a7028-a7a0-414d-8378-b235b696b546','8893a98c-f27f-4bfd-ba3e-596679b36956',10),
        ('52f59f9c-45f0-487d-a108-f297f5ad95f3','8893a98c-f27f-4bfd-ba3e-596679b36956',11),
        ('57144704-3acb-4711-9fa5-c39367038dd8','8893a98c-f27f-4bfd-ba3e-596679b36956',12),
        ('9ea48de0-b55e-4588-9b20-09b24989b61b','8893a98c-f27f-4bfd-ba3e-596679b36956',13),
        ('17b7435d-543e-48e9-9e17-058efa7ee7f7','8893a98c-f27f-4bfd-ba3e-596679b36956',14),
        ('65027de5-f517-4ac7-9ba9-2552845d36e9','8893a98c-f27f-4bfd-ba3e-596679b36956',15),
        ('05a3f0c1-9a31-4f9a-88cf-1682a139a28b','8c23daf4-1a70-4e13-ada3-4f7d0f1822ef',4),
        ('0ddc7ba2-e198-4bf8-b95a-f30f3c1745cf','8c23daf4-1a70-4e13-ada3-4f7d0f1822ef',8),
        ('1b46ae66-9724-44d9-aa1a-b6bd18fe6976','8c23daf4-1a70-4e13-ada3-4f7d0f1822ef',12),
        ('26693d2c-274e-45f0-a197-31f1bd91e377','8c23daf4-1a70-4e13-ada3-4f7d0f1822ef',16),
        ('33d7fb50-e3b3-4960-a132-63dfe33da76c','8c23daf4-1a70-4e13-ada3-4f7d0f1822ef',20),
        ('c0fc0f0f-1f1f-492a-9b52-5696e119f018','aacc47c3-a8c6-41ce-9237-79373d9712d8',3),
        ('38e37fef-5606-4e6c-a162-2f80d2b39531','aacc47c3-a8c6-41ce-9237-79373d9712d8',4),
        ('955d8694-ca08-4e34-96cb-a051f9a37c61','aacc47c3-a8c6-41ce-9237-79373d9712d8',5),
        ('12c27c2b-1897-42d9-94d9-3f6eebe1c9c9','aacc47c3-a8c6-41ce-9237-79373d9712d8',6),
        ('9a0abe93-d25d-4834-9246-2a53ceb03bf9','aacc47c3-a8c6-41ce-9237-79373d9712d8',7),
        ('68279d91-bd95-4127-9592-990e6e237f88','aacc47c3-a8c6-41ce-9237-79373d9712d8',8),
        ('494004b9-5040-4b82-a952-f7cbcf1dbc1c','aacc47c3-a8c6-41ce-9237-79373d9712d8',9),
        ('d5246cfa-381a-4f2d-a42d-e16f4287f83e','aacc47c3-a8c6-41ce-9237-79373d9712d8',10),
        ('7c66386c-7fbb-4ddf-bb82-6876f0962f45','aacc47c3-a8c6-41ce-9237-79373d9712d8',11),
        ('640532a0-8ca6-4b40-9c33-b147809259f5','aacc47c3-a8c6-41ce-9237-79373d9712d8',12),
        ('e7f8ec6b-1b67-4810-b01e-de3e94e02eae','aacc47c3-a8c6-41ce-9237-79373d9712d8',13),
        ('338853f8-4844-4fa2-beb0-5d403b1a2fbb','aacc47c3-a8c6-41ce-9237-79373d9712d8',14),
        ('136ffde4-cafc-4b06-a568-804953d54b65','aacc47c3-a8c6-41ce-9237-79373d9712d8',15),
        ('da3539f2-51bb-49f9-bafb-f6f7d51432be','b5e3f891-2c4a-4d67-9e8f-1a2b3c4d5e6f',4),
        ('65ae86ad-fe93-4373-a69a-a28ddc242506','b5e3f891-2c4a-4d67-9e8f-1a2b3c4d5e6f',5),
        ('7654e467-29f2-4dcc-94f9-6bb4aa709aee','b5e3f891-2c4a-4d67-9e8f-1a2b3c4d5e6f',6),
        ('07d614ca-0970-4f27-b150-7aa3fd526c1b','b5e3f891-2c4a-4d67-9e8f-1a2b3c4d5e6f',7),
        ('7f1b6e98-3305-4e9d-b945-cdde16663844','b5e3f891-2c4a-4d67-9e8f-1a2b3c4d5e6f',8),
        ('1993bc29-3866-4610-86ca-f15ecb181a62','b5e3f891-2c4a-4d67-9e8f-1a2b3c4d5e6f',9),
        ('49895661-a06b-437e-a5d9-8b248740d67b','b5e3f891-2c4a-4d67-9e8f-1a2b3c4d5e6f',10),
        ('0374fbe7-8ba2-42cc-875f-bb0c403d175f','b5e3f891-2c4a-4d67-9e8f-1a2b3c4d5e6f',11),
        ('67bceff7-d252-41ab-8142-87519243881c','b5e3f891-2c4a-4d67-9e8f-1a2b3c4d5e6f',12),
        ('315c9bb2-61c7-4487-a0cb-f2d6b4729c07','b61cebee-8493-4af2-935c-8a9ecc398825',5),
        ('4bc34e3c-54be-4f0b-bfaa-9f5bd05dedea','b61cebee-8493-4af2-935c-8a9ecc398825',6),
        ('36b54534-5f7b-483f-9d1a-7d7e8a9cdf0f','b61cebee-8493-4af2-935c-8a9ecc398825',7),
        ('102490e7-a60e-4e49-8719-4424c710fcfa','b61cebee-8493-4af2-935c-8a9ecc398825',8),
        ('f4f3795d-73f9-42e0-85d6-0205aebb0015','b61cebee-8493-4af2-935c-8a9ecc398825',9),
        ('e9493ae8-79c1-4ada-887b-5d0c421ea3ba','b61cebee-8493-4af2-935c-8a9ecc398825',10),
        ('c86e6ae7-57c8-4ba4-8685-4d639cd82261','b61cebee-8493-4af2-935c-8a9ecc398825',11),
        ('a721e815-59ee-42c9-91ac-3d0ddbc7f619','b61cebee-8493-4af2-935c-8a9ecc398825',12),
        ('b9e0d25c-181c-4dc5-b917-89594012484b','b61cebee-8493-4af2-935c-8a9ecc398825',13),
        ('dc516356-0714-49e9-bb25-fc5b46b9ea59','b61cebee-8493-4af2-935c-8a9ecc398825',14),
        ('96925fa7-06d8-473f-8e54-a7f978a473b5','b61cebee-8493-4af2-935c-8a9ecc398825',15),
        ('93c64dcf-0fe9-487f-a46e-bde2b40a6a9b','d3e4f5a6-b7c8-4d91-ae2f-1b2c3d4e5f6a',6),
        ('3f108059-79f1-41ed-93fe-5b0d74c20c7e','d3e4f5a6-b7c8-4d91-ae2f-1b2c3d4e5f6a',8),
        ('651ae5d6-0ffb-4f8f-9ae7-28adcc5f92b0','d3e4f5a6-b7c8-4d91-ae2f-1b2c3d4e5f6a',10),
        ('519f71f3-6426-4496-8b2e-93a3cca908e7','e268ac99-2a94-47c1-9e37-b019e6d024be',0),
        ('65b45a91-f3d6-4cf2-a8ba-470a8210e120','f0293755-8bf2-4654-8d7c-eb08dbd919c6',5),
        ('af08d499-49bd-4520-a143-3eb074c17a23','f0293755-8bf2-4654-8d7c-eb08dbd919c6',6),
        ('697fbcad-842f-4089-be04-2153cbdccefe','f0293755-8bf2-4654-8d7c-eb08dbd919c6',7),
        ('4dc477cd-9433-487d-89f5-7944db2197fd','f0293755-8bf2-4654-8d7c-eb08dbd919c6',8),
    ]

    for d in _DURATIONS:
        cur.execute(_ins_d, d)


MIGRATIONS = [
    (71, _m071_donor_good_only),
    (72, _m072_stories_pinned),
    (73, _m073_add_wan27),
    (74, _m074_rename_key_env),
    (75, _m075_add_skyreels),
    (76, _m076_skyreels_body_update),
    (77, _m077_skyreels_sound_false),
    (78, _m078_skyreels_durations),
    (79, _m079_created_at_clock_timestamp),
    (80, _m080_drop_stories_top_quality),
    (81, _m081_add_max_model_passes_setting),
    (82, _m082_redistribute_settings_env),
    (83, _m083_create_cycle_config),
    (84, _m084_add_words_per_second),
    (85, _m085_cycle_config_to_kv),
    (86, _m086_user_role_links_pk),
    (87, _m087_publication_counter),
    (88, _m088_batches_title),
    (89, _m089_replace_ai_models),
]


# ---------------------------------------------------------------------------
# Запуск миграций
# ---------------------------------------------------------------------------

def run_migrations():
    """Применяет все незапущенные миграции. Каждая в отдельной транзакции."""
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM settings WHERE key = 'db_version'")
            row = cur.fetchone()
            current = int(row[0]) if row else 0
        conn.commit()

        for version, fn in MIGRATIONS:
            if version <= current:
                continue
            try:
                with conn.cursor() as cur:
                    fn(cur)
                    cur.execute(
                        "INSERT INTO settings (key, value) VALUES ('db_version', %s)"
                        " ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                        (str(version),),
                    )
                conn.commit()
                write_log_entry(None, f"[DB] Миграция {version} применена", level='silent')
            except Exception as e:
                conn.rollback()
                write_log_entry(None, f"[DB] Ошибка миграции {version}: {e}", level='silent')
                raise
    finally:
        conn.close()
