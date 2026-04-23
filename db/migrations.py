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
