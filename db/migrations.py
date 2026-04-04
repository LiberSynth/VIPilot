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


# ---------------------------------------------------------------------------
# Функции миграций
# ---------------------------------------------------------------------------

def _m001_baseline_schema(cur):
    """
    Базовая схема: все таблицы приложения.
    Deployed: 2026-04-04
    """
    cur.execute("""
        CREATE TABLE IF NOT EXISTS environment (
            key   VARCHAR(100) PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key   VARCHAR(100) PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS schedule (
            id       UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
            time_utc VARCHAR(5)   NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ai_platforms (
            id   UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
            name VARCHAR(200) NOT NULL,
            url  VARCHAR(500) NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ai_models (
            id             UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
            name           VARCHAR(200) NOT NULL,
            url            VARCHAR(200) NOT NULL,
            body           JSONB        NOT NULL DEFAULT '{}',
            "order"        INTEGER      NOT NULL DEFAULT 0,
            active         BOOLEAN      NOT NULL DEFAULT FALSE,
            ai_platform_id UUID         REFERENCES ai_platforms(id),
            platform_id    UUID         REFERENCES ai_platforms(id),
            type           VARCHAR(50)  NOT NULL,
            created_at     TIMESTAMPTZ  NOT NULL DEFAULT now()
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS targets (
            id             UUID     PRIMARY KEY DEFAULT gen_random_uuid(),
            name           VARCHAR(200) NOT NULL,
            aspect_ratio_x SMALLINT NOT NULL DEFAULT 9,
            aspect_ratio_y SMALLINT NOT NULL DEFAULT 16,
            active         BOOLEAN  NOT NULL DEFAULT TRUE
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stories (
            id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            result     TEXT        NOT NULL,
            model_id   UUID        REFERENCES ai_models(id)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS batches (
            id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
            scheduled_at TIMESTAMPTZ,
            target_id    UUID        NOT NULL REFERENCES targets(id),
            status       VARCHAR(30) NOT NULL DEFAULT 'pending',
            story_id     UUID        REFERENCES stories(id),
            video_url    TEXT,
            video_file   TEXT,
            video_data   BYTEA,
            data         JSONB,
            created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
            completed_at TIMESTAMPTZ
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS log (
            id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
            batch_id   UUID        REFERENCES batches(id),
            pipeline   VARCHAR(30) NOT NULL,
            message    TEXT,
            status     VARCHAR(20),
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS log_entries (
            id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
            log_id     UUID        NOT NULL REFERENCES log(id),
            message    TEXT        NOT NULL,
            level      VARCHAR(10) NOT NULL DEFAULT 'info',
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)


def _m002_model_grades_and_batch_models(cur):
    """
    Добавляет grade к ai_models; adhoc/text_model_id/video_model_id к batches;
    уникальный индекс и триггер проверки типов моделей на batches.
    Deployed: 2026-04-04
    """
    cur.execute("""
        ALTER TABLE ai_models
            ADD COLUMN IF NOT EXISTS grade VARCHAR(20) NOT NULL DEFAULT 'good'
    """)
    cur.execute("""
        ALTER TABLE batches
            ADD COLUMN IF NOT EXISTS adhoc BOOLEAN NOT NULL DEFAULT false
    """)
    cur.execute("""
        ALTER TABLE batches
            ADD COLUMN IF NOT EXISTS text_model_id UUID REFERENCES ai_models(id)
    """)
    cur.execute("""
        ALTER TABLE batches
            ADD COLUMN IF NOT EXISTS video_model_id UUID REFERENCES ai_models(id)
    """)
    cur.execute("""
        DO $$ BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'batches_scheduled_at_target_id_key'
                  AND conrelid = 'batches'::regclass
            ) THEN
                ALTER TABLE batches
                    ADD CONSTRAINT batches_scheduled_at_target_id_key
                    UNIQUE (scheduled_at, target_id);
            END IF;
        END $$
    """)
    cur.execute("""
        CREATE OR REPLACE FUNCTION trg_fn_batch_model_types()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF NEW.text_model_id IS NOT NULL THEN
                IF NOT EXISTS (
                    SELECT 1 FROM ai_models
                    WHERE id = NEW.text_model_id AND type = 'text'
                ) THEN
                    RAISE EXCEPTION 'text_model_id must reference ai_models with type=text';
                END IF;
            END IF;
            IF NEW.video_model_id IS NOT NULL THEN
                IF NOT EXISTS (
                    SELECT 1 FROM ai_models
                    WHERE id = NEW.video_model_id AND type = 'text-to-video'
                ) THEN
                    RAISE EXCEPTION 'video_model_id must reference ai_models with type=text-to-video';
                END IF;
            END IF;
            RETURN NEW;
        END;
        $$
    """)
    cur.execute("""
        DO $$ BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_trigger
                WHERE tgname = 'trg_batch_model_types'
                  AND tgrelid = 'batches'::regclass
            ) THEN
                CREATE TRIGGER trg_batch_model_types
                BEFORE INSERT OR UPDATE OF text_model_id, video_model_id ON batches
                FOR EACH ROW EXECUTE FUNCTION trg_fn_batch_model_types();
            END IF;
        END $$
    """)


def _m003_schedule_created_at(cur):
    """
    Добавляет created_at к schedule.
    Deployed: 2026-04-04
    """
    cur.execute("""
        ALTER TABLE schedule
            ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    """)


def _m004_seed_ai_models(cur):
    """
    Полный набор платформ и моделей ИИ.
    Текстовые (OpenRouter): 34 модели с правильными order/grade/url.
    Видео (fal): sora-2, kling-video/v1.6/standard, veo2, minimax/video-01.
    Идемпотентно: WHERE NOT EXISTS по name.
    Deployed: 2026-04-04
    """
    TEXT_BODY = (
        '{"messages": [{"role": "system", "content": "{}"},'
        ' {"role": "user", "content": "{}"}],'
        ' "max_tokens": 300, "temperature": 0.9}'
    )

    cur.execute("""
        INSERT INTO ai_platforms (name, url)
        SELECT v.name, v.url FROM (VALUES
            ('OpenRouter', 'https://openrouter.ai/api/v1/chat/completions'),
            ('fal',        'https://queue.fal.run/fal-ai')
        ) AS v(name, url)
        WHERE NOT EXISTS (SELECT 1 FROM ai_platforms WHERE name = v.name)
    """)

    cur.execute("""
        INSERT INTO ai_models
            (name, url, body, "order", active, ai_platform_id, platform_id, type, grade)
        SELECT
            v.name, v.url, v.body::jsonb,
            v.ord, v.active,
            p.id, p.id,
            v.type, v.grade
        FROM (VALUES
            -- text-to-video (fal)
            ('sora-2',
             'sora-2/text-to-video',
             '{"prompt": "{}", "duration": "{int}", "aspect_ratio": "{:d}:{:d}"}',
             1, TRUE, 'fal', 'text-to-video', 'good'),
            ('kling-video/v1.6/standard',
             'kling-video/v1.6/standard/text-to-video',
             '{"prompt": "{}", "duration": "{:d}", "aspect_ratio": "{:d}:{:d}"}',
             2, TRUE, 'fal', 'text-to-video', 'good'),
            ('veo2',
             'veo2',
             '{"prompt": "{}", "duration": "{:d}s", "aspect_ratio": "{:d}:{:d}"}',
             3, TRUE, 'fal', 'text-to-video', 'good'),
            ('minimax/video-01',
             'minimax/video-01',
             '{"prompt": "{}", "duration": "{:d}s", "aspect_ratio": "{:d}:{:d}"}',
             4, FALSE, 'fal', 'text-to-video', 'good'),
            -- text — good (active)
            ('gemma-2-9b-it',
             'google/gemma-2-9b-it',
             %s, 1, TRUE, 'OpenRouter', 'text', 'good'),
            ('qwen3.6-plus',
             'qwen/qwen3.6-plus:free',
             %s, 2, TRUE, 'OpenRouter', 'text', 'good'),
            ('phi-4',
             'microsoft/phi-4',
             %s, 3, TRUE, 'OpenRouter', 'text', 'good'),
            ('llama-3.1-8b-instruct',
             'meta-llama/llama-3.1-8b-instruct',
             %s, 4, TRUE, 'OpenRouter', 'text', 'good'),
            -- text — limited
            ('nemotron-3-super-120b',
             'nvidia/nemotron-3-super-120b-a12b:free',
             %s, 5, FALSE, 'OpenRouter', 'text', 'limited'),
            ('trinity-large-preview',
             'arcee-ai/trinity-large-preview:free',
             %s, 6, FALSE, 'OpenRouter', 'text', 'limited'),
            ('lfm-2.5-1.2b-instruct',
             'liquid/lfm-2.5-1.2b-instruct:free',
             %s, 7, FALSE, 'OpenRouter', 'text', 'limited'),
            ('nemotron-3-nano-30b',
             'nvidia/nemotron-3-nano-30b-a3b:free',
             %s, 8, FALSE, 'OpenRouter', 'text', 'limited'),
            ('trinity-mini',
             'arcee-ai/trinity-mini:free',
             %s, 9, FALSE, 'OpenRouter', 'text', 'limited'),
            ('nemotron-nano-12b-v2-vl',
             'nvidia/nemotron-nano-12b-v2-vl:free',
             %s, 10, FALSE, 'OpenRouter', 'text', 'limited'),
            ('qwen3-next-80b',
             'qwen/qwen3-next-80b-a3b-instruct:free',
             %s, 11, FALSE, 'OpenRouter', 'text', 'limited'),
            ('nemotron-nano-9b-v2',
             'nvidia/nemotron-nano-9b-v2:free',
             %s, 12, FALSE, 'OpenRouter', 'text', 'limited'),
            ('glm-4.5-air',
             'z-ai/glm-4.5-air:free',
             %s, 13, FALSE, 'OpenRouter', 'text', 'limited'),
            ('gemma-3n-e2b-it',
             'google/gemma-3n-e2b-it:free',
             %s, 14, FALSE, 'OpenRouter', 'text', 'limited'),
            ('gemma-3n-e4b-it',
             'google/gemma-3n-e4b-it:free',
             %s, 15, FALSE, 'OpenRouter', 'text', 'limited'),
            ('gemma-3-4b-it',
             'google/gemma-3-4b-it:free',
             %s, 16, FALSE, 'OpenRouter', 'text', 'limited'),
            ('gemma-3-12b-it',
             'google/gemma-3-12b-it:free',
             %s, 17, FALSE, 'OpenRouter', 'text', 'limited'),
            ('llama-3.3-70b-instruct',
             'meta-llama/llama-3.3-70b-instruct:free',
             %s, 18, FALSE, 'OpenRouter', 'text', 'limited'),
            ('llama-3.2-3b-instruct',
             'meta-llama/llama-3.2-3b-instruct:free',
             %s, 19, FALSE, 'OpenRouter', 'text', 'limited'),
            -- text — poor
            ('hermes-3-llama-3.1-405b',
             'nousresearch/hermes-3-llama-3.1-405b:free',
             %s, 20, FALSE, 'OpenRouter', 'text', 'poor'),
            ('lfm-2.5-1.2b-thinking',
             'liquid/lfm-2.5-1.2b-thinking:free',
             %s, 21, FALSE, 'OpenRouter', 'text', 'poor'),
            ('qwen3-coder',
             'qwen/qwen3-coder:free',
             %s, 22, FALSE, 'OpenRouter', 'text', 'poor'),
            ('dolphin-mistral-24b-venice',
             'cognitivecomputations/dolphin-mistral-24b-venice-edition:free',
             %s, 23, FALSE, 'OpenRouter', 'text', 'poor'),
            -- text — fallback (без платформы)
            ('openrouter/free',
             'openrouter/free',
             %s, 24, FALSE, NULL, 'text', 'fallback'),
            -- text — rejected
            ('deepseek-r1-distill-llama-70b',
             'deepseek/deepseek-r1-distill-llama-70b',
             %s, 25, FALSE, 'OpenRouter', 'text', 'rejected'),
            ('gemma-3-27b-it',
             'google/gemma-3-27b-it:free',
             %s, 26, FALSE, 'OpenRouter', 'text', 'rejected'),
            ('phi-3-mini-128k-instruct',
             'microsoft/phi-3-mini-128k-instruct:free',
             %s, 27, FALSE, 'OpenRouter', 'text', 'rejected'),
            ('mistral-7b-instruct',
             'mistralai/mistral-7b-instruct-v0.1',
             %s, 28, FALSE, 'OpenRouter', 'text', 'rejected'),
            ('minimax-m2.5',
             'minimax/minimax-m2.5:free',
             %s, 29, FALSE, 'OpenRouter', 'text', 'rejected'),
            ('openchat-7b',
             'openchat/openchat-7b:free',
             %s, 30, FALSE, 'OpenRouter', 'text', 'rejected'),
            ('qwen3.6-plus-preview',
             'qwen/qwen3.6-plus-preview:free',
             %s, 31, FALSE, 'OpenRouter', 'text', 'rejected'),
            ('step-3.5-flash',
             'stepfun/step-3.5-flash:free',
             %s, 32, FALSE, 'OpenRouter', 'text', 'rejected'),
            ('gpt-oss-120b',
             'openai/gpt-oss-120b:free',
             %s, 33, FALSE, 'OpenRouter', 'text', 'rejected'),
            ('gpt-oss-20b',
             'openai/gpt-oss-20b:free',
             %s, 34, FALSE, 'OpenRouter', 'text', 'rejected')
        ) AS v(name, url, body, ord, active, platform_name, type, grade)
        LEFT JOIN ai_platforms p ON p.name = v.platform_name
        WHERE NOT EXISTS (SELECT 1 FROM ai_models WHERE name = v.name)
    """, (TEXT_BODY,) * 34)


# ---------------------------------------------------------------------------
# Реестр миграций — добавляйте только в конец, никогда не переиспользуйте номера
# ---------------------------------------------------------------------------

MIGRATIONS = [
    (1, _m001_baseline_schema),
    (2, _m002_model_grades_and_batch_models),
    (3, _m003_schedule_created_at),
    (4, _m004_seed_ai_models),
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
                print(f"[DB] Миграция {version} применена")
            except Exception as e:
                conn.rollback()
                print(f"[DB] Ошибка миграции {version}: {e}")
                raise
    finally:
        conn.close()
