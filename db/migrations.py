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
            video_data_transcoded BYTEA,
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
            ('fal',        'https://queue.fal.run')
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

def _m005_batch_original_video(cur):
    """
    Добавляет поле video_data_original (BYTEA) в таблицу batches
    для хранения оригинального видео, скачанного с модели до транскодирования.
    """
    cur.execute("""
        ALTER TABLE batches
            ADD COLUMN IF NOT EXISTS video_data_original BYTEA
    """)


def _m006_target_transcode(cur):
    """
    Добавляет поле transcode (BOOLEAN) в таблицу targets.
    По умолчанию FALSE, для VKontakte — TRUE.
    """
    cur.execute("""
        ALTER TABLE targets
            ADD COLUMN IF NOT EXISTS transcode BOOLEAN NOT NULL DEFAULT FALSE
    """)
    cur.execute("""
        UPDATE targets SET transcode = TRUE WHERE name = 'VKontakte'
    """)


def _m007_rename_video_data(cur):
    """
    Переименовывает колонку video_data → video_data_transcoded в таблице batches,
    чтобы название явно отражало, что это транскодированное (обработанное ffmpeg) видео.
    Идемпотентно: пропускает переименование, если video_data уже отсутствует
    (т.е. столбец изначально создан с правильным именем через _m001).
    """
    cur.execute("""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'batches' AND column_name = 'video_data'
            ) THEN
                ALTER TABLE batches RENAME COLUMN video_data TO video_data_transcoded;
            END IF;
        END $$
    """)


def _m008_fix_ai_model_grades(cur):
    """
    Исправляет grade для трёх текстовых моделей, которые на проде имеют
    неверное значение из-за того, что _m004 только вставляет новые записи,
    но не обновляет существующие.

    Нужные значения:
      - mistral-7b-instruct     → rejected
      - openrouter/free         → fallback
      - qwen3.6-plus-preview    → rejected
    """
    cur.execute("""
        UPDATE ai_models SET grade = 'rejected' WHERE name = 'mistral-7b-instruct'
    """)
    cur.execute("""
        UPDATE ai_models SET grade = 'fallback' WHERE name = 'openrouter/free'
    """)
    cur.execute("""
        UPDATE ai_models SET grade = 'rejected' WHERE name = 'qwen3.6-plus-preview'
    """)


def _m009_batches_target_nullable(cur):
    """
    Снимает ограничение NOT NULL с поля target_id в таблице batches,
    чтобы можно было создавать служебные батчи (пробные запросы) без привязки к таргету.
    """
    cur.execute("""
        ALTER TABLE batches
            ALTER COLUMN target_id DROP NOT NULL
    """)


def _m010_sync_ai_models_from_dev(cur):
    """
    Синхронизация ai_models с дева на прод: обновление всех совпадающих по name записей
    (url, body, order, active, ai_platform_id, platform_id, type, grade),
    а также добавление отсутствующих моделей (ltx-video, wan-2.6).
    Deployed: -
    """
    updates = [
        ('deepseek-r1-distill-llama-70b', 'deepseek/deepseek-r1-distill-llama-70b', '{"messages": [{"role": "system", "content": "{}"}, {"role": "user", "content": "{}"}], "max_tokens": 300, "temperature": 0.9}', 25, False, '1b696238-1fdd-4b95-bff0-86a37be13c78', '1b696238-1fdd-4b95-bff0-86a37be13c78', 'text', 'rejected'),
        ('dolphin-mistral-24b-venice', 'cognitivecomputations/dolphin-mistral-24b-venice-edition:free', '{"messages": [{"role": "system", "content": "{}"}, {"role": "user", "content": "{}"}], "max_tokens": 300, "temperature": 0.9}', 23, False, '1b696238-1fdd-4b95-bff0-86a37be13c78', '1b696238-1fdd-4b95-bff0-86a37be13c78', 'text', 'poor'),
        ('gemma-3-12b-it', 'google/gemma-3-12b-it:free', '{"messages": [{"role": "system", "content": "{}"}, {"role": "user", "content": "{}"}], "max_tokens": 300, "temperature": 0.9}', 17, False, '1b696238-1fdd-4b95-bff0-86a37be13c78', '1b696238-1fdd-4b95-bff0-86a37be13c78', 'text', 'limited'),
        ('gemma-3-27b-it', 'google/gemma-3-27b-it:free', '{"messages": [{"role": "system", "content": "{}"}, {"role": "user", "content": "{}"}], "max_tokens": 300, "temperature": 0.9}', 26, False, '1b696238-1fdd-4b95-bff0-86a37be13c78', '1b696238-1fdd-4b95-bff0-86a37be13c78', 'text', 'rejected'),
        ('gemma-3-4b-it', 'google/gemma-3-4b-it:free', '{"messages": [{"role": "system", "content": "{}"}, {"role": "user", "content": "{}"}], "max_tokens": 300, "temperature": 0.9}', 16, False, '1b696238-1fdd-4b95-bff0-86a37be13c78', '1b696238-1fdd-4b95-bff0-86a37be13c78', 'text', 'limited'),
        ('gemma-3n-e2b-it', 'google/gemma-3n-e2b-it:free', '{"messages": [{"role": "system", "content": "{}"}, {"role": "user", "content": "{}"}], "max_tokens": 300, "temperature": 0.9}', 14, False, '1b696238-1fdd-4b95-bff0-86a37be13c78', '1b696238-1fdd-4b95-bff0-86a37be13c78', 'text', 'limited'),
        ('gemma-3n-e4b-it', 'google/gemma-3n-e4b-it:free', '{"messages": [{"role": "system", "content": "{}"}, {"role": "user", "content": "{}"}], "max_tokens": 300, "temperature": 0.9}', 15, False, '1b696238-1fdd-4b95-bff0-86a37be13c78', '1b696238-1fdd-4b95-bff0-86a37be13c78', 'text', 'limited'),
        ('glm-4.5-air', 'z-ai/glm-4.5-air:free', '{"messages": [{"role": "system", "content": "{}"}, {"role": "user", "content": "{}"}], "max_tokens": 300, "temperature": 0.9}', 13, False, '1b696238-1fdd-4b95-bff0-86a37be13c78', '1b696238-1fdd-4b95-bff0-86a37be13c78', 'text', 'limited'),
        ('hermes-3-llama-3.1-405b', 'nousresearch/hermes-3-llama-3.1-405b:free', '{"messages": [{"role": "system", "content": "{}"}, {"role": "user", "content": "{}"}], "max_tokens": 300, "temperature": 0.9}', 20, False, '1b696238-1fdd-4b95-bff0-86a37be13c78', '1b696238-1fdd-4b95-bff0-86a37be13c78', 'text', 'poor'),
        ('kling-video/v1.6/standard', 'fal-ai/kling-video/v1.6/standard/text-to-video', '{"prompt": "{}", "duration": "{:d}", "aspect_ratio": "{:d}:{:d}"}', 2, True, '0c8d1e1c-fe65-45d3-a1c3-be69e7941e17', '0c8d1e1c-fe65-45d3-a1c3-be69e7941e17', 'text-to-video', 'good'),
        ('lfm-2.5-1.2b-instruct', 'liquid/lfm-2.5-1.2b-instruct:free', '{"messages": [{"role": "system", "content": "{}"}, {"role": "user", "content": "{}"}], "max_tokens": 300, "temperature": 0.9}', 7, False, '1b696238-1fdd-4b95-bff0-86a37be13c78', '1b696238-1fdd-4b95-bff0-86a37be13c78', 'text', 'limited'),
        ('lfm-2.5-1.2b-thinking', 'liquid/lfm-2.5-1.2b-thinking:free', '{"messages": [{"role": "system", "content": "{}"}, {"role": "user", "content": "{}"}], "max_tokens": 300, "temperature": 0.9}', 21, False, '1b696238-1fdd-4b95-bff0-86a37be13c78', '1b696238-1fdd-4b95-bff0-86a37be13c78', 'text', 'poor'),
        ('llama-3.1-8b-instruct', 'meta-llama/llama-3.1-8b-instruct', '{"messages": [{"role": "system", "content": "{}"}, {"role": "user", "content": "{}"}], "max_tokens": 300, "temperature": 0.9}', 4, True, '1b696238-1fdd-4b95-bff0-86a37be13c78', '1b696238-1fdd-4b95-bff0-86a37be13c78', 'text', 'good'),
        ('llama-3.2-3b-instruct', 'meta-llama/llama-3.2-3b-instruct:free', '{"messages": [{"role": "system", "content": "{}"}, {"role": "user", "content": "{}"}], "max_tokens": 300, "temperature": 0.9}', 19, False, '1b696238-1fdd-4b95-bff0-86a37be13c78', '1b696238-1fdd-4b95-bff0-86a37be13c78', 'text', 'limited'),
        ('llama-3.3-70b-instruct', 'meta-llama/llama-3.3-70b-instruct:free', '{"messages": [{"role": "system", "content": "{}"}, {"role": "user", "content": "{}"}], "max_tokens": 300, "temperature": 0.9}', 18, False, '1b696238-1fdd-4b95-bff0-86a37be13c78', '1b696238-1fdd-4b95-bff0-86a37be13c78', 'text', 'limited'),
        ('minimax-m2.5', 'minimax/minimax-m2.5:free', '{"messages": [{"role": "system", "content": "{}"}, {"role": "user", "content": "{}"}], "max_tokens": 300, "temperature": 0.9}', 29, False, '1b696238-1fdd-4b95-bff0-86a37be13c78', '1b696238-1fdd-4b95-bff0-86a37be13c78', 'text', 'rejected'),
        ('minimax/video-01', 'fal-ai/minimax/video-01', '{"prompt": "{}", "duration": "{:d}s", "aspect_ratio": "{:d}:{:d}"}', 4, False, '0c8d1e1c-fe65-45d3-a1c3-be69e7941e17', '0c8d1e1c-fe65-45d3-a1c3-be69e7941e17', 'text-to-video', 'poor'),
        ('mistral-7b-instruct', 'mistralai/mistral-7b-instruct-v0.1', '{"messages": [{"role": "system", "content": "{}"}, {"role": "user", "content": "{}"}], "max_tokens": 300, "temperature": 0.9}', 28, False, '1b696238-1fdd-4b95-bff0-86a37be13c78', '1b696238-1fdd-4b95-bff0-86a37be13c78', 'text', 'rejected'),
        ('nemotron-3-nano-30b', 'nvidia/nemotron-3-nano-30b-a3b:free', '{"messages": [{"role": "system", "content": "{}"}, {"role": "user", "content": "{}"}], "max_tokens": 300, "temperature": 0.9}', 8, False, '1b696238-1fdd-4b95-bff0-86a37be13c78', '1b696238-1fdd-4b95-bff0-86a37be13c78', 'text', 'limited'),
        ('nemotron-3-super-120b', 'nvidia/nemotron-3-super-120b-a12b:free', '{"messages": [{"role": "system", "content": "{}"}, {"role": "user", "content": "{}"}], "max_tokens": 300, "temperature": 0.9}', 5, False, '1b696238-1fdd-4b95-bff0-86a37be13c78', '1b696238-1fdd-4b95-bff0-86a37be13c78', 'text', 'limited'),
        ('nemotron-nano-12b-v2-vl', 'nvidia/nemotron-nano-12b-v2-vl:free', '{"messages": [{"role": "system", "content": "{}"}, {"role": "user", "content": "{}"}], "max_tokens": 300, "temperature": 0.9}', 10, False, '1b696238-1fdd-4b95-bff0-86a37be13c78', '1b696238-1fdd-4b95-bff0-86a37be13c78', 'text', 'limited'),
        ('nemotron-nano-9b-v2', 'nvidia/nemotron-nano-9b-v2:free', '{"messages": [{"role": "system", "content": "{}"}, {"role": "user", "content": "{}"}], "max_tokens": 300, "temperature": 0.9}', 12, False, '1b696238-1fdd-4b95-bff0-86a37be13c78', '1b696238-1fdd-4b95-bff0-86a37be13c78', 'text', 'limited'),
        ('openchat-7b', 'openchat/openchat-7b:free', '{"messages": [{"role": "system", "content": "{}"}, {"role": "user", "content": "{}"}], "max_tokens": 300, "temperature": 0.9}', 30, False, '1b696238-1fdd-4b95-bff0-86a37be13c78', '1b696238-1fdd-4b95-bff0-86a37be13c78', 'text', 'rejected'),
        ('phi-3-mini-128k-instruct', 'microsoft/phi-3-mini-128k-instruct:free', '{"messages": [{"role": "system", "content": "{}"}, {"role": "user", "content": "{}"}], "max_tokens": 300, "temperature": 0.9}', 27, False, '1b696238-1fdd-4b95-bff0-86a37be13c78', '1b696238-1fdd-4b95-bff0-86a37be13c78', 'text', 'rejected'),
        ('qwen3-coder', 'qwen/qwen3-coder:free', '{"messages": [{"role": "system", "content": "{}"}, {"role": "user", "content": "{}"}], "max_tokens": 300, "temperature": 0.9}', 22, False, '1b696238-1fdd-4b95-bff0-86a37be13c78', '1b696238-1fdd-4b95-bff0-86a37be13c78', 'text', 'poor'),
        ('qwen3-next-80b', 'qwen/qwen3-next-80b-a3b-instruct:free', '{"messages": [{"role": "system", "content": "{}"}, {"role": "user", "content": "{}"}], "max_tokens": 300, "temperature": 0.9}', 11, False, '1b696238-1fdd-4b95-bff0-86a37be13c78', '1b696238-1fdd-4b95-bff0-86a37be13c78', 'text', 'limited'),
        ('qwen3.6-plus-preview', 'qwen/qwen3.6-plus-preview:free', '{"messages": [{"role": "system", "content": "{}"}, {"role": "user", "content": "{}"}], "max_tokens": 300, "temperature": 0.9}', 31, False, '1b696238-1fdd-4b95-bff0-86a37be13c78', '1b696238-1fdd-4b95-bff0-86a37be13c78', 'text', 'rejected'),
        ('sora-2', 'fal-ai/sora-2/text-to-video', '{"prompt": "{}", "duration": "{:int}", "aspect_ratio": "{:d}:{:d}"}', 1, True, '0c8d1e1c-fe65-45d3-a1c3-be69e7941e17', '0c8d1e1c-fe65-45d3-a1c3-be69e7941e17', 'text-to-video', 'good'),
        ('trinity-large-preview', 'arcee-ai/trinity-large-preview:free', '{"messages": [{"role": "system", "content": "{}"}, {"role": "user", "content": "{}"}], "max_tokens": 300, "temperature": 0.9}', 6, False, '1b696238-1fdd-4b95-bff0-86a37be13c78', '1b696238-1fdd-4b95-bff0-86a37be13c78', 'text', 'limited'),
        ('trinity-mini', 'arcee-ai/trinity-mini:free', '{"messages": [{"role": "system", "content": "{}"}, {"role": "user", "content": "{}"}], "max_tokens": 300, "temperature": 0.9}', 9, False, '1b696238-1fdd-4b95-bff0-86a37be13c78', '1b696238-1fdd-4b95-bff0-86a37be13c78', 'text', 'limited'),
        ('veo2', 'fal-ai/veo2', '{"prompt": "{}", "duration": "{:d}s", "aspect_ratio": "{:d}:{:d}"}', 3, True, '0c8d1e1c-fe65-45d3-a1c3-be69e7941e17', '0c8d1e1c-fe65-45d3-a1c3-be69e7941e17', 'text-to-video', 'good'),
    ]
    for (name, url, body, order, active, ai_platform_id, platform_id, type_, grade) in updates:
        cur.execute("""
            UPDATE ai_models
            SET url            = %s,
                body           = %s::jsonb,
                "order"        = %s,
                active         = %s,
                ai_platform_id = %s,
                platform_id    = %s,
                type           = %s,
                grade          = %s
            WHERE name = %s
        """, (url, body, order, active, ai_platform_id, platform_id, type_, grade, name))

    # openrouter/free — ai_platform_id специально NULL на деве
    cur.execute("""
        UPDATE ai_models
        SET url            = 'openrouter/free',
            body           = '{"messages": [{"role": "system", "content": "{}"}, {"role": "user", "content": "{}"}], "max_tokens": 300, "temperature": 0.9}'::jsonb,
            "order"        = 24,
            active         = false,
            ai_platform_id = NULL,
            platform_id    = '1b696238-1fdd-4b95-bff0-86a37be13c78',
            type           = 'text',
            grade          = 'fallback'
        WHERE name = 'openrouter/free'
    """)

    # Вставляем ltx-video (если нет)
    cur.execute("""
        INSERT INTO ai_models (id, name, url, body, "order", active, ai_platform_id, platform_id, type, grade)
        SELECT
            'e268ac99-2a94-47c1-9e37-b019e6d024be',
            'ltx-video',
            'fal-ai/ltx-video',
            '{"width": "{:int}", "height": "{:int}", "prompt": "{}", "num_frames": "{:int}"}'::jsonb,
            5, false,
            '0c8d1e1c-fe65-45d3-a1c3-be69e7941e17',
            '0c8d1e1c-fe65-45d3-a1c3-be69e7941e17',
            'text-to-video',
            'poor'
        WHERE NOT EXISTS (SELECT 1 FROM ai_models WHERE name = 'ltx-video')
    """)

    # Вставляем wan-2.6 (если нет)
    cur.execute("""
        INSERT INTO ai_models (id, name, url, body, "order", active, ai_platform_id, platform_id, type, grade)
        SELECT
            '224e7552-a2aa-49fe-9d5c-5cb274491cef',
            'wan-2.6',
            'wan/v2.6/text-to-video',
            '{"prompt": "{}", "duration": "{}", "resolution": "1080p", "aspect_ratio": "{0}:{1}"}'::jsonb,
            6, false,
            '0c8d1e1c-fe65-45d3-a1c3-be69e7941e17',
            '0c8d1e1c-fe65-45d3-a1c3-be69e7941e17',
            'text-to-video',
            'poor'
        WHERE NOT EXISTS (SELECT 1 FROM ai_models WHERE name = 'wan-2.6')
    """)


def _m011_fix_fal_platform_url(cur):
    """
    Убирает лишний суффикс /fal-ai из URL платформы fal.
    model_url в ai_models уже содержит 'fal-ai/...',
    поэтому platform_url должен быть https://queue.fal.run (без /fal-ai),
    иначе submit_url = platform_url + '/' + model_url даёт двойной fal-ai.
    Deployed: -
    """
    cur.execute("""
        UPDATE ai_platforms
           SET url = 'https://queue.fal.run'
         WHERE name = 'fal'
           AND url = 'https://queue.fal.run/fal-ai'
    """)


def _m013_sync_video_models(cur):
    """
    Полная синхронизация video-моделей с девом:
    1. Добавляет колонку price (TEXT) в ai_models.
    2. Вставляет seedance/v1.5/pro и ltx-2.3 (идемпотентно).
    3. Обновляет order, active, price для всех text-to-video моделей.
    Deployed: -
    """
    # 1. Добавить колонку price
    cur.execute("""
        ALTER TABLE ai_models
            ADD COLUMN IF NOT EXISTS price TEXT
    """)

    # 2. Вставить seedance/v1.5/pro (если нет)
    cur.execute("""
        INSERT INTO ai_models
            (id, name, url, body, "order", active, ai_platform_id, platform_id, type, grade)
        SELECT
            'b5e3f891-2c4a-4d67-9e8f-1a2b3c4d5e6f',
            'seedance/v1.5/pro',
            'fal-ai/bytedance/seedance/v1.5/pro/text-to-video',
            '{"prompt": "{}", "aspect_ratio": "{0}:{1}", "duration": "{}", "resolution": "720p"}'::jsonb,
            1, true,
            '0c8d1e1c-fe65-45d3-a1c3-be69e7941e17',
            '0c8d1e1c-fe65-45d3-a1c3-be69e7941e17',
            'text-to-video',
            'good'
        WHERE NOT EXISTS (SELECT 1 FROM ai_models WHERE name = 'seedance/v1.5/pro')
    """)

    # 3. Вставить ltx-2.3 (если нет)
    cur.execute("""
        INSERT INTO ai_models
            (id, name, url, body, "order", active, ai_platform_id, platform_id, type, grade)
        SELECT
            'd3e4f5a6-b7c8-4d91-ae2f-1b2c3d4e5f6a',
            'ltx-2.3',
            'fal-ai/ltx-2.3/text-to-video',
            '{"prompt": "{}", "duration": "{:int}", "resolution": "1080p", "aspect_ratio": "{}:{}"}'::jsonb,
            2, false,
            '0c8d1e1c-fe65-45d3-a1c3-be69e7941e17',
            '0c8d1e1c-fe65-45d3-a1c3-be69e7941e17',
            'text-to-video',
            'good'
        WHERE NOT EXISTS (SELECT 1 FROM ai_models WHERE name = 'ltx-2.3')
    """)

    # 4. Обновить order, active, price для всех video-моделей
    video_updates = [
        ('seedance/v1.5/pro',         1, True,  '0.520 $/10сек'),
        ('ltx-2.3',                   2, False, '0.600 $/10сек'),
        ('sora-2',                    3, False, '1.000 $/10сек'),
        ('kling-video/v1.6/standard', 4, False, '0.450 $/10сек'),
        ('veo2',                      5, False, '5.000 $/10сек'),
        ('minimax/video-01',          6, False, '0.830 $/10сек'),
        ('ltx-video',                 7, False, None),
        ('wan-2.6',                   8, False, '1.500 $/10сек'),
    ]
    for name, order, active, price in video_updates:
        cur.execute("""
            UPDATE ai_models
               SET "order" = %s,
                   active  = %s,
                   price   = %s
             WHERE name = %s
        """, (order, active, price, name))


def _m012_gemma_no_system_role(cur):
    """
    Убирает role=system из body.messages у всех Gemma-моделей (google/gemma-*).
    Google AI Studio не поддерживает system-роль ('Developer instruction is not enabled').
    После миграции шаблон содержит только user-сообщение; _build_body в story.py
    при отсутствии system-роли склеивает system_prompt + user_prompt в одно user-сообщение.
    Deployed: -
    """
    cur.execute("""
        UPDATE ai_models
           SET body = jsonb_set(
               body,
               '{messages}',
               (
                   SELECT jsonb_agg(msg ORDER BY ordinality)
                   FROM jsonb_array_elements(body->'messages') WITH ORDINALITY AS t(msg, ordinality)
                   WHERE msg->>'role' != 'system'
               )
           )
         WHERE url ILIKE 'google/gemma%'
           AND body->'messages' @> '[{"role": "system"}]'
    """)


def _m014_drop_batches_unique_constraint(cur):
    """
    Снимает уникальное ограничение batches_scheduled_at_target_id_key.
    Дубли предотвращает программная логика планировщика, а не БД.
    Ограничение блокировало создание новых батчей для слотов, у которых
    уже есть отменённый батч с тем же (scheduled_at, target_id).
    """
    cur.execute("""
        ALTER TABLE batches
            DROP CONSTRAINT IF EXISTS batches_scheduled_at_target_id_key
    """)


def _m015_fix_cancelled_status(cur):
    """
    Приводит статус отмены батча к английскому виду.
    Заменяет 'отменён' → 'cancelled' и удаляет несуществующий статус 'устарел'.
    """
    cur.execute("""
        UPDATE batches
        SET status = 'cancelled'
        WHERE status IN ('отменён', 'устарел')
    """)


def _m016_targets_config(cur):
    """
    Добавляет колонку config (JSONB) в таблицу targets для хранения
    платформо-специфичных параметров. Для VKontakte сохраняет group_id сообщества.
    """
    cur.execute("""
        ALTER TABLE targets
            ADD COLUMN IF NOT EXISTS config JSONB NOT NULL DEFAULT '{}'
    """)
    cur.execute("""
        UPDATE targets SET config = '{"group_id": 236929597}' WHERE name = 'VKontakte'
    """)


def _m017_dzen_publisher_id(cur):
    """
    Устанавливает publisher_id для таргета Дзен в поле config.
    Запускается после _m016_targets_config (которая добавила колонку config).
    Идемпотентно: обновляет только если publisher_id ещё не задан.
    """
    cur.execute("""
        UPDATE targets
        SET config = config || '{"publisher_id": "69d3ec6ae9ff6e1b8d5c6326"}'::jsonb
        WHERE name = 'Дзен'
          AND (config->>'publisher_id') IS NULL
    """)


def _m018_targets_browser_session(cur):
    """
    Добавляет колонку browser_session (JSONB) в таблицу targets.
    Хранит Playwright storage state (cookies + localStorage) для авторизованной сессии Дзен.
    """
    cur.execute("""
        ALTER TABLE targets
            ADD COLUMN IF NOT EXISTS browser_session JSONB DEFAULT NULL
    """)


def _m019_rename_session_context(cur):
    """
    Переименовывает колонку browser_session → session_context в таблице targets.
    """
    cur.execute("""
        ALTER TABLE targets
            RENAME COLUMN browser_session TO session_context
    """)


def _m020_targets_slug_and_publish_methods(cur):
    """
    Добавляет поле slug в targets и обогащает config publish_method.*.
    VKontakte → slug=vk, publish_method.story и publish_method.wall берутся из
    глобальных настроек vk_publish_story / vk_publish_wall (по умолчанию 1).
    Дзен → slug=dzen, publish_method.video=1.
    Deployed: -
    """
    cur.execute("""
        ALTER TABLE targets
            ADD COLUMN IF NOT EXISTS slug VARCHAR(50)
    """)
    cur.execute("""
        UPDATE targets
        SET slug = 'vk'
        WHERE name = 'VKontakte' AND (slug IS NULL OR slug = '')
    """)
    cur.execute("""
        UPDATE targets
        SET slug = 'dzen'
        WHERE name = 'Дзен' AND (slug IS NULL OR slug = '')
    """)
    cur.execute("""
        WITH settings AS (
            SELECT
                COALESCE((SELECT value FROM settings WHERE key = 'vk_publish_story'), '1') AS story_val,
                COALESCE((SELECT value FROM settings WHERE key = 'vk_publish_wall'),  '1') AS wall_val
        )
        UPDATE targets
        SET config = config
            || jsonb_build_object(
                'publish_method', jsonb_build_object(
                    'story', (SELECT CASE WHEN story_val = '1' THEN 1 ELSE 0 END FROM settings),
                    'wall',  (SELECT CASE WHEN wall_val  = '1' THEN 1 ELSE 0 END FROM settings)
                )
            )
        WHERE name = 'VKontakte'
          AND (config->'publish_method') IS NULL
    """)
    cur.execute("""
        UPDATE targets
        SET config = config || '{"publish_method": {"video": 1}}'::jsonb
        WHERE name = 'Дзен'
          AND (config->'publish_method') IS NULL
    """)


def _m021_migrate_old_batch_statuses(cur):
    """
    Переводит батчи из старых промежуточных статусов в новые составные.
    story_posted  → vk.story.published
    wall_posting  → vk.wall.posting
    Deployed: -
    """
    cur.execute("""
        UPDATE batches
        SET status = 'vk.story.published'
        WHERE status = 'story_posted'
    """)
    cur.execute("""
        UPDATE batches
        SET status = 'vk.wall.posting'
        WHERE status = 'wall_posting'
    """)


def _m022_widen_batches_status(cur):
    """
    Расширяет batches.status с VARCHAR(30) до TEXT.
    Составные статусы {slug}.{method}.{phase} могут превышать 30 символов
    при произвольных slug/method именах.
    Deployed: -
    """
    cur.execute("ALTER TABLE batches ALTER COLUMN status TYPE TEXT")


def _m023_stories_title(cur):
    """
    Добавляет поле title TEXT NOT NULL DEFAULT '' в таблицу stories.
    Заполняет title и при необходимости result для существующих записей по правилам:
      - первая строка result не содержит '.' → title = первая_строка.rstrip('.'),
        result = остаток.strip()
      - первая строка содержит '.' → title = первые 4 слова.rstrip('.'),
        result не меняется
    Deployed: -
    """
    cur.execute("""
        ALTER TABLE stories
            ADD COLUMN IF NOT EXISTS title TEXT NOT NULL DEFAULT ''
    """)

    cur.execute("SELECT id, result FROM stories WHERE title = ''")
    rows = cur.fetchall()
    for row in rows:
        story_id, result = row[0], row[1]
        if not result:
            continue
        first_line = result.split('\n')[0]
        if '.' not in first_line:
            title = first_line.rstrip('.')
            new_result = result[len(first_line):].strip()
            cur.execute(
                "UPDATE stories SET title = %s, result = %s WHERE id = %s",
                (title, new_result, story_id),
            )
        else:
            title = ' '.join(result.split()[:4]).rstrip('.')
            cur.execute(
                "UPDATE stories SET title = %s WHERE id = %s",
                (title, story_id),
            )


def _m024_batches_type_replace_adhoc_target_id(cur):
    """
    Заменяет adhoc (BOOLEAN) и target_id (UUID FK) на type (TEXT NOT NULL) в таблице batches.

    Правила переноса данных:
      adhoc=FALSE                          → type='slot'
      adhoc=TRUE AND target_id IS NOT NULL → type='adhoc'
      adhoc=TRUE AND target_id IS NULL     → type='probe'

    Удаляет колонки adhoc и target_id, а также FK-ограничение на targets.
    Deployed: -
    """
    cur.execute("""
        ALTER TABLE batches
            ADD COLUMN IF NOT EXISTS type TEXT NOT NULL DEFAULT 'slot'
    """)
    cur.execute("""
        UPDATE batches
           SET type = CASE
               WHEN adhoc = FALSE                          THEN 'slot'
               WHEN adhoc = TRUE AND target_id IS NOT NULL THEN 'adhoc'
               WHEN adhoc = TRUE AND target_id IS NULL     THEN 'probe'
               ELSE 'slot'
           END
    """)
    cur.execute("""
        ALTER TABLE batches
            ALTER COLUMN type DROP DEFAULT
    """)
    cur.execute("""
        DO $$ BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.table_constraints
                WHERE table_name = 'batches'
                  AND constraint_name = 'batches_target_id_fkey'
            ) THEN
                ALTER TABLE batches DROP CONSTRAINT batches_target_id_fkey;
            END IF;
        END $$
    """)
    cur.execute("""
        ALTER TABLE batches
            DROP COLUMN IF EXISTS adhoc,
            DROP COLUMN IF EXISTS target_id
    """)


def _m025_drop_all_foreign_keys(cur):
    """
    Удаляет все оставшиеся FK-ограничения из схемы.
    Целостность данных обеспечивается на уровне приложения.
    Deployed: -
    """
    constraints = [
        ("ai_models",   "ai_models_platform_id_fkey"),
        ("ai_models",   "ai_models_ai_platform_id_fkey"),
        ("batches",     "batches_story_id_fkey"),
        ("batches",     "batches_video_model_id_fkey"),
        ("batches",     "batches_text_model_id_fkey"),
        ("log",         "log_batch_id_fkey"),
        ("log_entries", "log_entries_log_id_fkey"),
        ("stories",     "stories_model_id_fkey"),
    ]
    for table, name in constraints:
        cur.execute(f"""
            DO $$ BEGIN
                IF EXISTS (
                    SELECT 1 FROM information_schema.table_constraints
                    WHERE table_name = '{table}' AND constraint_name = '{name}'
                ) THEN
                    ALTER TABLE {table} DROP CONSTRAINT {name};
                END IF;
            END $$
        """)


def _m026_users(cur):
    """
    Создаёт таблицы user_roles и users для системы авторизации.
    user_roles: id (UUID), name, slug
    users: id (UUID), name, login, password, role_id
    Deployed: -
    """
    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_roles (
            id   UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
            name VARCHAR(200) NOT NULL,
            slug VARCHAR(100) NOT NULL UNIQUE
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id       UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
            name     VARCHAR(200) NOT NULL,
            login    VARCHAR(200) NOT NULL UNIQUE,
            password VARCHAR(500) NOT NULL,
            role_id  UUID
        )
    """)


def _m027_sync_vk_publish_method_from_settings(cur):
    """Синхронизирует publish_method VK-таргета из настроек (vk_publish_story / vk_publish_wall).
    Исправляет рассинхрон: настройки хранились в db_get/db_set, а _build_steps читает target.config."""
    cur.execute("SELECT value FROM settings WHERE key = 'vk_publish_story' LIMIT 1")
    row = cur.fetchone()
    story_on = (row[0] if row else "1") == "1"

    cur.execute("SELECT value FROM settings WHERE key = 'vk_publish_wall' LIMIT 1")
    row = cur.fetchone()
    wall_on = (row[0] if row else "0") == "1"

    import json as _json
    methods = _json.dumps({"story": 1 if story_on else 0, "wall": 1 if wall_on else 0})
    cur.execute(
        "UPDATE targets SET config = config || jsonb_build_object('publish_method', %s::jsonb)"
        " WHERE slug = 'vk'",
        (methods,),
    )


def _m029_targets_order(cur):
    """Добавляет столбец order в targets и выставляет vk=1, dzen=2."""
    cur.execute('ALTER TABLE targets ADD COLUMN IF NOT EXISTS "order" INTEGER')
    cur.execute('UPDATE targets SET "order" = 1 WHERE slug = \'vk\'')
    cur.execute('UPDATE targets SET "order" = 2 WHERE slug = \'dzen\'')


def _m028_fill_null_story_ids(cur):
    """Заполняет batches.story_id случайными значениями из stories там, где он NULL.
    Работает только если в таблице stories есть хотя бы одна запись."""
    cur.execute("SELECT COUNT(*) FROM stories")
    count = cur.fetchone()[0]
    if count == 0:
        return
    cur.execute("""
        UPDATE batches
        SET story_id = (SELECT id FROM stories ORDER BY random() LIMIT 1)
        WHERE story_id IS NULL
    """)


def _m029_user_role_links(cur):
    """
    Переход users↔roles к many-to-many.
    - Создаёт таблицу user_role_links (user_id UUID, role_id UUID, PK составной).
    - Переносит существующие назначения из users.role_id в user_role_links (только если колонка ещё существует).
    - Удаляет колонку role_id из users.
    Deployed: -
    """
    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_role_links (
            user_id UUID NOT NULL,
            role_id UUID NOT NULL,
            PRIMARY KEY (user_id, role_id)
        )
    """)
    cur.execute("""
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'users' AND column_name = 'role_id'
    """)
    if cur.fetchone():
        cur.execute("""
            INSERT INTO user_role_links (user_id, role_id)
            SELECT id, role_id FROM users WHERE role_id IS NOT NULL
            ON CONFLICT DO NOTHING
        """)
    cur.execute("""
        ALTER TABLE users DROP COLUMN IF EXISTS role_id
    """)


def _m030_user_roles_module_field(cur):
    """
    Добавляет колонку module (TEXT NOT NULL DEFAULT '') в таблицу user_roles.
    Обновляет существующие строки: root→ROOT, producer→PRODUCER, operator→OPERATOR.
    Deployed: -
    """
    cur.execute("""
        ALTER TABLE user_roles
            ADD COLUMN IF NOT EXISTS module TEXT NOT NULL DEFAULT ''
    """)
    cur.execute("""
        UPDATE user_roles SET module = 'ROOT'     WHERE slug = 'root'
    """)
    cur.execute("""
        UPDATE user_roles SET module = 'PRODUCER' WHERE slug = 'producer'
    """)
    cur.execute("""
        UPDATE user_roles SET module = 'OPERATOR' WHERE slug = 'operator'
    """)


MIGRATIONS = [
    (1, _m001_baseline_schema),
    (2, _m002_model_grades_and_batch_models),
    (3, _m003_schedule_created_at),
    (4, _m004_seed_ai_models),
    (5, _m005_batch_original_video),
    (6, _m006_target_transcode),
    (7, _m007_rename_video_data),
    (8, _m008_fix_ai_model_grades),
    (9, _m009_batches_target_nullable),
    (10, _m010_sync_ai_models_from_dev),
    (11, _m011_fix_fal_platform_url),
    (12, _m012_gemma_no_system_role),
    (13, _m013_sync_video_models),
    (14, _m014_drop_batches_unique_constraint),
    (15, _m015_fix_cancelled_status),
    (16, _m016_targets_config),
    (17, _m017_dzen_publisher_id),
    (18, _m018_targets_browser_session),
    (19, _m019_rename_session_context),
    (20, _m020_targets_slug_and_publish_methods),
    (21, _m021_migrate_old_batch_statuses),
    (22, _m022_widen_batches_status),
    (23, _m023_stories_title),
    (24, _m024_batches_type_replace_adhoc_target_id),
    (25, _m025_drop_all_foreign_keys),
    (26, _m026_users),
    (27, _m027_sync_vk_publish_method_from_settings),
    (28, _m028_fill_null_story_ids),
    (29, _m029_targets_order),
    (30, _m029_user_role_links),
    (31, _m030_user_roles_module_field),
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
