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
            ai_platform_id UUID,
            platform_id    UUID,
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
            model_id   UUID
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS batches (
            id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
            scheduled_at TIMESTAMPTZ,
            target_id    UUID        NOT NULL,
            status       VARCHAR(30) NOT NULL DEFAULT 'pending',
            story_id     UUID,
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
            batch_id   UUID,
            pipeline   VARCHAR(30) NOT NULL,
            message    TEXT,
            status     VARCHAR(20),
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS log_entries (
            id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
            log_id     UUID        NOT NULL,
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
            ADD COLUMN IF NOT EXISTS text_model_id UUID
    """)
    cur.execute("""
        ALTER TABLE batches
            ADD COLUMN IF NOT EXISTS video_model_id UUID
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
        ('ltx-video',                 7, False, '0.02 $/видео'),
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
    Идемпотентна: пропускается если browser_session уже не существует.
    """
    cur.execute("""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name='targets' AND column_name='browser_session'
            ) THEN
                ALTER TABLE targets RENAME COLUMN browser_session TO session_context;
            END IF;
        END $$
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
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'batches' AND column_name = 'adhoc'
            ) THEN
                UPDATE batches
                   SET type = CASE
                       WHEN adhoc = FALSE                          THEN 'slot'
                       WHEN adhoc = TRUE AND target_id IS NOT NULL THEN 'adhoc'
                       WHEN adhoc = TRUE AND target_id IS NULL     THEN 'probe'
                       ELSE 'slot'
                   END;
            END IF;
        END $$
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


def _m032_seed_default_users(cur):
    """
    Гарантирует наличие дефолтных пользователей (root, producer, operator) и
    их связей с ролями. Idempotent: использует WHERE NOT EXISTS / ON CONFLICT DO NOTHING.
    Исправляет продакшн-БД, где seed_db() не отработал из-за поглощённой ошибки.
    """
    cur.execute("""
        INSERT INTO user_roles (name, slug, module)
        SELECT v.name, v.slug, v.module FROM (VALUES
            ('root',     'root',     'ROOT'),
            ('producer', 'producer', 'PRODUCER'),
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


def _m033_sync_targets_from_dev(cur):
    """
    Синхронизация таблицы targets с dev на прод.
    Сличает записи по полю name, обновляет aspect_ratio_x, aspect_ratio_y,
    active, transcode, config, slug. session_context не трогает.
    Deployed: -
    """
    rows = [
        {
            'name': 'VKontakte',
            'aspect_ratio_x': 9,
            'aspect_ratio_y': 16,
            'active': True,
            'transcode': True,
            'config': '{"group_id": 236929597, "publish_method": {"wall": 1, "story": 0}}',
            'slug': 'vk',
        },
        {
            'name': 'Дзен',
            'aspect_ratio_x': 9,
            'aspect_ratio_y': 16,
            'active': True,
            'transcode': True,
            'config': '{"publisher_id": "69d3ec6ae9ff6e1b8d5c6326", "publish_method": {"video": 1}}',
            'slug': 'dzen',
        },
    ]
    for r in rows:
        cur.execute("""
            UPDATE targets
               SET aspect_ratio_x = %s,
                   aspect_ratio_y = %s,
                   active         = %s,
                   transcode      = %s,
                   config         = %s::jsonb,
                   slug           = %s
             WHERE name = %s
        """, (
            r['aspect_ratio_x'],
            r['aspect_ratio_y'],
            r['active'],
            r['transcode'],
            r['config'],
            r['slug'],
            r['name'],
        ))


def _m034_stories_result_to_content(cur):
    """
    Переименовывает колонку result → content в таблице stories.
    Атомарно; данные не теряются.
    Идемпотентно: пропускает переименование, если result уже отсутствует.
    """
    cur.execute("""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'stories' AND column_name = 'result'
            ) THEN
                ALTER TABLE stories RENAME COLUMN result TO content;
            END IF;
        END $$
    """)


def _m035_drop_text_model_id(cur):
    """
    Переносит text_model_id из batches в stories.model_id.
    - Синхронизирует stories.model_id из batches.text_model_id (берём батч с MAX created_at по story_id)
    - Обновляет триггер check_batch_model_types — убирает проверку text_model_id
    - Дропает FK-constraint batches_text_model_id_fkey
    - Дропает колонку batches.text_model_id
    Deployed: -
    """
    cur.execute("""
        UPDATE stories s
        SET model_id = b.text_model_id
        FROM (
            SELECT DISTINCT ON (story_id) story_id, text_model_id
            FROM batches
            WHERE text_model_id IS NOT NULL AND story_id IS NOT NULL
            ORDER BY story_id, created_at DESC
        ) b
        WHERE s.id = b.story_id
          AND (s.model_id IS NULL OR s.model_id != b.text_model_id)
    """)
    cur.execute("""
        CREATE OR REPLACE FUNCTION trg_fn_batch_model_types()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
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
            IF EXISTS (
                SELECT 1 FROM pg_trigger
                WHERE tgname = 'trg_batch_model_types'
                  AND tgrelid = 'batches'::regclass
            ) THEN
                DROP TRIGGER trg_batch_model_types ON batches;
            END IF;
        END $$
    """)
    cur.execute("""
        DO $$ BEGIN
            IF EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'batches_text_model_id_fkey'
                  AND conrelid = 'batches'::regclass
            ) THEN
                ALTER TABLE batches DROP CONSTRAINT batches_text_model_id_fkey;
            END IF;
        END $$
    """)
    cur.execute("""
        DO $$ BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'batches' AND column_name = 'text_model_id'
            ) THEN
                ALTER TABLE batches DROP COLUMN text_model_id;
            END IF;
        END $$
    """)
    cur.execute("""
        CREATE TRIGGER trg_batch_model_types
        BEFORE INSERT OR UPDATE OF video_model_id ON batches
        FOR EACH ROW EXECUTE FUNCTION trg_fn_batch_model_types()
    """)


def _m036_movies_table(cur):
    """
    Выносит видео-данные из batches в отдельную таблицу movies.

    1. Создаёт таблицу movies (id, created_at, url, raw_data, transcoded_data, model_id).
    2. Переносит данные из batches → movies для батчей с видео.
    3. Добавляет movie_id (UUID, nullable) в batches.
    4. Заполняет batches.movie_id по перенесённым записям.
    5. Удаляет старые поля: video_url, video_file, video_data_original, video_data_transcoded,
       video_model_id.
    6. Удаляет триггер trg_batch_model_types и функцию trg_fn_batch_model_types.
    Deployed: -
    """
    cur.execute("""
        CREATE TABLE IF NOT EXISTS movies (
            id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
            created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
            url             TEXT,
            raw_data        BYTEA,
            transcoded_data BYTEA,
            model_id        UUID
        )
    """)

    cur.execute("""
        ALTER TABLE batches
            ADD COLUMN IF NOT EXISTS movie_id UUID
    """)

    cur.execute("""
        DO $$
        DECLARE
            rec RECORD;
            new_movie_id UUID;
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'batches' AND column_name = 'video_data_original'
            ) THEN
                FOR rec IN
                    SELECT id, video_url, video_data_original, video_data_transcoded, video_model_id
                    FROM batches
                    WHERE video_data_original IS NOT NULL
                       OR video_data_transcoded IS NOT NULL
                       OR video_url IS NOT NULL
                       OR video_model_id IS NOT NULL
                LOOP
                    INSERT INTO movies (url, raw_data, transcoded_data, model_id)
                    VALUES (rec.video_url, rec.video_data_original, rec.video_data_transcoded, rec.video_model_id)
                    RETURNING id INTO new_movie_id;

                    UPDATE batches SET movie_id = new_movie_id WHERE id = rec.id;
                END LOOP;
            END IF;
        END $$
    """)

    cur.execute("""
        DO $$ BEGIN
            IF EXISTS (
                SELECT 1 FROM pg_trigger
                WHERE tgname = 'trg_batch_model_types'
                  AND tgrelid = 'batches'::regclass
            ) THEN
                DROP TRIGGER trg_batch_model_types ON batches;
            END IF;
        END $$
    """)

    cur.execute("""
        DROP FUNCTION IF EXISTS trg_fn_batch_model_types()
    """)

    cur.execute("""
        ALTER TABLE batches
            DROP COLUMN IF EXISTS video_url,
            DROP COLUMN IF EXISTS video_file,
            DROP COLUMN IF EXISTS video_data_original,
            DROP COLUMN IF EXISTS video_data_transcoded,
            DROP COLUMN IF EXISTS video_model_id
    """)


def _m037_stories_grade(cur):
    """
    Добавляет поле grade TEXT в таблицу stories.
    Используется для хранения оценки качества сюжета (например 'good').
    """
    cur.execute("""
        ALTER TABLE stories
            ADD COLUMN IF NOT EXISTS grade TEXT
    """)


def _m038_stories_manual_changed(cur):
    """
    Добавляет поле manual_changed BOOLEAN NOT NULL DEFAULT FALSE в таблицу stories.
    Используется для отслеживания вручную изменённых сюжетов в панели Сценарист.
    """
    cur.execute("""
        ALTER TABLE stories
            ADD COLUMN IF NOT EXISTS manual_changed BOOLEAN NOT NULL DEFAULT FALSE
    """)
    cur.execute("""
        UPDATE stories SET manual_changed = FALSE
    """)


def _m039_delete_qwen36_plus_preview(cur):
    """
    Удаляет несуществующую модель qwen3.6-plus-preview из ai_models.
    На OpenRouter такого эндпоинта нет (HTTP 404), модель никогда не работала.
    Идемпотентно: DELETE ничего не делает, если запись уже отсутствует.
    """
    cur.execute("""
        DELETE FROM ai_models WHERE name = 'qwen3.6-plus-preview'
    """)


def _m040_rename_donating_to_donated(cur):
    """
    Переименовывает статус батча 'donating' → 'donated'.
    Статус терминальный (батч уже отдал свои данные), прошедшее время точнее.
    Идемпотентно: UPDATE не затронет строки, если 'donating' уже отсутствует.
    """
    cur.execute("""
        UPDATE batches SET status = 'donated' WHERE status = 'donating'
    """)


def _m041_rename_story_error_to_error(cur):
    """
    Переименовывает статус батча 'story_error' → 'error'.
    Статус story_error удалён из кода (Task #83), старые записи в БД приводятся
    к стандартному 'error'. Идемпотентно.
    """
    cur.execute("""
        UPDATE batches SET status = 'error' WHERE status = 'story_error'
    """)


def _m042_donor_refactor(cur):
    """
    Task #91: Рефакторинг механизма донора.

    1. Создаёт ХП claim_donor_batch(batch_id uuid) — атомарный захват донора
       с pg_advisory_xact_lock. Помечает донора как 'donated', записывает
       donor_batch_id в batches.data текущего батча.
    2. Создаёт функцию get_donor_count() — счётчик доступных доноров.
    3. Переименовывает статус 'probe' → 'movie_probe' у существующих батчей.
    """
    cur.execute("""
        CREATE OR REPLACE FUNCTION claim_donor_batch(p_batch_id uuid)
        RETURNS void LANGUAGE plpgsql AS $$
        DECLARE
            v_donor_id uuid;
        BEGIN
            PERFORM pg_advisory_xact_lock(hashtext('donor_claim'));

            SELECT b.id INTO v_donor_id
            FROM batches b
            JOIN movies m ON m.id = b.movie_id
            WHERE (m.transcoded_data IS NOT NULL OR m.raw_data IS NOT NULL)
              AND b.status IN ('cancelled', 'movie_probe')
            ORDER BY b.created_at ASC
            LIMIT 1
            FOR UPDATE OF b;

            IF v_donor_id IS NULL THEN
                RETURN;
            END IF;

            UPDATE batches SET status = 'donated' WHERE id = v_donor_id;

            UPDATE batches
            SET data = COALESCE(data, '{}'::jsonb)
                       || jsonb_build_object('donor_batch_id', v_donor_id::text)
            WHERE id = p_batch_id;
        END;
        $$
    """)

    cur.execute("""
        CREATE OR REPLACE FUNCTION get_donor_count()
        RETURNS bigint LANGUAGE plpgsql AS $$
        DECLARE
            v_count bigint;
        BEGIN
            PERFORM pg_advisory_xact_lock(hashtext('donor_claim'));

            SELECT COUNT(*) INTO v_count
            FROM batches b
            JOIN movies m ON m.id = b.movie_id
            WHERE (m.transcoded_data IS NOT NULL OR m.raw_data IS NOT NULL)
              AND b.status IN ('cancelled', 'movie_probe');

            RETURN v_count;
        END;
        $$
    """)

    cur.execute("""
        UPDATE batches SET status = 'movie_probe'
        WHERE status = 'probe'
    """)


def _m043_fix_remaining_probe_status(cur):
    """
    Страховочная миграция: конвертирует все оставшиеся записи 'probe' → 'movie_probe'.
    m042 уже делает это, но конкретная DB содержала батчи с status='probe' и type,
    не совпадающим с 'movie_probe' (исторические данные из m021). m043 гарантирует
    полную чистку независимо от type батча. Идемпотентно.
    """
    cur.execute("""
        UPDATE batches SET status = 'movie_probe'
        WHERE status = 'probe'
    """)


def _m044_ltx_video_price(cur):
    """Проставляет цену 0.02 $/видео для модели ltx-video."""
    cur.execute("""
        UPDATE ai_models SET price = '0.02 $/видео' WHERE name = 'ltx-video'
    """)


def _m045_add_indexes(cur):
    """
    Добавляет явные индексы на все колонки, участвующие в WHERE/JOIN/ORDER BY.
    Используется IF NOT EXISTS — идемпотентно.
    """
    indexes = [
        ("idx_batches_status",       "batches",     "status"),
        ("idx_batches_story_id",      "batches",     "story_id"),
        ("idx_batches_movie_id",      "batches",     "movie_id"),
        ("idx_batches_scheduled_at",  "batches",     "scheduled_at"),
        ("idx_batches_type",          "batches",     "type"),
        ("idx_batches_completed_at",  "batches",     "completed_at"),
        ("idx_batches_created_at",    "batches",     "created_at"),
        ("idx_stories_grade",         "stories",     "grade"),
        ("idx_stories_created_at",    "stories",     "created_at"),
        ("idx_stories_model_id",      "stories",     "model_id"),
        ("idx_movies_model_id",       "movies",      "model_id"),
        ("idx_log_batch_id",          "log",         "batch_id"),
        ("idx_log_created_at",        "log",         "created_at"),
        ("idx_log_entries_log_id",    "log_entries", "log_id"),
        ("idx_ai_models_type",        "ai_models",   "type"),
        ("idx_ai_models_active",      "ai_models",   "active"),
        ("idx_ai_models_platform_id", "ai_models",   "platform_id"),
        ("idx_targets_active",        "targets",     "active"),
        ("idx_targets_slug",          "targets",     "slug"),
        ("idx_schedule_time_utc",     "schedule",    "time_utc"),
    ]
    for idx_name, table, column in indexes:
        cur.execute(
            f'CREATE INDEX IF NOT EXISTS {idx_name} ON {table} ({column})'
        )


def _m046_drop_ai_platform_id(cur):
    """
    Удаляет дублирующее поле ai_platform_id из таблицы ai_models.
    Активным кодом используется только platform_id.
    Идемпотентно: DROP COLUMN IF EXISTS.
    """
    cur.execute("""
        ALTER TABLE ai_models
            DROP COLUMN IF EXISTS ai_platform_id
    """)


def _m047_log_status_cancelled(cur):
    """
    Переводит все записи таблицы log со статусом 'прервана' в 'cancelled'.
    В БД хранятся только английские статусы; 'прервана' было ошибочным
    русским значением, использовавшимся вместо 'cancelled'.
    """
    cur.execute("""
        UPDATE log SET status = 'cancelled' WHERE status = 'прервана'
    """)


def _m048_remove_model_name_from_batch_data(cur):
    """
    Удаляет ключ 'model_name' из поля data всех записей таблицы batches.
    Поле model_name было устаревшим костылем: имя модели теперь получается
    через JOIN с movies.model_id → ai_models.name.
    Начиная с этой миграции код не пишет и не читает model_name из batch.data.
    """
    cur.execute("""
        UPDATE batches
           SET data = data - 'model_name'
         WHERE data ? 'model_name'
    """)


def _m050_remove_story_probe_and_model_name_from_batch_data(cur):
    """
    Удаляет мусорные ключи 'story_probe' и 'model_name' из поля data всех строк batches.
    - story_probe: булев флаг True, писался при создании батча, нигде не читался из data.
      Тип батча определяется через колонку batches.type, а не через этот флаг.
    - model_name: устаревший костыль, убранный миграцией #48 из кода.
      Здесь вычищаем остатки из уже существующих строк.
    """
    cur.execute("""
        UPDATE batches
           SET data = data - 'story_probe' - 'model_name'
         WHERE data ? 'story_probe' OR data ? 'model_name'
    """)


def _m049_widen_log_entries_level(cur):
    """
    Расширяет log_entries.level с VARCHAR(10) до VARCHAR(20).
    Необходимо для поддержки уровня 'fatal_error' (11 символов),
    введённого как часть унификации обработки ошибок через AppException.
    Deployed: -
    """
    cur.execute("ALTER TABLE log_entries ALTER COLUMN level TYPE VARCHAR(20)")


def _m051_remove_obsolete_settings(cur):
    """
    Удаляет мёртвые ключи из таблицы settings:
    - vk_transcode — нигде в коде не используется
    - history_days — только в документации, кодом не читается
    Deployed: -
    """
    cur.execute("DELETE FROM settings WHERE key IN ('vk_transcode', 'history_days')")


def _m052_model_durations(cur):
    """
    Создаёт таблицу model_durations и заполняет её допустимыми значениями duration
    для всех 8 видео-моделей.
    Значение 0 означает, что модель не принимает параметр duration (без ограничений).
    """
    cur.execute("""
        CREATE TABLE IF NOT EXISTS model_durations (
            id        UUID    PRIMARY KEY DEFAULT gen_random_uuid(),
            model_id  UUID    NOT NULL,
            duration  INTEGER NOT NULL,
            UNIQUE (model_id, duration)
        )
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_model_durations_model_id
            ON model_durations (model_id)
    """)

    model_durations = {
        'seedance/v1.5/pro':         [4, 5, 6, 7, 8, 9, 10, 11, 12],
        'ltx-2.3':                   [6, 8, 10],
        'sora-2':                    [4, 8, 12, 16, 20],
        'kling-video/v1.6/standard': [5, 10],
        'veo2':                      [5, 6, 7, 8],
        'minimax/video-01':          [0],
        'ltx-video':                 [0],
        'wan-2.6':                   [5, 10, 15],
    }

    for model_name, durations in model_durations.items():
        cur.execute("SELECT id FROM ai_models WHERE name = %s", (model_name,))
        row = cur.fetchone()
        if not row:
            continue
        model_id = row[0]
        for d in durations:
            cur.execute("""
                INSERT INTO model_durations (model_id, duration)
                VALUES (%s, %s)
                ON CONFLICT (model_id, duration) DO NOTHING
            """, (model_id, d))


def _m053_log_entries_log_id_nullable(cur):
    """
    Снимает ограничение NOT NULL с поля log_id в таблице log_entries,
    чтобы можно было вставлять записи без привязки к конкретному log (log_id=NULL).
    """
    cur.execute("""
        ALTER TABLE log_entries
            ALTER COLUMN log_id DROP NOT NULL
    """)


def _m054_drop_remaining_foreign_keys(cur):
    """
    Удаляет batches_target_id_fkey и другие FK, которые могли пережить m025
    на старых базах. Использует IF EXISTS для безопасного выполнения.
    """
    cur.execute("""
        DO $$ DECLARE
            rec RECORD;
        BEGIN
            FOR rec IN
                SELECT tc.table_name, tc.constraint_name
                FROM information_schema.table_constraints tc
                WHERE tc.constraint_type = 'FOREIGN KEY'
                  AND tc.table_schema = 'public'
            LOOP
                EXECUTE format(
                    'ALTER TABLE %I DROP CONSTRAINT IF EXISTS %I',
                    rec.table_name, rec.constraint_name
                );
            END LOOP;
        END $$
    """)


def _m055_delete_vk_publish_settings(cur):
    """Удаляет устаревшие ключи vk_publish_story и vk_publish_wall из таблицы settings.
    Источником правды теперь служат targets.config.publish_method.story/wall."""
    cur.execute("DELETE FROM settings WHERE key IN ('vk_publish_story', 'vk_publish_wall')")


def _m056_deepseek_platform(cur):
    """
    Добавляет поддержку DeepSeek API как самостоятельной текстовой платформы.

    1. Добавляет колонку key_env (VARCHAR) в ai_platforms — имя переменной окружения с ключом API.
    2. Устанавливает key_env для существующих платформ: OpenRouter и fal.
    3. Вставляет платформу DeepSeek с url https://api.deepseek.com/chat/completions.
    4. Вставляет модель deepseek-chat (тип text, grade good, active).
    """
    cur.execute("""
        ALTER TABLE ai_platforms
            ADD COLUMN IF NOT EXISTS key_env VARCHAR(100)
    """)
    cur.execute("""
        UPDATE ai_platforms SET key_env = 'OPENROUTER_API_KEY' WHERE name = 'OpenRouter'
    """)
    cur.execute("""
        UPDATE ai_platforms SET key_env = 'FAL_API_KEY' WHERE name = 'fal'
    """)
    cur.execute("""
        INSERT INTO ai_platforms (name, url, key_env)
        SELECT 'DeepSeek', 'https://api.deepseek.com/chat/completions', 'DEEPSEEK_API_KEY'
        WHERE NOT EXISTS (SELECT 1 FROM ai_platforms WHERE name = 'DeepSeek')
    """)

    TEXT_BODY = (
        '{"messages": [{"role": "system", "content": "{}"},'
        ' {"role": "user", "content": "{}"}],'
        ' "max_tokens": 300, "temperature": 0.9}'
    )
    cur.execute("""
        INSERT INTO ai_models
            (name, url, body, "order", active, platform_id, type, grade)
        SELECT
            'deepseek-chat',
            'deepseek-chat',
            %s::jsonb,
            1,
            TRUE,
            p.id,
            'text',
            'good'
        FROM ai_platforms p
        WHERE p.name = 'DeepSeek'
          AND NOT EXISTS (SELECT 1 FROM ai_models WHERE name = 'deepseek-chat')
    """, (TEXT_BODY,))


def _m058_deepseek_chat_api_params(cur):
    """
    Фиксирует параметры API для модели deepseek-chat:
    temperature=0.2, top_p=0.9, presence_penalty=0.3, frequency_penalty=0.5.
    """
    TEXT_BODY = (
        '{"messages": [{"role": "system", "content": "{}"},'
        ' {"role": "user", "content": "{}"}],'
        ' "max_tokens": 300, "temperature": 0.7, "top_p": 0.9,'
        ' "presence_penalty": 0.3, "frequency_penalty": 0.5}'
    )
    cur.execute("""
        UPDATE ai_models SET body = %s::jsonb WHERE name = 'deepseek-chat'
    """, (TEXT_BODY,))


def _m059_rename_prompt_settings_keys(cur):
    """
    Переименовывает ключи настроек промптов, чтобы они соответствовали новым подписям UI:
    - 'system_prompt' → 'format_prompt'  (Форматный промпт)
    - 'metaprompt'    → 'text_prompt'    (Текстовый промпт)
    Deployed: -
    """
    cur.execute("""
        UPDATE settings SET key = 'format_prompt' WHERE key = 'system_prompt'
    """)
    cur.execute("""
        UPDATE settings SET key = 'text_prompt' WHERE key = 'metaprompt'
    """)


def _m060_stories_top_quality(cur):
    """
    Добавляет поле top_quality (BOOLEAN NOT NULL DEFAULT FALSE) в таблицу stories
    для пометки сюжетов образцового качества.
    """
    cur.execute("""
        ALTER TABLE stories
            ADD COLUMN IF NOT EXISTS top_quality BOOLEAN NOT NULL DEFAULT FALSE
    """)


def _m057_rename_model_id_keys_in_batches_data(cur):
    """
    Переименовывает устаревшие ключи в поле batches.data для устранения неоднозначности:
    - 'model_id'       → 'movie_model_id'  (видео-пайплайн)
    - 'probe_model_id' → 'story_model_id'  (story-probe пайплайн)

    Применяется к уже существующим строкам (in-flight и архивным батчам).
    """
    cur.execute("""
        UPDATE batches
           SET data = (data - 'model_id') || jsonb_build_object('movie_model_id', data->>'model_id')
         WHERE data ? 'model_id'
    """)
    cur.execute("""
        UPDATE batches
           SET data = (data - 'probe_model_id') || jsonb_build_object('story_model_id', data->>'probe_model_id')
         WHERE data ? 'probe_model_id'
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
    (32, _m032_seed_default_users),
    (33, _m033_sync_targets_from_dev),
    (34, _m034_stories_result_to_content),
    (35, _m035_drop_text_model_id),
    (36, _m036_movies_table),
    (37, _m037_stories_grade),
    (38, _m038_stories_manual_changed),
    (39, _m039_delete_qwen36_plus_preview),
    (40, _m040_rename_donating_to_donated),
    (41, _m041_rename_story_error_to_error),
    (42, _m042_donor_refactor),
    (43, _m043_fix_remaining_probe_status),
    (44, _m044_ltx_video_price),
    (45, _m045_add_indexes),
    (46, _m046_drop_ai_platform_id),
    (47, _m047_log_status_cancelled),
    (48, _m048_remove_model_name_from_batch_data),
    (49, _m049_widen_log_entries_level),
    (50, _m050_remove_story_probe_and_model_name_from_batch_data),
    (51, _m051_remove_obsolete_settings),
    (52, _m052_model_durations),
    (53, _m053_log_entries_log_id_nullable),
    (54, _m054_drop_remaining_foreign_keys),
    (55, _m055_delete_vk_publish_settings),
    (56, _m056_deepseek_platform),
    (57, _m057_rename_model_id_keys_in_batches_data),
    (58, _m058_deepseek_chat_api_params),
    (59, _m059_rename_prompt_settings_keys),
    (60, _m060_stories_top_quality),
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
