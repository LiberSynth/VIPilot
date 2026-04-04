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


# ---------------------------------------------------------------------------
# Реестр миграций — добавляйте только в конец, никогда не переиспользуйте номера
# ---------------------------------------------------------------------------

MIGRATIONS = [
    (1, _m001_baseline_schema),
    (2, _m002_model_grades_and_batch_models),
    (3, _m003_schedule_created_at),
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
