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
# Следующая миграция: _m074_...


def _m073_add_wan27(cur):
    """Добавляет модель Wan 2.7 в ai_models и её допустимые длительности."""
    cur.execute("""
        INSERT INTO ai_models (name, url, body, "order", active, grade, type, platform_id)
        SELECT
            'Wan 2.7',
            'fal-ai/wan/v2.7/text-to-video',
            '{"prompt": "{}", "duration": "{:int}", "aspect_ratio": "{:d}:{:d}", "resolution": "720p"}'::jsonb,
            17,
            TRUE,
            'good',
            'text-to-video',
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
            ORDER BY b.created_at ASC
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
