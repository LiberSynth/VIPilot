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

from pathlib import Path

from .connection import get_db
from log.log import write_log_entry

_VIDEO_DIR = Path(__file__).resolve().parents[1] / "video"


# ---------------------------------------------------------------------------
# Функции миграций
# ---------------------------------------------------------------------------

def _m1001_drop_targets_legacy_columns(cur):
    """Drop legacy target columns no longer used by stage-batches."""
    cur.execute("ALTER TABLE targets DROP COLUMN IF EXISTS aspect_ratio_x")
    cur.execute("ALTER TABLE targets DROP COLUMN IF EXISTS aspect_ratio_y")
    cur.execute("ALTER TABLE targets DROP COLUMN IF EXISTS transcode")


def _m1002_add_movies_transcoded(cur):
    """Add movies.transcoded with backfill based on transcoded_data files."""
    cur.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS transcoded BIT(1)")
    cur.execute("SELECT id::text FROM movies WHERE transcoded IS NULL")
    missing_rows = [str(r[0]) for r in cur.fetchall()]
    for movie_id in missing_rows:
        has_transcoded_file = (_VIDEO_DIR / f"{movie_id} - transcoded_data.mp4").exists()
        cur.execute(
            "UPDATE movies SET transcoded = %s::bit(1) WHERE id = %s::uuid",
            ("1" if has_transcoded_file else "0", movie_id),
        )
    cur.execute("ALTER TABLE movies ALTER COLUMN transcoded SET DEFAULT B'0'")
    cur.execute("ALTER TABLE movies ALTER COLUMN transcoded SET NOT NULL")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_movies_transcoded ON movies (transcoded)")
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_movies_transcode_pick ON movies (used, transcoded, created_at, id)"
    )


# ---------------------------------------------------------------------------
# Реестр миграций — добавляйте только в конец, никогда не переиспользуйте номера
# ---------------------------------------------------------------------------

def _m1003_backfill_transcode_schedule_from_planning(cur):
    """Copy scheduled_at and story_id from planning source into transcode batches."""
    cur.execute(
        """
        UPDATE batches tc
        SET scheduled_at = pl.scheduled_at,
            story_id = COALESCE(tc.story_id, pl.story_id)
        FROM batches pl
        WHERE tc.type = 'transcode'
          AND pl.id = tc.batch_id_source
          AND pl.type = 'planning'
        """
    )


def _m1004_backfill_movies_story_id(cur):
    """Fill movies.story_id from the latest linked batch."""
    cur.execute(
        """
        UPDATE movies m
        SET story_id = src.story_id
        FROM (
            SELECT DISTINCT ON (b.movie_id)
                   b.movie_id,
                   b.story_id
            FROM batches b
            WHERE b.movie_id IS NOT NULL
              AND b.story_id IS NOT NULL
            ORDER BY b.movie_id, b.created_at DESC, b.id DESC
        ) AS src
        WHERE m.id = src.movie_id
          AND m.story_id IS NULL
        """
    )


def _m1005_mark_published_movies_used(cur):
    """Mark published movies used and transcoded (already published — no re-transcode)."""
    cur.execute(
        """
        UPDATE movies m
        SET used = B'1',
            transcoded = B'1'
        WHERE EXISTS (
            SELECT 1
            FROM batches b
            WHERE b.movie_id = m.id
              AND (
                  b.status IN ('published', 'published_partially')
                  OR b.status LIKE '%.published'
              )
        )
        """
    )


def _m1006_fix_published_movies_transcoded(cur):
    """Backfill transcoded=1 for published movies (fix after _m1005 without transcoded)."""
    cur.execute(
        """
        UPDATE movies m
        SET used = B'1',
            transcoded = B'1'
        WHERE EXISTS (
            SELECT 1
            FROM batches b
            WHERE b.movie_id = m.id
              AND (
                  b.status IN ('published', 'published_partially')
                  OR b.status LIKE '%.published'
              )
        )
        """
    )


def _m1007_log_category_and_drop_message_status(cur):
    cur.execute("ALTER TABLE log RENAME COLUMN pipeline TO category")
    cur.execute("ALTER TABLE log DROP COLUMN IF EXISTS message")
    cur.execute("ALTER TABLE log DROP COLUMN IF EXISTS status")
    cur.execute("DROP INDEX IF EXISTS idx_log_pipeline")
    cur.execute("DROP INDEX IF EXISTS idx_log_status")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_log_category ON log (category)")


MIGRATIONS = [
    (2026052901, _m1001_drop_targets_legacy_columns),
    (2026052902, _m1002_add_movies_transcoded),
    (2026052903, _m1003_backfill_transcode_schedule_from_planning),
    (2026052904, _m1004_backfill_movies_story_id),
    (2026052905, _m1005_mark_published_movies_used),
    (2026052906, _m1006_fix_published_movies_transcoded),
    (2026052907, _m1007_log_category_and_drop_message_status),
]


# ---------------------------------------------------------------------------
# Запуск миграций
# ---------------------------------------------------------------------------

def run_migrations():
    """Применяет все незапущенные миграции. Каждая в отдельной транзакции."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM settings WHERE key = 'db_version'")
            row = cur.fetchone()
            current = int(row[0]) if row else 0

    for version, fn in MIGRATIONS:
        if version <= current:
            continue
        try:
            with get_db() as conn:
                with conn.cursor() as cur:
                    fn(cur)
                    cur.execute(
                        "INSERT INTO settings (key, value) VALUES ('db_version', %s)"
                        " ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                        (str(version),),
                    )
            write_log_entry(None, "system", f"[DB] Миграция {version} применена", level='silent')
        except Exception as e:
            write_log_entry(None, "system", f"[DB] Ошибка миграции {version}: {e}", level='silent')
            raise
