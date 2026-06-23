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
    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'log'
          AND column_name IN ('pipeline', 'category')
    """)
    cols = {r[0] for r in cur.fetchall()}
    if 'pipeline' in cols and 'category' not in cols:
        cur.execute("ALTER TABLE log RENAME COLUMN pipeline TO category")
    cur.execute("ALTER TABLE log DROP COLUMN IF EXISTS message")
    cur.execute("ALTER TABLE log DROP COLUMN IF EXISTS status")
    cur.execute("DROP INDEX IF EXISTS idx_log_pipeline")
    cur.execute("DROP INDEX IF EXISTS idx_log_status")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_log_category ON log (category)")

def merge_duplicate_batch_logs(cur) -> None:
    """Один log на batch_id: перенос log_entries, удаление лишних строк log."""
    cur.execute("""
        WITH ranked AS (
            SELECT id,
                   batch_id,
                   ROW_NUMBER() OVER (
                       PARTITION BY batch_id
                       ORDER BY created_at ASC, id ASC
                   ) AS rn
            FROM log
            WHERE batch_id IS NOT NULL
        ),
        mapping AS (
            SELECT e.id AS extra_id, k.id AS keep_id
            FROM ranked e
            JOIN ranked k
              ON k.batch_id = e.batch_id AND k.rn = 1
            WHERE e.rn > 1
        )
        UPDATE log_entries le
        SET log_id = m.keep_id
        FROM mapping m
        WHERE le.log_id = m.extra_id
    """)
    cur.execute("""
        DELETE FROM log l
        USING (
            SELECT id,
                   ROW_NUMBER() OVER (
                       PARTITION BY batch_id
                       ORDER BY created_at ASC, id ASC
                   ) AS rn
            FROM log
            WHERE batch_id IS NOT NULL
        ) r
        WHERE l.id = r.id AND r.rn > 1
    """)

def _m1008_log_entries_category(cur):
    cur.execute("ALTER TABLE log_entries ADD COLUMN IF NOT EXISTS category TEXT")

    cur.execute("""
        UPDATE log_entries le
        SET category = l.category
        FROM log l
        WHERE l.id = le.log_id
          AND le.category IS NULL
          AND l.category IS NOT NULL
    """)

    cur.execute("""
        UPDATE log_entries le
        SET category = (regexp_match(le.message, '^\\[([^\\]]+)\\]'))[1]
        FROM log l
        WHERE l.id = le.log_id
          AND le.category IS NULL
          AND l.batch_id IS NULL
          AND le.message ~ '^\\[[^\\]]+\\]'
    """)

    cur.execute("""
        UPDATE log_entries SET category = 'system'
        WHERE category IS NULL
    """)

    merge_duplicate_batch_logs(cur)

    cur.execute("""
        UPDATE log_entries
        SET message = regexp_replace(message, '^\\[' || category || '\\]\\s*', '')
        WHERE message ~ ('^\\[' || category || '\\]')
    """)

    cur.execute("DROP INDEX IF EXISTS idx_log_category")
    cur.execute("ALTER TABLE log DROP COLUMN IF EXISTS category")

    cur.execute("ALTER TABLE log_entries ALTER COLUMN category SET DEFAULT 'system'")
    cur.execute("UPDATE log_entries SET category = 'system' WHERE category IS NULL")
    cur.execute("ALTER TABLE log_entries ALTER COLUMN category SET NOT NULL")

    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_log_batch_id_unique
            ON log (batch_id)
        WHERE batch_id IS NOT NULL
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_log_entries_category
            ON log_entries (category)
    """)

_LOG_ENTRY_CHANNELS = (
    "api", "planning", "story", "video", "transcode", "publish", "runner", "playwright",
    "main", "main_loop", "http", "db", "upgrade", "startup", "cleanup", "browser",
    "notify", "consts", "dzen", "rutube", "vkvideo", "system",
)

def _m1009_log_entries_channel(cur):
    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'log_entries'
          AND column_name IN ('category', 'channel')
    """)
    cols = {r[0] for r in cur.fetchall()}
    if "category" in cols and "channel" not in cols:
        cur.execute("ALTER TABLE log_entries RENAME COLUMN category TO channel")

    cur.execute("DROP INDEX IF EXISTS idx_log_entries_category")

    cur.execute("""
        UPDATE log_entries
        SET channel = lower((regexp_match(message, '^\\[([^\\]]+)\\]'))[1]),
            message = regexp_replace(message, '^\\[[^\\]]+\\]\\s*', '')
        WHERE message ~ '^\\[[^\\]]+\\]'
          AND lower((regexp_match(message, '^\\[([^\\]]+)\\]'))[1]) = ANY(%s)
    """, (list(_LOG_ENTRY_CHANNELS),))

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_log_entries_channel
            ON log_entries (channel)
    """)

def _m1010_drop_log_entries_category(cur):
    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'log_entries'
          AND column_name IN ('category', 'channel')
    """)
    cols = {r[0] for r in cur.fetchall()}
    if "category" not in cols:
        return
    if "channel" in cols:
        cur.execute("""
            UPDATE log_entries
            SET channel = category
            WHERE category IS NOT NULL
        """)
        cur.execute("ALTER TABLE log_entries DROP COLUMN category")
    else:
        cur.execute("ALTER TABLE log_entries RENAME COLUMN category TO channel")
    cur.execute("DROP INDEX IF EXISTS idx_log_entries_category")

def _m1011_buffer_hours_to_minutes(cur):
    """Convert planning horizon setting from hours to minutes."""
    cur.execute("SELECT value FROM settings WHERE key = 'buffer_hours'")
    row = cur.fetchone()
    if row is not None:
        try:
            minutes = int(row[0]) * 60
        except (ValueError, TypeError):
            minutes = 60
        cur.execute(
            """
            INSERT INTO settings (key, value) VALUES ('buffer_minutes', %s)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
            """,
            (str(minutes),),
        )
        cur.execute("DELETE FROM settings WHERE key = 'buffer_hours'")
        return
    cur.execute("SELECT 1 FROM settings WHERE key = 'buffer_minutes'")
    if cur.fetchone() is None:
        cur.execute(
            "INSERT INTO settings (key, value) VALUES ('buffer_minutes', '60')"
        )

def _m1012_add_movies_published(cur):
    """Add movies.published; backfill from batch status LIKE '%published%' (Director UI checkmark)."""
    cur.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS published BIT(1)")
    cur.execute(
        """
        UPDATE movies m
        SET published = B'1'
        WHERE EXISTS (
            SELECT 1
            FROM batches b2
            WHERE b2.movie_id = m.id
              AND b2.status LIKE '%published%'
        )
        """
    )
    cur.execute("UPDATE movies SET published = B'0' WHERE published IS NULL")
    cur.execute("ALTER TABLE movies ALTER COLUMN published SET DEFAULT B'0'")
    cur.execute("ALTER TABLE movies ALTER COLUMN published SET NOT NULL")

def _m1013_rename_generating_to_processing(cur):
    """Rename batch status generating -> processing (unified in-progress status)."""
    cur.execute(
        """
        UPDATE batches
        SET status = %s
        WHERE status = %s
        """,
        ('processing', 'generating'),
    )

def _m1014_rename_generated_to_processed(cur):
    """Rename batch status generated -> processed (movie stage after provider poll)."""
    cur.execute(
        """
        UPDATE batches
        SET status = %s
        WHERE status = %s
        """,
        ('processed', 'generated'),
    )

def _m1015_rename_publish_and_transcoding_statuses(cur):
    """Unify batch status names: transcoding->processing, published*->completed/partially, .published->.completed."""
    cur.execute(
        "UPDATE batches SET status = %s WHERE status = %s",
        ('processing', 'transcoding'),
    )
    cur.execute(
        "UPDATE batches SET status = %s WHERE status = %s",
        ('partially', 'published_partially'),
    )
    cur.execute(
        "UPDATE batches SET status = %s WHERE status = %s",
        ('completed', 'published'),
    )
    cur.execute(
        """
        UPDATE batches
        SET status = REPLACE(status, %s, %s)
        WHERE status LIKE %s
        """,
        ('.published', '.completed', '%.published'),
    )

def _m1016_add_stories_prompt(cur):
    """Add stories.prompt for T2V prompt text (NULL when unset)."""
    cur.execute("ALTER TABLE stories ADD COLUMN IF NOT EXISTS prompt TEXT")

MIGRATIONS = [
    (2026052901, _m1001_drop_targets_legacy_columns),
    (2026052902, _m1002_add_movies_transcoded),
    (2026052903, _m1003_backfill_transcode_schedule_from_planning),
    (2026052904, _m1004_backfill_movies_story_id),
    (2026052905, _m1005_mark_published_movies_used),
    (2026052906, _m1006_fix_published_movies_transcoded),
    (2026052907, _m1007_log_category_and_drop_message_status),
    (2026052908, _m1008_log_entries_category),
    (2026052909, _m1009_log_entries_channel),
    (2026052910, _m1010_drop_log_entries_category),
    (2026052911, _m1011_buffer_hours_to_minutes),
    (2026052912, _m1012_add_movies_published),
    (2026052913, _m1013_rename_generating_to_processing),
    (2026052914, _m1014_rename_generated_to_processed),
    (2026052915, _m1015_rename_publish_and_transcoding_statuses),
    (2026062201, _m1016_add_stories_prompt),
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
            write_log_entry(None, 'db', f'Миграция {version} применена', level='silent')
        except Exception as e:
            write_log_entry(None, 'db', f'Ошибка миграции {version}: {e}', level='silent')
            raise
