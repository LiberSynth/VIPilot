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
  4. Перенесите изменение схемы в bootstrap() (db/init.py).
  5. Ориентир: удалять не раньше двух недель после деплоя на prod.

Схема для развёртывания с нуля — только bootstrap() в db/init.py.
MIGRATIONS — для одноразовых data/backfill шагов при апгрейде (если понадобятся).
══════════════════════════════════════════════════════════════════════════
"""

import json

from .connection import get_db
from log.log import write_log_entry

# ---------------------------------------------------------------------------
# Реестр миграций — добавляйте только в конец, никогда не переиспользуйте номера
# ---------------------------------------------------------------------------

_SEEDANCE_MODELS = (
    ('Seedance 2.0', 'seedance-2-0', 100),
    ('Seedance 2.0 Fast', 'seedance-2-0-fast', 101),
    ('Seedance 2.0 Mini', 'seedance-2-0-mini', 102),
)

def _migrate_seedance_platform(cur):
    cur.execute("""
        INSERT INTO ai_platforms (name, url, env_key_name)
        SELECT 'Seedance', 'https://api.seedance2.ai', 'SEEDANCE_API_KEY'
        WHERE NOT EXISTS (SELECT 1 FROM ai_platforms WHERE name = 'Seedance')
    """)
    cur.execute("SELECT id FROM ai_platforms WHERE name = 'Seedance'")
    row = cur.fetchone()
    if not row:
        return
    platform_id = row[0]

    for name, model_key, order in _SEEDANCE_MODELS:
        body = {
            'model': model_key,
            'input': {
                'generation_type': 'text-to-video',
                'resolution': '720p',
                'generate_audio': True,
                'watermark': False,
            },
        }
        cur.execute("""
            INSERT INTO ai_models (name, platform_id, type, url, body, "order")
            SELECT %s, %s, 'text-to-video', 'v1/videos/generations', %s::jsonb, %s
            WHERE NOT EXISTS (
                SELECT 1 FROM ai_models WHERE platform_id = %s AND name = %s
            )
        """, (name, platform_id, json.dumps(body), order, platform_id, name))

        cur.execute(
            "SELECT id FROM ai_models WHERE platform_id = %s AND name = %s",
            (platform_id, name),
        )
        mrow = cur.fetchone()
        if not mrow:
            continue
        model_id = mrow[0]
        for duration in range(4, 16):
            cur.execute("""
                INSERT INTO model_durations (model_id, duration)
                SELECT %s, %s
                WHERE NOT EXISTS (
                    SELECT 1 FROM model_durations WHERE model_id = %s AND duration = %s
                )
            """, (model_id, duration, model_id, duration))

_SEEDANCE_PRICES = (
    ('Seedance 2.0', '3.040 $/10сек'),       # 12 credits/s × 10s @ 720p
    ('Seedance 2.0 Fast', '2.533 $/10сек'), # 10 credits/s × 10s @ 720p
    ('Seedance 2.0 Mini', '1.520 $/10сек'),  # 6 credits/s × 10s @ 720p
)

def _migrate_seedance_prices(cur):
    for name, price in _SEEDANCE_PRICES:
        cur.execute("""
            UPDATE ai_models m
            SET price = %s
            FROM ai_platforms p
            WHERE m.platform_id = p.id
              AND p.name = 'Seedance'
              AND m.name = %s
              AND (m.price IS NULL OR m.price IS DISTINCT FROM %s)
        """, (price, name, price))

MIGRATIONS = [
    (2026071601, _migrate_seedance_platform),
    (2026071701, _migrate_seedance_prices),
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
