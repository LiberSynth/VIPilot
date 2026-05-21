"""
upgrade.py — пост-апгрейд обработка.

Единственная точка, где проверяется и устанавливается серверное окружение.
Вызывается из main.py при каждом старте приложения.

При совпадении build_number с BUILD выполнения нет (ранний выход).
При смене BUILD — полный цикл: проверки окружения, миграции и т.д.
На Windows pg_repack CLI обычно отсутствует (см. _check_pg_repack); сжатие — VACUUM FULL.
На Linux проверки окружения строгие; сбой любой проверки — исключение.
env_set(build_number) вызывается только после успешного завершения всех шагов.
"""
import os
import pathlib
import platform
import shutil
import subprocess

from utils._build import BUILD
from db import env_get, env_set
from db.init import bootstrap, run_migrations
from db.seed import seed_db
from common.exceptions import FatalError
from log.log import write_log_entry

_BUILD_KEY = 'build_number'


def _check_ffmpeg() -> tuple[bool, str]:
    path = shutil.which('ffmpeg')
    if not path:
        return False, 'ffmpeg не найден'
    try:
        r = subprocess.run(['ffmpeg', '-version'], capture_output=True, timeout=5)
        ver = r.stdout.decode().splitlines()[0] if r.stdout else '?'
        return True, f'ffmpeg: {ver}'
    except Exception as e:
        return False, f'ffmpeg: ошибка — {e}'


def _check_flask() -> tuple[bool, str]:
    try:
        import flask
        return True, f'flask {flask.__version__}'
    except ImportError:
        return False, 'flask не установлен'


def _check_requests() -> tuple[bool, str]:
    try:
        import requests
        return True, f'requests {requests.__version__}'
    except ImportError:
        return False, 'requests не установлен'


def _check_psycopg2() -> tuple[bool, str]:
    try:
        import psycopg2
        return True, f'psycopg2 {psycopg2.__version__}'
    except ImportError:
        return False, 'psycopg2 не установлен'


def _check_playwright() -> tuple[bool, str]:
    try:
        from importlib.metadata import version
        ver = version('playwright')
        return True, f'playwright {ver}'
    except Exception:
        try:
            import playwright  # noqa: F401
            return True, 'playwright (версия неизвестна)'
        except ImportError:
            return False, 'playwright не установлен'


def _install_chromium() -> bool:
    """Запускает playwright install chromium chromium-headless-shell. Возвращает True при успехе."""
    import sys
    try:
        result = subprocess.run(
            [sys.executable, "-m", "playwright", "install", "chromium", "chromium-headless-shell"],
            timeout=180,
            check=False,
        )
        return result.returncode == 0
    except Exception as e:
        write_log_entry(None, f'[upgrade] Ошибка установки Chromium: {e}', level='silent')
        return False


def _check_chromium() -> tuple[bool, str]:
    path = (shutil.which('chromium')
            or shutil.which('chromium-browser')
            or shutil.which('google-chrome'))
    if path:
        return True, f'chromium: {path}'
    try:
        from playwright.sync_api import sync_playwright
        p = sync_playwright().start()
        exe = p.chromium.executable_path
        p.stop()
        if pathlib.Path(exe).exists():
            return True, f'chromium (playwright): {exe}'
        write_log_entry(None, '[upgrade] Chromium не найден — устанавливаю через playwright.', level='silent')
        if _install_chromium():
            return True, 'chromium: установлен через playwright install chromium'
        return False, 'chromium: не удалось установить через playwright install chromium'
    except Exception as e:
        return False, f'chromium: {e}'


def _check_dotenv() -> tuple[bool, str]:
    try:
        import dotenv  # noqa: F401
        from importlib.metadata import version
        ver = version('python-dotenv')
        return True, f'python-dotenv {ver}'
    except ImportError:
        pass
    try:
        subprocess.run(
            ['pip', 'install', 'python-dotenv'],
            check=True,
            timeout=60,
            capture_output=True,
        )
        import dotenv  # noqa: F401
        return True, 'python-dotenv: установлен через pip install'
    except Exception as e:
        return False, f'python-dotenv: не удалось установить — {e}'


def _check_pg_repack() -> tuple[bool, str]:
    """Проверяет CLI pg_repack и серверное extension pg_repack.

    Если extension не установлен — пытается создать его сам (по образцу
    `_check_chromium`, который сам устанавливает Chromium). Возвращает False
    только если: автоустановка CLI не удалась; CREATE EXTENSION упал; версии CLI и extension
    не совпадают.

    На Windows официального pg_repack CLI обычно нет — проверка смягчается
    (сжатие через VACUUM FULL). При `VIPILOT_REQUIRE_PG_REPACK=1` на Windows
    проверка строгая.
    """
    from utils.pkg_bootstrap import ensure_pg_repack_in_path

    if not ensure_pg_repack_in_path(auto_install=True):
        force = os.environ.get('VIPILOT_REQUIRE_PG_REPACK') == '1'
        if platform.system() == 'Windows' and not force:
            write_log_entry(
                None,
                '[upgrade] pg_repack: на Windows CLI не установлен — '
                'кнопка «Дефрагментация» использует VACUUM FULL. '
                'Задайте VIPILOT_REQUIRE_PG_REPACK=1, если нужен строгий pg_repack.',
                level='silent',
            )
            return True, (
                'pg_repack: пропуск на Windows — нет CLI (сжатие через VACUUM FULL)'
            )
        return False, 'pg_repack: CLI не найден и автоустановка не удалась'
    cli_path = shutil.which('pg_repack')
    if not cli_path:
        return False, 'pg_repack: CLI не найден после автоустановки'
    try:
        r = subprocess.run(['pg_repack', '--version'], capture_output=True, timeout=5)
        out = (r.stdout.decode() + r.stderr.decode()).strip()
        cli_ver = out.split()[-1] if out else ''
    except Exception as e:
        return False, f'pg_repack: ошибка вызова CLI — {e}'
    if not cli_ver:
        return False, f'pg_repack: не удалось распарсить версию CLI ({out!r})'

    try:
        from db.connection import get_db
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT extversion FROM pg_extension WHERE extname='pg_repack'")
                row = cur.fetchone()
                if not row:
                    write_log_entry(None, '[upgrade] pg_repack extension отсутствует — устанавливаю.', level='silent')
                    cur.execute('CREATE EXTENSION IF NOT EXISTS pg_repack')
                    conn.commit()
                    cur.execute("SELECT extversion FROM pg_extension WHERE extname='pg_repack'")
                    row = cur.fetchone()
                    if not row:
                        return False, 'pg_repack: установка extension не дала результата'
                ext_ver = row[0]
    except Exception as e:
        return False, f'pg_repack: ошибка установки/чтения extension — {e}'

    if cli_ver != ext_ver:
        return False, f'pg_repack: версии не совпадают (CLI={cli_ver}, extension={ext_ver})'
    return True, f'pg_repack {cli_ver} (CLI и extension)'


def _check_model_durations() -> tuple[bool, str]:
    try:
        from db.connection import get_db
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT m.name
                    FROM ai_models m
                    WHERE m.type = 'text-to-video'
                      AND m.active = TRUE
                      AND NOT EXISTS (
                          SELECT 1 FROM model_durations md WHERE md.model_id = m.id
                      )
                """)
                missing = [row[0] for row in cur.fetchall()]
        if missing:
            names = ', '.join(missing)
            raise FatalError(f'model_durations: нет записей для активных видео-моделей: {names}')
        return True, 'model_durations: все активные видео-модели имеют записи'
    except FatalError:
        raise
    except Exception as e:
        return False, f'model_durations: ошибка проверки — {e}'


_CHECKS = [
    ('ffmpeg',      _check_ffmpeg),
    ('flask',       _check_flask),
    ('requests',    _check_requests),
    ('psycopg2',    _check_psycopg2),
    ('dotenv',      _check_dotenv),
    ('playwright',  _check_playwright),
    ('chromium',    _check_chromium),
    ('pg_repack',   _check_pg_repack),
]

_POST_MIGRATION_CHECKS = [
    ('model_durations', _check_model_durations),
]


def _run_checks(checks, label) -> None:
    """Запускает список проверок. Бросает FatalError или RuntimeError при неудаче."""
    write_log_entry(None, f'[upgrade] {label}.', level='silent')
    failed = []
    for name, fn in checks:
        try:
            ok, msg = fn()
        except FatalError:
            raise
        except Exception as e:
            ok, msg = False, f'{name}: непредвиденная ошибка — {e}'
        tag = 'OK  ' if ok else 'FAIL'
        write_log_entry(None, f'[upgrade]   [{tag}] {msg}', level='silent')
        if not ok:
            failed.append(msg)
    if failed:
        summary = '; '.join(failed)
        raise RuntimeError(f'[upgrade] Проверка не пройдена: {summary}')
    write_log_entry(None, f'[upgrade] {label}: всё в порядке', level='silent')


def _run_env_checks() -> None:
    """
    Проверяет серверное окружение. Бросает RuntimeError если хотя бы одна проверка не прошла.
    """
    _run_checks(_CHECKS, 'Проверка серверного окружения')


def _run_post_migration_checks() -> None:
    """
    Проверки после применения миграций. Бросает FatalError если данные не соответствуют ожиданиям.
    """
    _run_checks(_POST_MIGRATION_CHECKS, 'Пост-миграционные проверки')


def check_upgrade():
    """
    Инициализирует БД при старте приложения.

    Признак первого запуска — отсутствие build_number в environment.
    Порядок: bootstrap → ранний выход если build не изменился → env_checks →
             seed_db (первый запуск) → run_migrations → post_checks → env_set.
    build_number фиксируется только после успешного завершения всех шагов.
    """
    bootstrap()

    stored = env_get(_BUILD_KEY, '')

    if stored == BUILD:
        return False
    write_log_entry(None, f'[upgrade] Обновление: {stored or "первый запуск"} -> {BUILD}', level='silent')
    _run_env_checks()

    if not stored:
        seed_db()
    run_migrations()
    _run_post_migration_checks()
    env_set(_BUILD_KEY, BUILD)
