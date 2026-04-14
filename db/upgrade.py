"""
upgrade.py — пост-апгрейд обработка.

Единственная точка, где проверяется и устанавливается серверное окружение.
Вызывается из main.py при каждом старте приложения.

На деве (REPLIT_DEPLOYMENT != "1") возвращает управление немедленно.
На проде сравнивает build_number из таблицы environment с BUILD из _build.py.
При обнаружении обновления запускает проверку всего серверного окружения
и бросает исключение если хотя бы одна проверка не прошла.
env_set(build_number) вызывается только после успешного завершения всех проверок.
"""
import os
import pathlib
import shutil
import subprocess

from utils._build import BUILD
from db import env_get, env_set
from db.init import _bootstrap, run_migrations, seed_db
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


def _check_ffprobe() -> tuple[bool, str]:
    path = shutil.which('ffprobe')
    if not path:
        return False, 'ffprobe не найден'
    return True, f'ffprobe: {path}'


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
    """Запускает playwright install chromium. Возвращает True при успехе."""
    try:
        result = subprocess.run(
            ["playwright", "install", "chromium"],
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
        write_log_entry(None, '[upgrade] Chromium не найден — устанавливаю через playwright...', level='silent')
        if _install_chromium():
            return True, 'chromium: установлен через playwright install chromium'
        return False, 'chromium: не удалось установить через playwright install chromium'
    except Exception as e:
        return False, f'chromium: {e}'


class FatalError(RuntimeError):
    """Фатальная ошибка апгрейда — приложение не должно запускаться."""


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
    ('ffmpeg',     _check_ffmpeg),
    ('ffprobe',    _check_ffprobe),
    ('flask',      _check_flask),
    ('requests',   _check_requests),
    ('psycopg2',   _check_psycopg2),
    ('playwright', _check_playwright),
    ('chromium',   _check_chromium),
]

_POST_MIGRATION_CHECKS = [
    ('model_durations', _check_model_durations),
]


def _run_checks(checks, label) -> None:
    """Запускает список проверок. Бросает FatalError или RuntimeError при неудаче."""
    write_log_entry(None, f'[upgrade] {label}...', level='silent')
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


def check_upgrade() -> bool:
    """
    Инициализирует БД и возвращает True если приложение обновилось с последнего запуска.

    _bootstrap() и seed_db() вызываются только если база совсем новая (build_number отсутствует).
    run_migrations() вызывается при каждом старте — и на деве, и на проде.
    На проде дополнительно: проверка окружения, пост-миграционные проверки, запись build_number.
    """
    if os.environ.get('REPLIT_DEPLOYMENT') != '1':
        stored = env_get(_BUILD_KEY, '')
        if not stored:
            _bootstrap()
            seed_db()
        run_migrations()
        _run_post_migration_checks()
        return False

    stored = env_get(_BUILD_KEY, '')
    if not stored:
        _bootstrap()

    if stored == BUILD:
        return False

    write_log_entry(None, f'[upgrade] Обновление: {stored or "первый запуск"} → {BUILD}', level='silent')
    _run_env_checks()
    run_migrations()
    seed_db()
    _run_post_migration_checks()
    env_set(_BUILD_KEY, BUILD)
    return True
