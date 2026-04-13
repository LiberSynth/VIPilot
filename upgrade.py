"""
upgrade.py — пост-апгрейд обработка.

Вызывается из main.py при каждом старте приложения.
check_upgrade() сравнивает build_number из environment с _build.py,
обновляет значение в БД и при обнаружении обновления запускает
проверку серверного окружения.
"""
import pathlib
import shutil
import subprocess

from utils._build import BUILD
from db import env_get, env_set

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
        return False, (f'chromium: нет по пути {exe} — '
                       f'запустите: playwright install chromium')
    except Exception as e:
        return False, f'chromium: {e}'


_CHECKS = [
    ('ffmpeg',     _check_ffmpeg),
    ('ffprobe',    _check_ffprobe),
    ('flask',      _check_flask),
    ('requests',   _check_requests),
    ('psycopg2',   _check_psycopg2),
    ('playwright', _check_playwright),
    ('chromium',   _check_chromium),
]


def _run_env_checks() -> bool:
    print('[upgrade] Проверка серверного окружения...')
    all_ok = True
    for name, fn in _CHECKS:
        try:
            ok, msg = fn()
        except Exception as e:
            ok, msg = False, f'{name}: непредвиденная ошибка — {e}'
        tag = 'OK  ' if ok else 'FAIL'
        print(f'[upgrade]   [{tag}] {msg}')
        if not ok:
            all_ok = False
    status = 'всё в порядке' if all_ok else 'есть проблемы!'
    print(f'[upgrade] Окружение: {status}')
    return all_ok


def check_upgrade() -> bool:
    """
    Возвращает True если приложение обновилось с последнего запуска.

    Сравнивает build_number из таблицы environment с текущим BUILD.
    При изменении — обновляет значение в БД и запускает проверку окружения.
    """
    stored = env_get(_BUILD_KEY, '')
    if stored == BUILD:
        return False
    env_set(_BUILD_KEY, BUILD)
    print(f'[upgrade] Обновление: {stored or "первый запуск"} → {BUILD}')
    _run_env_checks()
    return True
