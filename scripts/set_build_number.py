"""
set_build_number.py — фиксирует номер сборки в utils/_build.py.

Запускается как build-шаг деплоя, пока git ещё доступен.
Завершается с ошибкой (exit code 1), если git недоступен — чтобы деплой
не прошёл молча с неверным значением.
"""
import subprocess
import sys
import os

import psycopg2

_BUILD_FILE = os.path.join(os.path.dirname(__file__), '..', 'utils', '_build.py')


def main() -> None:
    try:
        result = subprocess.run(
            ['git', 'rev-list', '--count', 'HEAD'],
            capture_output=True,
            timeout=10,
        )
        if result.returncode != 0:
            print(f'[set_build_number] git завершился с кодом {result.returncode}', file=sys.stderr)
            sys.exit(1)
        count = result.stdout.decode().strip()
        if not count:
            print('[set_build_number] git вернул пустое значение', file=sys.stderr)
            sys.exit(1)
    except FileNotFoundError:
        print('[set_build_number] git не найден — деплой без git недопустим', file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f'[set_build_number] Ошибка при вызове git: {e}', file=sys.stderr)
        sys.exit(1)

    build_file = os.path.normpath(_BUILD_FILE)
    with open(build_file, 'w') as f:
        f.write(
            '# _build.py — фиксированный номер сборки приложения.\n'
            '#\n'
            '# Назначение: хранить BUILD между запусками, когда git недоступен на проде.\n'
            '# Перезаписывается скриптом scripts/set_build_number.py в момент деплоя.\n'
            '# Значение используется в upgrade.py для сравнения с build_number в БД.\n'
            f'BUILD = "{count}"\n'
        )
    print(f'[set_build_number] BUILD = {count} → {build_file}')

    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    from utils.consts import PLAYWRIGHT_BROWSERS_PATH
    os.environ.setdefault('PLAYWRIGHT_BROWSERS_PATH', PLAYWRIGHT_BROWSERS_PATH)

    try:
        pw_result = subprocess.run(
            [sys.executable, '-m', 'playwright', 'install', 'chromium', 'chromium-headless-shell'],
            timeout=300,
        )
        if pw_result.returncode != 0:
            print(f'[set_build_number] playwright install завершился с кодом {pw_result.returncode}', file=sys.stderr)
            sys.exit(1)
        print('[set_build_number] Playwright Chromium установлен')
    except FileNotFoundError:
        print('[set_build_number] playwright не найден — деплой без браузера недопустим', file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f'[set_build_number] Ошибка при установке Playwright: {e}', file=sys.stderr)
        sys.exit(1)

    _install_pg_repack_extension()


def _install_pg_repack_extension() -> None:
    """Подключается к БД и создаёт extension pg_repack (идемпотентно).

    Падает с exit(1), если DATABASE_URL не задан, БД недоступна или
    CREATE EXTENSION завершился ошибкой — деплой без сжатия БД недопустим.
    """
    dsn = os.environ.get('DATABASE_URL')
    if not dsn:
        print('[set_build_number] DATABASE_URL не задан — невозможно создать extension pg_repack', file=sys.stderr)
        sys.exit(1)
    try:
        conn = psycopg2.connect(dsn, connect_timeout=10)
    except Exception as e:
        print(f'[set_build_number] Не удалось подключиться к БД: {e}', file=sys.stderr)
        sys.exit(1)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute('CREATE EXTENSION IF NOT EXISTS pg_repack')
        print('[set_build_number] Extension pg_repack установлено (или уже было)')
    except Exception as e:
        print(f'[set_build_number] CREATE EXTENSION pg_repack упал: {e}', file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == '__main__':
    main()
