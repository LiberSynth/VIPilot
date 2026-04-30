"""
set_build_number.py — фиксирует номер сборки в utils/_build.py.

Запускается как build-шаг деплоя, пока git ещё доступен.
Завершается с ошибкой (exit code 1), если git недоступен — чтобы деплой
не прошёл молча с неверным значением.
"""
import subprocess
import sys
import os

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


if __name__ == '__main__':
    main()
