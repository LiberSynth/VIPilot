import subprocess


def _check_ffmpeg():
    """Проверяет наличие ffmpeg и выводит версию. Вызывается при апгрейде/деплое."""
    try:
        result = subprocess.run(
            ['ffmpeg', '-version'],
            capture_output=True,
            timeout=10,
        )
        if result.returncode == 0:
            first_line = result.stdout.decode(errors='replace').splitlines()[0]
            print(f"[upgrade] ffmpeg доступен: {first_line}")
        else:
            err = result.stderr.decode(errors='replace')[:200]
            print(f"[upgrade] ВНИМАНИЕ: ffmpeg вернул код {result.returncode}: {err}")
    except FileNotFoundError:
        print("[upgrade] ВНИМАНИЕ: ffmpeg не найден в PATH — транскодирование недоступно")
    except Exception as e:
        print(f"[upgrade] ВНИМАНИЕ: ffmpeg проверка не удалась: {e}")


def run_upgrades():
    _check_ffmpeg()
