"""
Визуальное сравнение качества транскодирования.
Берёт исходное видео, транскодирует текущими настройками (те же что в pipelines/transcode.py)
и склеивает оба в одно: оригинал слева, транскод справа.
Результат сохраняется в scripts/compare_output.mp4

Запуск:
    python scripts/compare_transcode.py [путь_к_видео]

Если путь не указан — берётся первый файл из папки videos/
"""

import os
import sys
import subprocess
import tempfile
import glob


def find_source_video(path_arg):
    if path_arg:
        if path_arg.startswith("http://") or path_arg.startswith("https://"):
            return download_video(path_arg)
        if not os.path.isfile(path_arg):
            print(f"Файл не найден: {path_arg}")
            sys.exit(1)
        return path_arg
    videos = sorted(glob.glob("videos/*.mp4"))
    if not videos:
        print("Нет видеофайлов в папке videos/")
        sys.exit(1)
    return videos[0]


def download_video(url):
    import urllib.request
    print(f"Скачиваю оригинал: {url[:80]}...")
    with tempfile.NamedTemporaryFile(suffix='.mp4', delete=False) as tmp:
        tmp_path = tmp.name
    try:
        urllib.request.urlretrieve(url, tmp_path)
        mb = os.path.getsize(tmp_path) / 1024 / 1024
        print(f"  Скачано: {mb:.1f} МБ")
        return tmp_path
    except Exception as e:
        print(f"Ошибка скачивания: {e}")
        sys.exit(1)


def probe_duration(src):
    try:
        result = subprocess.run(
            [
                'ffprobe', '-v', 'error',
                '-select_streams', 'v:0',
                '-show_entries', 'format=duration',
                '-of', 'default=noprint_wrappers=1:nokey=1',
                src,
            ],
            capture_output=True,
            timeout=30,
        )
        if result.returncode == 0:
            return float(result.stdout.decode().strip())
    except Exception:
        pass
    return None


def probe_resolution(src):
    try:
        result = subprocess.run(
            [
                'ffprobe', '-v', 'error',
                '-select_streams', 'v:0',
                '-show_entries', 'stream=width,height',
                '-of', 'csv=p=0',
                src,
            ],
            capture_output=True,
            timeout=30,
        )
        if result.returncode == 0:
            parts = result.stdout.decode().strip().split(',')
            return int(parts[0]), int(parts[1])
    except Exception:
        pass
    return None, None


def transcode(src, dst, duration):
    cmd = [
        'ffmpeg', '-y',
        '-i', src,
        '-f', 'lavfi', '-i', 'anullsrc=r=44100:cl=stereo',
        '-c:v', 'libx264',
        '-profile:v', 'baseline',
        '-preset', 'ultrafast',
        '-crf', '26',
        '-pix_fmt', 'yuv420p',
        '-r', '30',
        '-c:a', 'aac',
        '-b:a', '96k',
        '-movflags', '+faststart',
    ]
    if duration is not None:
        cmd += ['-t', str(duration)]
    else:
        cmd += ['-shortest']
    cmd.append(dst)

    print("  Запускаю ffmpeg...")
    proc = subprocess.run(cmd, capture_output=True, timeout=300)
    if proc.returncode != 0:
        print("Ошибка ffmpeg:")
        print(proc.stderr.decode(errors='replace')[-800:])
        sys.exit(1)


def make_side_by_side(src, transcoded, output, duration):
    """Склеивает оба видео рядом с подписями через ffmpeg filtergraph."""
    w, h = probe_resolution(src)
    if w is None:
        w, h = 720, 1280

    font_size = max(28, h // 40)

    # Подписи рисуем через drawtext
    filter_complex = (
        f"[0:v]scale={w}:{h}:force_original_aspect_ratio=decrease,"
        f"pad={w}:{h}:(ow-iw)/2:(oh-ih)/2,"
        f"drawtext=text='Оригинал':fontsize={font_size}:fontcolor=white:"
        f"box=1:boxcolor=black@0.5:boxborderw=8:x=20:y=20[left];"

        f"[1:v]scale={w}:{h}:force_original_aspect_ratio=decrease,"
        f"pad={w}:{h}:(ow-iw)/2:(oh-ih)/2,"
        f"drawtext=text='Транскод (crf 26 ultrafast)':fontsize={font_size}:fontcolor=white:"
        f"box=1:boxcolor=black@0.5:boxborderw=8:x=20:y=20[right];"

        f"[left][right]hstack=inputs=2[out]"
    )

    cmd = [
        'ffmpeg', '-y',
        '-i', src,
        '-i', transcoded,
        '-filter_complex', filter_complex,
        '-map', '[out]',
        '-c:v', 'libx264',
        '-preset', 'fast',
        '-crf', '18',
        '-pix_fmt', 'yuv420p',
        '-movflags', '+faststart',
    ]
    if duration is not None:
        cmd += ['-t', str(duration)]
    cmd.append(output)

    print("  Склеиваю side-by-side...")
    proc = subprocess.run(cmd, capture_output=True, timeout=300)
    if proc.returncode != 0:
        print("Ошибка склейки ffmpeg:")
        print(proc.stderr.decode(errors='replace')[-800:])
        sys.exit(1)


def main():
    path_arg = sys.argv[1] if len(sys.argv) > 1 else None
    src = find_source_video(path_arg)

    src_mb = os.path.getsize(src) / 1024 / 1024
    print(f"Исходник: {src}  ({src_mb:.1f} МБ)")

    duration = probe_duration(src)
    if duration:
        print(f"Длительность: {duration:.1f} сек")

    output = "scripts/compare_output.mp4"

    with tempfile.NamedTemporaryFile(suffix='.mp4', delete=False) as tmp:
        transcoded_path = tmp.name

    try:
        print("\nШаг 1 — транскодирование...")
        transcode(src, transcoded_path, duration)

        out_mb = os.path.getsize(transcoded_path) / 1024 / 1024
        print(f"  Транскод: {out_mb:.1f} МБ  (было {src_mb:.1f} МБ, -{ (1 - out_mb/src_mb)*100:.0f}%)")

        print("\nШаг 2 — склейка side-by-side...")
        make_side_by_side(src, transcoded_path, output, duration)

    finally:
        try:
            os.unlink(transcoded_path)
        except Exception:
            pass

    result_mb = os.path.getsize(output) / 1024 / 1024
    print(f"\nГотово! Сохранено: {output}  ({result_mb:.1f} МБ)")
    print("Откройте файл в плеере: оригинал слева, транскод справа.")


if __name__ == '__main__':
    main()
