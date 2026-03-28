import os
import time
import random
import threading
import requests
import subprocess
from datetime import datetime, timezone
from flask import Flask, render_template, request, redirect, url_for, session, flash

FAL_KEY = os.environ['FAL_API_KEY']
VK_TOKEN = os.environ['VK_USER_TOKEN']
ADMIN_PASSWORD = os.environ.get('ADMIN_PASSWORD', 'admin')
GROUP_ID = 236929597

FAL_MODEL = 'fal-ai/minimax/video-01'
FAL_SUBMIT_URL = f'https://queue.fal.run/{FAL_MODEL}'
FAL_STATUS_BASE = 'https://queue.fal.run/fal-ai/minimax/requests'
FAL_HEADERS = {'Authorization': f'Key {FAL_KEY}', 'Content-Type': 'application/json'}

VIDEO_PATH = '/tmp/story_raw.mp4'
VIDEO_VK_PATH = '/tmp/story_vk.mp4'
METAPROMPT_PATH = 'config/metaprompt.txt'

GENERATE_HOUR = 4

STYLE_SUFFIXES = [
    'вертикальное видео, кинематографическая съёмка, тёплый свет, 4K качество',
    'вертикальный формат, сюрреализм, яркие цвета, высокое качество',
    'вертикальное видео, магический реализм, красивое освещение',
    'вертикальный кадр, художественная съёмка, насыщенные цвета',
]

app_state = {
    'running': False,
    'last_published': None,
    'last_ok': False,
}


def load_scenarios():
    try:
        with open(METAPROMPT_PATH, encoding='utf-8') as f:
            lines = [l.strip() for l in f.read().splitlines() if l.strip()]
        return lines if lines else ['Строительная каска катится по улице, из неё вырастает кирпичный дом']
    except Exception:
        return ['Строительная каска катится по улице, из неё вырастает кирпичный дом']


def generate_prompt():
    scenarios = load_scenarios()
    base = random.choice(scenarios)
    style = random.choice(STYLE_SUFFIXES)
    return f"{base}. {style}."


def now():
    return datetime.now(timezone.utc).strftime('%d.%m.%Y %H:%M:%S UTC')


def generate_video():
    prompt = generate_prompt()
    print(f'[{now()}] Промпт: {prompt[:100]}...')
    app_state['running'] = True

    try:
        resp = requests.post(FAL_SUBMIT_URL, headers=FAL_HEADERS, json={
            'prompt': prompt,
            'duration': 6,
            'aspect_ratio': '9:16',
        }, timeout=30)
        data = resp.json()

        if 'request_id' not in data:
            print(f'[{now()}] Ошибка запроса: {data}')
            return False

        request_id = data['request_id']
        status_url = data['status_url']
        print(f'[{now()}] Генерация запущена. ID: {request_id}')

        for attempt in range(240):
            time.sleep(30)
            try:
                s = requests.get(status_url, headers={'Authorization': f'Key {FAL_KEY}'}, timeout=10).json()
                status = s.get('status')
                print(f'[{now()}] Статус [{attempt+1}]: {status}')

                if status == 'COMPLETED':
                    result = requests.get(
                        f'{FAL_STATUS_BASE}/{request_id}',
                        headers={'Authorization': f'Key {FAL_KEY}'},
                        timeout=10
                    ).json()
                    video_url = result.get('video', {}).get('url')
                    if not video_url:
                        print(f'[{now()}] Нет URL видео: {result}')
                        return False
                    return download_and_transcode(video_url)

                elif status == 'FAILED':
                    print(f'[{now()}] Генерация провалилась: {s}')
                    return False
            except Exception as e:
                print(f'[{now()}] Ошибка опроса: {e}')

        print(f'[{now()}] Таймаут генерации')
        return False
    finally:
        app_state['running'] = False


def download_and_transcode(video_url):
    print(f'[{now()}] Скачиваю видео: {video_url[:80]}...')
    ok = False
    for attempt in range(3):
        try:
            r = requests.get(video_url, stream=True, timeout=120, headers={'User-Agent': 'Mozilla/5.0'})
            if r.status_code == 200:
                with open(VIDEO_PATH, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=65536):
                        f.write(chunk)
                size = os.path.getsize(VIDEO_PATH)
                if size > 10000:
                    print(f'[{now()}] Видео скачано: {round(size/1024/1024, 1)} МБ')
                    ok = True
                    break
            else:
                print(f'[{now()}] HTTP {r.status_code}, попытка {attempt+1}/3')
        except Exception as e:
            print(f'[{now()}] Ошибка скачивания (попытка {attempt+1}/3): {e}')
        time.sleep(10)
    if not ok:
        return False

    print(f'[{now()}] Транскодирую...')
    subprocess.run([
        'ffmpeg', '-i', VIDEO_PATH,
        '-c:v', 'libx264', '-profile:v', 'high', '-preset', 'fast', '-crf', '23',
        '-pix_fmt', 'yuv420p', '-r', '30',
        '-c:a', 'aac', '-b:a', '128k',
        '-movflags', '+faststart',
        VIDEO_VK_PATH, '-y'
    ], capture_output=True, timeout=120)
    print(f'[{now()}] Транскодирование завершено')
    return True


def publish_story():
    print(f'[{now()}] Публикую историю в VK...')
    try:
        r = requests.post('https://api.vk.com/method/stories.getVideoUploadServer', data={
            'group_id': GROUP_ID, 'add_to_news': 1, 'access_token': VK_TOKEN, 'v': '5.131'
        }, timeout=15)
        r.raise_for_status()
        server_data = r.json()
        if 'error' in server_data:
            print(f'[{now()}] Ошибка getVideoUploadServer: {server_data["error"]}')
            return False
        upload_url = server_data['response']['upload_url']
        print(f'[{now()}] Upload URL получен')

        for attempt in range(3):
            try:
                with open(VIDEO_VK_PATH, 'rb') as f:
                    up = requests.post(upload_url, files={'video_file': f}, timeout=300)
                up.raise_for_status()
                if not up.text.strip():
                    print(f'[{now()}] Пустой ответ от CDN, попытка {attempt+1}/3')
                    time.sleep(5)
                    continue
                up_data = up.json()
                if 'response' not in up_data:
                    print(f'[{now()}] Неожиданный ответ CDN: {up.text[:200]}')
                    return False
                upload_result = up_data['response']['upload_result']
                break
            except Exception as e:
                print(f'[{now()}] Ошибка загрузки видео (попытка {attempt+1}/3): {e}')
                time.sleep(5)
        else:
            print(f'[{now()}] Все попытки загрузки провалились')
            return False

        print(f'[{now()}] Видео загружено, сохраняю историю...')
        save = requests.post('https://api.vk.com/method/stories.save', data={
            'upload_results': upload_result, 'access_token': VK_TOKEN, 'v': '5.131'
        }, timeout=15).json()

        if 'response' in save:
            story_id = save['response']['items'][0]['id']
            ts = datetime.now(timezone.utc).strftime('%d.%m.%Y в %H:%M UTC')
            print(f'[{now()}] ✓ История опубликована! ID: {story_id}')
            app_state['last_published'] = ts
            app_state['last_ok'] = True
            return True
        else:
            print(f'[{now()}] Ошибка stories.save: {save}')
            return False
    except Exception as e:
        print(f'[{now()}] Исключение при публикации: {e}')
        return False


def scheduler_loop():
    generated_today = False
    published_today = False
    last_date = None
    next_retry_after = 0

    print(f'[{now()}] Публикатор запущен. Генерация в {GENERATE_HOUR:02d}:00 UTC.')

    while True:
        now_dt = datetime.now(timezone.utc)
        today = now_dt.date()

        if today != last_date:
            generated_today = False
            published_today = False
            last_date = today
            next_retry_after = 0

        if now_dt.hour >= GENERATE_HOUR and not generated_today and time.time() >= next_retry_after:
            print(f'[{now()}] Запускаю генерацию видео...')
            generated_today = generate_video()
            if not generated_today:
                next_retry_after = time.time() + 1800
                print(f'[{now()}] Следующая попытка через 30 минут')

        if generated_today and not published_today:
            published_today = publish_story()

        time.sleep(30)


flask_app = Flask(__name__, static_folder='.')
flask_app.secret_key = os.environ.get('FLASK_SECRET', os.urandom(24).hex())


@flask_app.route('/favicon.ico')
def favicon():
    from flask import send_file
    return send_file('generated-icon.png', mimetype='image/png')


@flask_app.route('/', methods=['GET', 'POST'])
def login():
    if session.get('auth'):
        return redirect(url_for('admin'))
    error = False
    if request.method == 'POST':
        if request.form.get('password') == ADMIN_PASSWORD:
            session['auth'] = True
            return redirect(url_for('admin'))
        error = True
    return render_template('login.html', error=error)


@flask_app.route('/admin')
def admin():
    if not session.get('auth'):
        return redirect(url_for('login'))
    try:
        with open(METAPROMPT_PATH, encoding='utf-8') as f:
            metaprompt = f.read()
    except Exception:
        metaprompt = ''
    scenarios = [l.strip() for l in metaprompt.splitlines() if l.strip()]
    return render_template('admin.html',
                           metaprompt=metaprompt,
                           scenarios=scenarios,
                           status=app_state)


@flask_app.route('/save', methods=['POST'])
def save():
    if not session.get('auth'):
        return redirect(url_for('login'))
    metaprompt = request.form.get('metaprompt', '').strip()
    if not metaprompt:
        flash('Нельзя сохранить пустой список сценариев', 'error')
        return redirect(url_for('admin'))
    lines = [l.strip() for l in metaprompt.splitlines() if l.strip()]
    if len(lines) == 0:
        flash('Добавьте хотя бы один сценарий', 'error')
        return redirect(url_for('admin'))
    os.makedirs('config', exist_ok=True)
    with open(METAPROMPT_PATH, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines) + '\n')
    flash(f'Сохранено {len(lines)} сценариев', 'success')
    return redirect(url_for('admin'))


@flask_app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))


_scheduler_started = False

def start_scheduler():
    global _scheduler_started
    if not _scheduler_started:
        _scheduler_started = True
        t = threading.Thread(target=scheduler_loop, daemon=True)
        t.start()


start_scheduler()

if __name__ == '__main__':
    flask_app.run(host='0.0.0.0', port=5000, debug=False)
