import os
import time
import random
import threading
import requests
import subprocess
import psycopg2
import psycopg2.extras
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


def get_db():
    return psycopg2.connect(os.environ['DATABASE_URL'])


def db_get(key, default=''):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT value FROM settings WHERE key = %s', (key,))
                row = cur.fetchone()
                return row[0] if row else default
    except Exception as e:
        print(f'[DB] Ошибка чтения {key}: {e}')
        return default


def db_set(key, value):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    INSERT INTO settings (key, value) VALUES (%s, %s)
                    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                ''', (key, value))
            conn.commit()
    except Exception as e:
        print(f'[DB] Ошибка записи {key}: {e}')


def parse_hhmm(s):
    try:
        h, m = s.strip().split(':')
        return int(h) % 24, int(m) % 60
    except Exception:
        return 6, 0

app_state = {
    'running': False,
    'last_published': None,
    'last_ok': False,
}

SUBJECTS = [
    'Стая рыб выпрыгивает из реки',
    'Снежинки падают с неба',
    'Осенние листья кружатся в воздухе',
    'Волны океана накатывают на берег',
    'Молнии бьют в землю',
    'Пузырьки поднимаются со дна озера',
    'Стая птиц летит над полем',
    'Бабочки порхают над цветами',
    'Капли дождя падают в лужу',
    'Льдинки тают на солнце',
    'Лепестки роз кружатся по ветру',
    'Искры вылетают из костра',
    'Муравьи несут крошки по тропинке',
    'Пчёлы роятся над ульем',
    'Листья берёзы падают в реку',
    'Семена одуванчика летят по ветру',
    'Огонь в камине догорает',
    'Дым поднимается спиралью вверх',
    'Снежный ком катится с горы',
    'Мыльные пузыри поднимаются в воздух',
    'Стая скворцов кружится в небе',
    'Кленовые вертолётики падают с дерева',
    'Горная лавина несётся вниз',
    'Звёзды падают с ночного неба',
    'Лавовый поток течёт по горе',
]

TRANSFORMATIONS = [
    'и превращаются в {material}, из которых {builds}',
    'и на лету трансформируются в {material} — {builds} словно сам собой',
    'и вдруг застывают, превращаясь в {material}, и {builds}',
    'и, коснувшись земли, становятся {material}, из которых {builds}',
    'и складываются в {material} — из них {builds}',
    'и рассыпаются {material}ом, который сам собой {builds}',
    'и в замедленной съёмке превращаются в {material}, {builds}',
    'и взрываются облаком {material}, из которого {builds}',
]

MATERIALS = [
    'кирпичи',
    'деревянные доски',
    'керамическую плитку',
    'стеклянные блоки',
    'бетонные панели',
    'черепицу',
    'мраморные плиты',
    'металлические балки',
    'рулоны утеплителя',
    'брёвна',
    'гранитный щебень',
    'листы фанеры',
    'рулоны рубероида',
    'арматурные прутья',
    'сайдинг',
]

BUILDS = [
    'вырастает красивый коттедж',
    'складывается уютный деревянный дом',
    'появляется кирпичный особняк',
    'строится загородный дом',
    'возникает терраса с видом на лес',
    'строится забор вокруг сада',
    'появляется крыша над головой',
    'складывается камин в гостиной',
    'вырастает стена дома',
    'строится дорожка к дому',
    'появляется веранда',
    'складывается гараж',
    'вырастает баня',
    'строится беседка в саду',
]

SETTINGS = [
    'на фоне заката над лесом',
    'в осеннем лесу',
    'у тихой реки на рассвете',
    'в заснеженном поле',
    'в летнем саду',
    'на берегу озера',
    'среди зелёных холмов',
    'в хвойном лесу',
    'на фоне грозового неба',
    'в золотой час заката',
]

STYLES = [
    'Кинематографическая съёмка, тёплый свет, 4K, вертикальное видео 9:16.',
    'Магический реализм, яркие насыщенные цвета, вертикальный формат 9:16.',
    'Художественная съёмка, мягкое освещение, сюрреализм, вертикальное видео 9:16.',
    'Визуальный аттракцион, замедленная съёмка, кинематограф, 9:16.',
    'Эпичная широкоугольная съёмка, золотой закат, вертикальный формат 9:16.',
]


def load_metaprompt():
    return db_get('metaprompt', 'Залипательное на тему строительства и ремонта.')


def generate_prompt():
    subject = random.choice(SUBJECTS)
    transform_template = random.choice(TRANSFORMATIONS)
    material = random.choice(MATERIALS)
    build = random.choice(BUILDS)
    setting = random.choice(SETTINGS)
    style = random.choice(STYLES)

    transform = transform_template.format(material=material, builds=build)
    scene = f'{subject} {transform}, {setting}.'
    full_prompt = f'{scene} {style}'

    print(f'[{now()}] Сюжет: {scene[:100]}')
    return full_prompt


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


def publish_to_wall():
    print(f'[{now()}] Публикую видео на стену сообщества...')
    try:
        save_resp = requests.post('https://api.vk.com/method/video.save', data={
            'group_id': GROUP_ID,
            'name': 'Строительство и ремонт',
            'description': '',
            'wallpost': 0,
            'access_token': VK_TOKEN,
            'v': '5.131',
        }, timeout=15).json()

        if 'error' in save_resp:
            print(f'[{now()}] Ошибка video.save: {save_resp["error"]}')
            return False

        upload_url = save_resp['response']['upload_url']
        video_id = save_resp['response']['video_id']
        owner_id = save_resp['response']['owner_id']
        print(f'[{now()}] video.save OK, загружаю файл...')

        with open(VIDEO_VK_PATH, 'rb') as f:
            up = requests.post(upload_url, files={'video_file': f}, timeout=300)
        up.raise_for_status()
        print(f'[{now()}] Видео загружено. Публикую пост...')

        post_resp = requests.post('https://api.vk.com/method/wall.post', data={
            'owner_id': -GROUP_ID,
            'from_group': 1,
            'attachments': f'video{owner_id}_{video_id}',
            'access_token': VK_TOKEN,
            'v': '5.131',
        }, timeout=15).json()

        if 'response' in post_resp:
            post_id = post_resp['response']['post_id']
            print(f'[{now()}] ✓ Видео опубликовано на стене! post_id: {post_id}')
            return True
        else:
            print(f'[{now()}] Ошибка wall.post: {post_resp}')
            return False
    except Exception as e:
        print(f'[{now()}] Исключение при публикации на стену: {e}')
        return False


def scheduler_loop():
    generated_today = False
    published_today = False
    last_date = None
    next_retry_after = 0

    while True:
        pub_h, pub_m = parse_hhmm(db_get('publish_time', '06:00'))
        gen_h = (pub_h - 2) % 24
        gen_m = pub_m

        now_dt = datetime.now(timezone.utc)
        today = now_dt.date()

        if today != last_date:
            generated_today = False
            published_today = False
            last_date = today
            next_retry_after = 0
            print(f'[{now()}] Новый день. Генерация в {gen_h:02d}:{gen_m:02d} UTC, публикация в {pub_h:02d}:{pub_m:02d} UTC.')

        now_minutes = now_dt.hour * 60 + now_dt.minute
        gen_minutes = gen_h * 60 + gen_m
        pub_minutes = pub_h * 60 + pub_m

        if now_minutes >= gen_minutes and not generated_today and time.time() >= next_retry_after:
            print(f'[{now()}] Запускаю генерацию видео...')
            generated_today = generate_video()
            if not generated_today:
                next_retry_after = time.time() + 1800
                print(f'[{now()}] Следующая попытка через 30 минут')

        if generated_today and not published_today and now_minutes >= pub_minutes:
            story_ok = publish_story()
            wall_ok = publish_to_wall()
            published_today = story_ok or wall_ok

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
    metaprompt = db_get('metaprompt', '')
    pub_h, pub_m = parse_hhmm(db_get('publish_time', '06:00'))
    gen_h = (pub_h - 2) % 24
    return render_template('admin.html',
                           metaprompt=metaprompt,
                           publish_time=f'{pub_h:02d}:{pub_m:02d}',
                           generate_time=f'{gen_h:02d}:{pub_m:02d}',
                           status=app_state)


@flask_app.route('/save', methods=['POST'])
def save():
    if not session.get('auth'):
        return redirect(url_for('login'))
    metaprompt = request.form.get('metaprompt', '').strip()
    if not metaprompt:
        flash('Мета-промпт не может быть пустым', 'error')
        return redirect(url_for('admin'))
    db_set('metaprompt', metaprompt)

    pub_time = request.form.get('publish_time', '').strip()
    if pub_time:
        h, m = parse_hhmm(pub_time)
        db_set('publish_time', f'{h:02d}:{m:02d}')

    flash('Настройки сохранены', 'success')
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
