import os
import time
import random
import threading
import requests
import subprocess
import psycopg2
import psycopg2.extras
from collections import deque
from datetime import datetime, timezone, timedelta
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify

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

MSK_OFFSET = timedelta(hours=3)


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


def parse_lead_mins(s):
    try:
        return max(10, min(1440, int(s)))
    except Exception:
        return 120


def parse_history_days(s):
    try:
        return max(1, min(365, int(s)))
    except Exception:
        return 7


def to_msk(h, m):
    total = (h * 60 + m + 180) % 1440
    return total // 60, total % 60


def to_utc_from_msk(h, m):
    total = (h * 60 + m - 180) % 1440
    return total // 60, total % 60


def init_db():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS settings (
                        key VARCHAR(100) PRIMARY KEY,
                        value TEXT NOT NULL
                    )
                ''')
                cur.execute('''
                    INSERT INTO settings (key, value) VALUES
                        ('metaprompt', 'Залипательное на тему ремонта, коттеджного строительства и продажи стройматериалов. Сюжет подбирай случайным образом. Неожиданный, вплоть до абсурдного, удивляющий, умеренно шокирующий, при этом красивый. Например: река, рыбки выпрыгивают из воды, они превращаются в стройматериалы, река исчезает и из них получается дом.'),
                        ('publish_time', '03:00'),
                        ('lead_time_mins', '120'),
                        ('notify_email', ''),
                        ('notify_phone', '')
                    ON CONFLICT (key) DO NOTHING
                ''')
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS video_urls (
                        id SERIAL PRIMARY KEY,
                        url TEXT NOT NULL UNIQUE,
                        created_at FLOAT NOT NULL
                    )
                ''')
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS cycles (
                        id SERIAL PRIMARY KEY,
                        started TEXT NOT NULL,
                        started_ts FLOAT NOT NULL,
                        status TEXT NOT NULL,
                        entries JSONB NOT NULL DEFAULT '[]',
                        summary JSONB NOT NULL DEFAULT '{}'
                    )
                ''')
            conn.commit()
        print('[DB] Инициализация выполнена')
    except Exception as e:
        print(f'[DB] Ошибка инициализации: {e}')


def db_save_cycle(cycle):
    import json
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    INSERT INTO cycles (started, started_ts, status, entries, summary)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id
                ''', (
                    cycle['started'],
                    cycle['started_ts'],
                    cycle['status'],
                    json.dumps(cycle['entries'], ensure_ascii=False),
                    json.dumps(cycle.get('summary', {}), ensure_ascii=False),
                ))
                row = cur.fetchone()
                cycle['db_id'] = row[0]
            conn.commit()
    except Exception as e:
        print(f'[DB] Ошибка сохранения цикла: {e}')


def db_load_cycles():
    import json
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    SELECT id, started, started_ts, status, entries, summary
                    FROM cycles ORDER BY started_ts DESC LIMIT 20
                ''')
                rows = cur.fetchall()
        result = []
        for row in rows:
            result.append({
                'db_id': row[0],
                'started': row[1],
                'started_ts': row[2],
                'status': row[3],
                'entries': row[4] if isinstance(row[4], list) else json.loads(row[4] or '[]'),
                'summary': row[5] if isinstance(row[5], dict) else json.loads(row[5] or '{}'),
            })
        return result
    except Exception as e:
        print(f'[DB] Ошибка загрузки циклов: {e}')
        return []


def db_trim_cycles(keep=20):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    DELETE FROM cycles WHERE id NOT IN (
                        SELECT id FROM cycles ORDER BY started_ts DESC LIMIT %s
                    )
                ''', (keep,))
            conn.commit()
    except Exception as e:
        print(f'[DB] Ошибка обрезки циклов: {e}')


app_state = {
    'running': False,
    'last_published': None,
    'last_ok': False,
    'current_prompt': None,
    'current_cycle': None,   # dict with 'started', 'status', 'entries' (list)
    'cycles': deque(maxlen=20),  # completed cycles, newest first
}


def msk_ts():
    return (datetime.now(timezone.utc) + MSK_OFFSET).strftime('%d.%m.%Y %H:%M МСК')


def start_cycle():
    cycle = {
        'started': msk_ts(),
        'started_ts': time.time(),
        'status': 'running',
        'entries': [],
        'summary': {'prompt': None, 'generated_at': None, 'published_at': None},
    }
    app_state['current_cycle'] = cycle
    return cycle


def end_cycle(ok):
    cycle = app_state['current_cycle']
    if cycle is None:
        return
    cycle['status'] = 'ok' if ok else 'error'
    completed = dict(cycle)
    app_state['cycles'].appendleft(completed)
    app_state['current_cycle'] = None
    db_save_cycle(completed)
    db_trim_cycles(keep=20)


def log_msg(msg, level='info'):
    ts = (datetime.now(timezone.utc) + MSK_OFFSET).strftime('%d.%m %H:%M:%S')
    entry = {'ts': ts, 'msg': msg, 'level': level}
    if app_state['current_cycle'] is not None:
        app_state['current_cycle']['entries'].append(entry)
    print(f'[{ts} МСК] {msg}')


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

    app_state['current_prompt'] = scene
    if app_state['current_cycle'] is not None:
        app_state['current_cycle']['summary']['prompt'] = scene
    log_msg(f'Сюжет: {scene}')
    return full_prompt


def is_emulation():
    return db_get('emulation_mode', '0') == '1'


def db_save_video_url(url):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT 1 FROM video_urls WHERE url = %s', (url,))
                if cur.fetchone():
                    return
                cur.execute(
                    'INSERT INTO video_urls (url, created_at) VALUES (%s, %s)',
                    (url, time.time())
                )
                cur.execute('SELECT COUNT(*) FROM video_urls')
                count = cur.fetchone()[0]
                if count > 50:
                    cur.execute(
                        'DELETE FROM video_urls WHERE id IN '
                        '(SELECT id FROM video_urls ORDER BY created_at ASC LIMIT %s)',
                        (count - 50,)
                    )
            conn.commit()
    except Exception as e:
        print(f'[DB] Ошибка сохранения URL видео: {e}')


def db_get_random_video_url():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT url FROM video_urls ORDER BY RANDOM() LIMIT 1')
                row = cur.fetchone()
                return row[0] if row else None
    except Exception as e:
        print(f'[DB] Ошибка получения URL видео: {e}')
        return None


def fal_request_id_to_url(request_id):
    try:
        r = requests.get(
            f'{FAL_STATUS_BASE}/{request_id}',
            headers={'Authorization': f'Key {FAL_KEY}'},
            timeout=10
        )
        r.raise_for_status()
        return r.json().get('video', {}).get('url')
    except Exception as e:
        return None


def transcode_video():
    raw_size = os.path.getsize(VIDEO_PATH)
    log_msg(f'Транскодирую в H.264... (исходник: {round(raw_size/1024/1024, 1)} МБ)')
    result = subprocess.run([
        'ffmpeg',
        '-t', '8', '-i', VIDEO_PATH,
        '-f', 'lavfi', '-i', 'anullsrc=r=44100:cl=stereo',
        '-t', '8',
        '-c:v', 'libx264', '-profile:v', 'baseline', '-preset', 'ultrafast', '-crf', '26',
        '-pix_fmt', 'yuv420p', '-r', '30',
        '-c:a', 'aac', '-b:a', '96k',
        '-movflags', '+faststart',
        VIDEO_VK_PATH, '-y'
    ], capture_output=True, timeout=600)
    if result.returncode != 0:
        err = result.stderr.decode(errors='replace')[-600:]
        log_msg(f'ffmpeg ошибка (код {result.returncode}): {err}', 'error')
        return False
    log_msg('Транскодирование завершено')
    if app_state['current_cycle'] is not None:
        app_state['current_cycle']['summary']['generated_at'] = msk_ts()
    return True


def generate_video():
    prompt = generate_prompt()
    app_state['running'] = True

    if is_emulation():
        try:
            log_msg('[ЭМУЛЯЦИЯ] Пропускаю генерацию, беру случайное видео из базы...')
            url = db_get_random_video_url()
            if not url:
                log_msg('[ЭМУЛЯЦИЯ] В базе нет видео — добавьте ID запросов fal.ai через панель', 'error')
                return False
            log_msg('[ЭМУЛЯЦИЯ] Видео выбрано, скачиваю...')
            url = url + '_BROKEN_TEST'  # TEST FAILURE
            return download_and_transcode(url)
        finally:
            app_state['running'] = False

    try:
        resp = requests.post(FAL_SUBMIT_URL, headers=FAL_HEADERS, json={
            'prompt': prompt,
            'duration': 6,
            'aspect_ratio': '9:16',
        }, timeout=30)
        data = resp.json()

        if 'request_id' not in data:
            log_msg(f'Ошибка запроса к fal.ai: {data}', 'error')
            return False

        request_id = data['request_id']
        status_url = data['status_url']
        log_msg(f'Генерация запущена. ID: {request_id}')

        for attempt in range(240):
            time.sleep(30)
            try:
                s = requests.get(status_url, headers={'Authorization': f'Key {FAL_KEY}'}, timeout=10).json()
                status = s.get('status')
                log_msg(f'Статус [{attempt+1}]: {status}')

                if status == 'COMPLETED':
                    try:
                        result = requests.get(
                            f'{FAL_STATUS_BASE}/{request_id}',
                            headers={'Authorization': f'Key {FAL_KEY}'},
                            timeout=10
                        ).json()
                        video_url = result.get('video', {}).get('url')
                        if not video_url:
                            log_msg(f'Нет URL видео в ответе: {result}', 'error')
                            return False
                        return download_and_transcode(video_url)
                    except Exception as e:
                        log_msg(f'Ошибка обработки готового видео: {e}', 'error')
                        return False

                elif status == 'FAILED':
                    log_msg(f'Генерация провалилась: {s}', 'error')
                    return False
            except Exception as e:
                log_msg(f'Ошибка опроса статуса: {e}', 'error')

        log_msg('Таймаут генерации (2 часа)', 'error')
        return False
    finally:
        app_state['running'] = False


def download_and_transcode(video_url):
    log_msg('Скачиваю видео...')
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
                    log_msg(f'Видео скачано: {round(size/1024/1024, 1)} МБ')
                    ok = True
                    break
            else:
                log_msg(f'HTTP {r.status_code} при скачивании, попытка {attempt+1}/3', 'error')
        except Exception as e:
            log_msg(f'Ошибка скачивания (попытка {attempt+1}/3): {e}', 'error')
        time.sleep(10)
    if not ok:
        return False

    result = transcode_video()
    if result and not is_emulation():
        db_save_video_url(video_url)
    return result


def publish_story():
    log_msg('Публикую историю в VK...')
    try:
        r = requests.post('https://api.vk.com/method/stories.getVideoUploadServer', data={
            'group_id': GROUP_ID, 'add_to_news': 1, 'access_token': VK_TOKEN, 'v': '5.131'
        }, timeout=15)
        r.raise_for_status()
        server_data = r.json()
        if 'error' in server_data:
            log_msg(f'Ошибка getVideoUploadServer: {server_data["error"]}', 'error')
            return False
        upload_url = server_data['response']['upload_url']
        log_msg('Upload URL получен, загружаю...')

        for attempt in range(3):
            try:
                with open(VIDEO_VK_PATH, 'rb') as f:
                    up = requests.post(upload_url, files={'video_file': f}, timeout=300)
                up.raise_for_status()
                if not up.text.strip():
                    log_msg(f'Пустой ответ от CDN, попытка {attempt+1}/3', 'error')
                    time.sleep(5)
                    continue
                up_data = up.json()
                if 'response' not in up_data:
                    log_msg(f'Неожиданный ответ CDN: {up.text[:200]}', 'error')
                    return False
                upload_result = up_data['response']['upload_result']
                break
            except Exception as e:
                log_msg(f'Ошибка загрузки видео (попытка {attempt+1}/3): {e}', 'error')
                time.sleep(5)
        else:
            log_msg('Все попытки загрузки провалились', 'error')
            return False

        log_msg('Видео загружено, сохраняю историю...')
        save = requests.post('https://api.vk.com/method/stories.save', data={
            'upload_results': upload_result, 'access_token': VK_TOKEN, 'v': '5.131'
        }, timeout=15).json()

        if 'response' in save:
            story_id = save['response']['items'][0]['id']
            ts = msk_ts()
            log_msg(f'✓ История опубликована! ID: {story_id}', 'ok')
            app_state['last_published'] = ts
            app_state['last_ok'] = True
            if app_state['current_cycle'] is not None:
                app_state['current_cycle']['summary']['published_at'] = ts
            return True
        else:
            log_msg(f'Ошибка stories.save: {save}', 'error')
            return False
    except Exception as e:
        log_msg(f'Исключение при публикации истории: {e}', 'error')
        return False


def publish_to_wall():
    log_msg('Публикую видео на стену сообщества...')
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
            log_msg(f'Ошибка video.save: {save_resp["error"]}', 'error')
            return False

        upload_url = save_resp['response']['upload_url']
        video_id = save_resp['response']['video_id']
        owner_id = save_resp['response']['owner_id']
        log_msg('video.save OK, загружаю файл...')

        with open(VIDEO_VK_PATH, 'rb') as f:
            up = requests.post(upload_url, files={'video_file': f}, timeout=300)
        up.raise_for_status()
        log_msg('Видео загружено. Публикую пост...')

        post_resp = requests.post('https://api.vk.com/method/wall.post', data={
            'owner_id': -GROUP_ID,
            'from_group': 1,
            'attachments': f'video{owner_id}_{video_id}',
            'access_token': VK_TOKEN,
            'v': '5.131',
        }, timeout=15).json()

        if 'response' in post_resp:
            post_id = post_resp['response']['post_id']
            log_msg(f'✓ Видео опубликовано на стене! post_id: {post_id}', 'ok')
            return True
        else:
            log_msg(f'Ошибка wall.post: {post_resp}', 'error')
            return False
    except Exception as e:
        log_msg(f'Исключение при публикации на стену: {e}', 'error')
        return False


def send_failure_email(message, log_entries=None):
    import smtplib
    from email.mime.text import MIMEText
    to_addr = db_get('notify_email', '').strip()
    smtp_host = os.environ.get('SMTP_HOST', '').strip()
    smtp_user = os.environ.get('SMTP_USER', '').strip()
    smtp_pass = os.environ.get('SMTP_PASSWORD', '').strip()
    if not all([to_addr, smtp_host, smtp_user, smtp_pass]):
        return
    smtp_port = int(os.environ.get('SMTP_PORT', '587'))
    smtp_from = os.environ.get('SMTP_FROM', smtp_user)
    try:
        body = message
        if log_entries:
            lines = '\n'.join(f"[{e['ts']}] {e['msg']}" for e in log_entries)
            body += f'\n\n--- Подробный лог ---\n{lines}'
        msg = MIMEText(body, 'plain', 'utf-8')
        msg['Subject'] = 'VK Publisher: сбой в пайплайне'
        msg['From'] = smtp_from
        msg['To'] = to_addr
        with smtplib.SMTP(smtp_host, smtp_port, timeout=15) as s:
            s.starttls()
            s.login(smtp_user, smtp_pass)
            s.send_message(msg)
        log_msg(f'[УВЕДОМЛЕНИЕ] Email отправлен на {to_addr}')
    except Exception as e:
        log_msg(f'[УВЕДОМЛЕНИЕ] Ошибка отправки email: {e}', 'error')


def send_failure_sms(message):
    phone = db_get('notify_phone', '').strip()
    smsc_login = os.environ.get('SMSC_LOGIN', '').strip()
    smsc_pass = os.environ.get('SMSC_PASS', '').strip()
    if not all([phone, smsc_login, smsc_pass]):
        return
    try:
        r = requests.get('https://smsc.ru/sys/send.php', params={
            'login': smsc_login,
            'psw': smsc_pass,
            'phones': phone,
            'mes': message[:160],
            'charset': 'utf-8',
            'fmt': 3,
        }, timeout=10)
        data = r.json()
        if data.get('error_code'):
            print(f'[NOTIFY] SMSC ошибка: {data}')
        else:
            print(f'[NOTIFY] SMS отправлено на {phone}')
    except Exception as e:
        print(f'[NOTIFY] Ошибка отправки SMS: {e}')


def notify_failure(reason):
    msg = f'Сбой {msk_ts()}: {reason}'
    log_msg(f'[УВЕДОМЛЕНИЕ] Отправляю уведомление о сбое: {reason}')
    cycle = app_state.get('current_cycle') or (app_state['cycles'][0] if app_state['cycles'] else None)
    entries = cycle.get('entries', []) if cycle else []
    send_failure_email(msg, log_entries=entries)
    send_failure_sms(msg)


def run_full_cycle():
    start_cycle()
    gen_ok = generate_video()
    pub_ok = False
    if gen_ok:
        story_ok = publish_story()
        wall_ok = publish_to_wall()
        pub_ok = story_ok or wall_ok
    success = pub_ok if gen_ok else False
    end_cycle(success)
    if not success:
        reason = 'ошибка генерации видео' if not gen_ok else 'ошибка публикации в VK'
        notify_failure(reason)
    return gen_ok, pub_ok


def scheduler_loop():
    generated_today = False
    published_today = False
    last_date = None
    next_retry_after = 0

    while True:
        pub_h, pub_m = parse_hhmm(db_get('publish_time', '03:00'))
        lead_mins = parse_lead_mins(db_get('lead_time_mins', '120'))

        gen_total = (pub_h * 60 + pub_m - lead_mins) % 1440
        gen_h = gen_total // 60
        gen_m = gen_total % 60

        now_utc = datetime.now(timezone.utc)
        today = now_utc.date()

        if today != last_date:
            generated_today = False
            published_today = False
            last_date = today
            next_retry_after = 0
            pub_h_msk, pub_m_msk = to_msk(pub_h, pub_m)
            gen_h_msk, gen_m_msk = to_msk(gen_h, gen_m)
            print(
                f'[{now_utc.strftime("%d.%m %H:%M:%S")} UTC] '
                f'Новый день. Генерация в {gen_h_msk:02d}:{gen_m_msk:02d} МСК, '
                f'публикация в {pub_h_msk:02d}:{pub_m_msk:02d} МСК '
                f'(упреждение {lead_mins} мин).'
            )

        now_mins = now_utc.hour * 60 + now_utc.minute
        pub_mins = pub_h * 60 + pub_m
        mins_to_pub = (pub_mins - now_mins) % 1440
        should_generate = mins_to_pub <= lead_mins

        if should_generate and not generated_today and not app_state['running'] and time.time() >= next_retry_after:
            gen_ok, pub_ok = run_full_cycle()
            generated_today = gen_ok
            published_today = pub_ok
            if not gen_ok:
                next_retry_after = time.time() + 1800
                print('[scheduler] Генерация не удалась. Следующая попытка через 30 минут.')
        elif generated_today and not published_today:
            now_mins_fresh = datetime.now(timezone.utc).hour * 60 + datetime.now(timezone.utc).minute
            mins_past_pub = (now_mins_fresh - pub_mins) % 1440
            if mins_past_pub < 720:
                start_cycle()
                story_ok = publish_story()
                wall_ok = publish_to_wall()
                published_today = story_ok or wall_ok
                end_cycle(published_today)

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
    pub_h_utc, pub_m_utc = parse_hhmm(db_get('publish_time', '03:00'))
    lead_mins = parse_lead_mins(db_get('lead_time_mins', '120'))

    gen_total = (pub_h_utc * 60 + pub_m_utc - lead_mins) % 1440
    gen_h_utc = gen_total // 60
    gen_m_utc = gen_total % 60

    pub_h_msk, pub_m_msk = to_msk(pub_h_utc, pub_m_utc)
    gen_h_msk, gen_m_msk = to_msk(gen_h_utc, gen_m_utc)

    history_days = parse_history_days(db_get('history_days', '7'))
    emulation_mode = db_get('emulation_mode', '0') == '1'
    notify_email = db_get('notify_email', '')
    notify_phone = db_get('notify_phone', '')

    return render_template('admin.html',
                           metaprompt=metaprompt,
                           publish_time_msk=f'{pub_h_msk:02d}:{pub_m_msk:02d}',
                           generate_time_msk=f'{gen_h_msk:02d}:{gen_m_msk:02d}',
                           lead_time_mins=lead_mins,
                           history_days=history_days,
                           emulation_mode=emulation_mode,
                           notify_email=notify_email,
                           notify_phone=notify_phone,
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

    pub_time_msk = request.form.get('publish_time', '').strip()
    if pub_time_msk:
        h_msk, m_msk = parse_hhmm(pub_time_msk)
        h_utc, m_utc = to_utc_from_msk(h_msk, m_msk)
        db_set('publish_time', f'{h_utc:02d}:{m_utc:02d}')

    lead_raw = request.form.get('lead_time_mins', '').strip()
    if lead_raw:
        db_set('lead_time_mins', str(parse_lead_mins(lead_raw)))

    history_raw = request.form.get('history_days', '').strip()
    if history_raw:
        db_set('history_days', str(parse_history_days(history_raw)))

    emulation_raw = request.form.get('emulation_mode', '0')
    db_set('emulation_mode', '1' if emulation_raw == '1' else '0')

    db_set('notify_email', request.form.get('notify_email', '').strip())
    db_set('notify_phone', request.form.get('notify_phone', '').strip())

    flash('Настройки сохранены', 'success')
    return redirect(url_for('admin'))


@flask_app.route('/log-data')
def log_data():
    if not session.get('auth'):
        return jsonify({})

    history_days = parse_history_days(db_get('history_days', '7'))
    cutoff_ts = time.time() - history_days * 86400

    def serialize_cycle(c, is_current=False):
        age_ok = is_current or c.get('started_ts', 0) >= cutoff_ts
        return {
            'started': c['started'],
            'started_ts': c.get('started_ts', 0),
            'status': c['status'],
            'summary': c.get('summary', {}),
            'entries': c['entries'] if age_ok else [],
        }

    cycles = [serialize_cycle(c) for c in app_state['cycles']]
    current = app_state['current_cycle']
    if current:
        cycles = [serialize_cycle(current, is_current=True)] + cycles

    return jsonify({
        'running': app_state['running'],
        'current_prompt': app_state['current_prompt'],
        'cycles': cycles,
    })


@flask_app.route('/run-now', methods=['POST'])
def run_now():
    if not session.get('auth'):
        return redirect(url_for('login'))
    if app_state['running']:
        flash('Генерация уже запущена', 'error')
        return redirect(url_for('admin'))

    def run():
        try:
            run_full_cycle()
        except Exception as e:
            log_msg(f'Критическая ошибка цикла: {e}', 'error')
            notify_failure(f'необработанное исключение: {e}')

    t = threading.Thread(target=run, daemon=True)
    t.start()
    flash('Цикл запущен — смотрите логи', 'success')
    return redirect(url_for('admin'))


@flask_app.route('/test-notify', methods=['POST'])
def test_notify():
    if not session.get('auth'):
        return redirect(url_for('login'))
    notify_failure('тестовый сбой (проверка уведомлений)')
    flash('Тестовое уведомление отправлено', 'success')
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
        init_db()
        saved = db_load_cycles()
        for c in saved:
            app_state['cycles'].append(c)
        if saved:
            last = saved[0]
            if last.get('summary', {}).get('published_at'):
                app_state['last_published'] = last['summary']['published_at']
                app_state['last_ok'] = last['status'] == 'ok'
        print(f'[DB] Загружено циклов из БД: {len(saved)}')
        t = threading.Thread(target=scheduler_loop, daemon=True)
        t.start()


start_scheduler()

if __name__ == '__main__':
    flask_app.run(host='0.0.0.0', port=5000, debug=False)
