import os
import time
import random
import requests
import subprocess
from datetime import datetime, timezone

FAL_KEY = os.environ['FAL_API_KEY']
VK_TOKEN = os.environ['VK_USER_TOKEN']
GROUP_ID = 236929597

FAL_MODEL = 'fal-ai/minimax/video-01'
FAL_SUBMIT_URL = f'https://queue.fal.run/{FAL_MODEL}'
FAL_STATUS_BASE = 'https://queue.fal.run/fal-ai/minimax/requests'
FAL_HEADERS = {'Authorization': f'Key {FAL_KEY}', 'Content-Type': 'application/json'}

VIDEO_PATH = '/tmp/story_raw.mp4'
VIDEO_VK_PATH = '/tmp/story_vk.mp4'

GENERATE_HOUR = 4
PUBLISH_HOUR = 6

SCENARIOS = [
    "Рыбки выпрыгивают из реки и превращаются в кирпичи, из которых за несколько секунд вырастает красивый дом",
    "Облака на небе слипаются и падают вниз, превращаясь в мешки с цементом, из которых строится дорожка",
    "Осенние листья летят по ветру и на лету превращаются в листы гипсокартона, складывающиеся в стену",
    "Волны океана накатывают на берег и застывают, образуя идеальную плитку для пола",
    "Стая птиц в небе выстраивается в форму дома и постепенно превращается в готовое здание",
    "Снежинки падают и укладываются в ровные ряды, образуя белоснежную кирпичную кладку",
    "Молнии ударяют в землю и оставляют после себя металлические балки перекрытий",
    "Радуга опускается на землю и её цвета превращаются в разноцветную керамическую плитку",
    "Пузырьки поднимаются со дна озера, на поверхности они лопаются и превращаются в пластиковые окна",
    "Стая рыб в аквариуме выстраивается в форму инструментов — дрелей, молотков, шпателей",
    "Семена одуванчика летят по ветру и там где приземляются — вырастают деревянные доски",
    "Огонь камина затухает и его угли превращаются в идеально ровный мраморный пол",
    "Муравьи тащат крошки хлеба, но крошки вдруг увеличиваются и становятся строительными блоками",
    "Водоворот воды в раковине закручивается и превращается в спиральную лестницу",
    "Перья птиц падают и складываются в утеплитель, который укладывается между стенами",
]


def generate_prompt():
    base = random.choice(SCENARIOS)
    style = random.choice([
        "вертикальное видео, кинематографическая съёмка, тёплый свет, 4K качество",
        "вертикальный формат, сюрреализм, яркие цвета, высокое качество",
        "вертикальное видео, магический реализм, красивое освещение",
        "вертикальный кадр, художественная съёмка, насыщенные цвета",
    ])
    return f"{base}. {style}."


def generate_video():
    prompt = generate_prompt()
    print(f'[{now()}] Промпт: {prompt[:80]}...')

    resp = requests.post(FAL_SUBMIT_URL, headers=FAL_HEADERS, json={
        'prompt': prompt,
        'duration': 6,
        'aspect_ratio': '9:16',
    })
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
                result = requests.get(f'{FAL_STATUS_BASE}/{request_id}', headers={'Authorization': f'Key {FAL_KEY}'}, timeout=10).json()
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

    r = requests.post('https://api.vk.com/method/stories.getVideoUploadServer', data={
        'group_id': GROUP_ID, 'add_to_news': 1, 'access_token': VK_TOKEN, 'v': '5.131'
    }, timeout=10)
    upload_url = r.json()['response']['upload_url']

    with open(VIDEO_VK_PATH, 'rb') as f:
        up = requests.post(upload_url, files={'video_file': f}, timeout=300)

    upload_result = up.json()['response']['upload_result']
    save = requests.post('https://api.vk.com/method/stories.save', data={
        'upload_results': upload_result, 'access_token': VK_TOKEN, 'v': '5.131'
    }, timeout=10).json()

    if 'response' in save:
        story_id = save['response']['items'][0]['id']
        print(f'[{now()}] ✓ История опубликована! ID: {story_id}')
        return True
    else:
        print(f'[{now()}] Ошибка публикации: {save}')
        return False


def now():
    return datetime.now(timezone.utc).strftime('%d.%m.%Y %H:%M:%S UTC')


def wait_until(target_hour):
    while True:
        current = datetime.now(timezone.utc)
        if current.hour == target_hour and current.minute == 0:
            return
        time.sleep(30)


print(f'[{now()}] Публикатор запущен. Генерация в {GENERATE_HOUR:02d}:00 UTC, публикация в {PUBLISH_HOUR:02d}:00 UTC.')

generated_today = False
published_today = False
last_date = None
next_retry_after = 0

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
