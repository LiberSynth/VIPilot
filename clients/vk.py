"""
VK API-клиент.
Отвечает за публикацию видео в историю и на стену сообщества ВКонтакте.
- История: stories.getVideoUploadServer → stories.save
- Стена + ВКВидео (клип): shortVideo.create → upload → shortVideo.edit → shortVideo.publish
Принимает видео в виде байт (bytes) — никаких файлов на диске.
"""

import io
import os
import time

import requests

from log import write_log_entry
from utils.utils import fmt_id_msg
from routes.api import build_publication_title, publication_file_name, hashtags

_VK_TOKEN = os.environ.get('VK_USER_TOKEN', '')
_VK_API   = 'https://api.vk.com/method'
_VK_VER   = '5.199'


def publish_story(video_data: bytes, group_id: int, log_id) -> int | None:
    """Публикует видео как историю ВКонтакте. Возвращает story_id или None."""
    r = requests.post(f'{_VK_API}/stories.getVideoUploadServer', data={
        'group_id':    group_id,
        'add_to_news': 1,
        'access_token': _VK_TOKEN,
        'v': _VK_VER,
    }, timeout=15).json()

    if 'error' in r:
        if log_id:
            write_log_entry(log_id, f"getVideoUploadServer: {r['error']}", level='error')
        return None

    upload_url = r['response']['upload_url']
    pub_title = build_publication_title()
    filename = publication_file_name(pub_title)

    for attempt in range(3):
        try:
            up = requests.post(
                upload_url,
                files={'video_file': (filename, io.BytesIO(video_data), 'video/mp4')},
                timeout=300,
            )
            up.raise_for_status()
            if not up.text.strip():
                if log_id:
                    write_log_entry(log_id, f'Пустой ответ CDN (попытка {attempt+1}/3)', level='warn')
                time.sleep(5)
                continue
            up_data = up.json()
            if 'response' not in up_data:
                if log_id:
                    write_log_entry(log_id, f'Неожиданный ответ CDN: {up.text[:200]}', level='error')
                return None
            upload_result = up_data['response']['upload_result']
            break
        except Exception as e:
            if log_id:
                write_log_entry(log_id, f'Ошибка загрузки (попытка {attempt+1}/3): {e}', level='warn')
            time.sleep(5)
    else:
        if log_id:
            write_log_entry(log_id, 'Все попытки загрузки истории провалились', level='error')
        return None

    save = requests.post(f'{_VK_API}/stories.save', data={
        'upload_results': upload_result,
        'access_token': _VK_TOKEN,
        'v': _VK_VER,
    }, timeout=15).json()

    if 'response' in save:
        story_id = save['response']['items'][0]['id']
        if log_id:
            write_log_entry(log_id, fmt_id_msg('История опубликована: id={}', story_id))
        return story_id

    if log_id:
        write_log_entry(log_id, f"stories.save: {save.get('error', save)}", level='error')
    return None


def publish_wall(video_data: bytes, group_id: int, log_id) -> int | None:
    """Публикует видео как клип ВКВидео и на стену сообщества. Возвращает video_id или None."""
    pub_title = build_publication_title()
    file_size = len(video_data)
    filename  = publication_file_name(pub_title)

    # ── 1. Получаем URL для загрузки клипа ───────────────────────────────
    create_resp = requests.post(f'{_VK_API}/shortVideo.create', data={
        'group_id':     group_id,
        'file_size':    file_size,
        'name':         pub_title,
        'access_token': _VK_TOKEN,
        'v': _VK_VER,
    }, timeout=15).json()

    if 'error' in create_resp:
        if log_id:
            write_log_entry(log_id, f"shortVideo.create: {create_resp['error']}", level='error')
        return None

    upload_url = create_resp['response']['upload_url']

    # ── 2. Загружаем видео ────────────────────────────────────────────────
    up = requests.post(
        upload_url,
        files={'file': (filename, io.BytesIO(video_data), 'video/mp4')},
        timeout=300,
    )
    up.raise_for_status()
    video_info = up.json()
    video_id   = video_info['video_id']
    owner_id   = video_info['owner_id']

    if log_id:
        write_log_entry(log_id, fmt_id_msg('ВК: видео загружено, video_id={}', video_id))

    # ── 3. Ждём обработки ────────────────────────────────────────────────
    time.sleep(10)

    # ── 4. Добавляем хэштеги ─────────────────────────────────────────────
    edit_resp = requests.post(f'{_VK_API}/shortVideo.edit', data={
        'video_id':     video_id,
        'owner_id':     owner_id,
        'description':  hashtags(),
        'access_token': _VK_TOKEN,
        'v': _VK_VER,
    }, timeout=15).json()

    if 'error' in edit_resp:
        if log_id:
            write_log_entry(log_id, f"shortVideo.edit: {edit_resp['error']}", level='warn')

    # ── 5. Публикуем в клипы и на стену ──────────────────────────────────
    pub_resp = requests.post(f'{_VK_API}/shortVideo.publish', data={
        'video_id':      video_id,
        'owner_id':      owner_id,
        'license_agree': 1,
        'wallpost':      1,
        'access_token':  _VK_TOKEN,
        'v': _VK_VER,
    }, timeout=15).json()

    if 'error' in pub_resp:
        if log_id:
            write_log_entry(log_id, f"shortVideo.publish: {pub_resp['error']}", level='error')
        return None

    if log_id:
        write_log_entry(log_id, fmt_id_msg('Клип опубликован в ВКВидео и на стене: video_id={}', video_id))
    return video_id
