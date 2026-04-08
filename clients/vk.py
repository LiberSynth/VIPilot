"""
VK API-клиент.
Отвечает за публикацию видео в историю и на стену сообщества ВКонтакте.
Принимает видео в виде байт (bytes) — никаких файлов на диске.
"""

import io
import os
import re
import time

import requests

from log import db_log_entry

_VK_TOKEN = os.environ.get('VK_USER_TOKEN', '')
_VK_API   = 'https://api.vk.com/method'
_VK_VER   = '5.131'


def _safe_filename(title: str) -> str:
    """Возвращает безопасное имя файла из title (без спецсимволов, макс 80 символов)."""
    safe = re.sub(r'[^\w\s\-]', '', title, flags=re.UNICODE).strip()
    safe = re.sub(r'\s+', '_', safe)[:80]
    return safe or 'video'


def publish_story(video_data: bytes, group_id: int, log_id, title: str = '') -> int | None:
    """Публикует видео как историю ВКонтакте. Возвращает story_id или None."""
    r = requests.post(f'{_VK_API}/stories.getVideoUploadServer', data={
        'group_id':    group_id,
        'add_to_news': 1,
        'access_token': _VK_TOKEN,
        'v': _VK_VER,
    }, timeout=15).json()

    if 'error' in r:
        if log_id:
            db_log_entry(log_id, f"getVideoUploadServer: {r['error']}", level='error')
        return None

    upload_url = r['response']['upload_url']
    filename = f"{_safe_filename(title)}.mp4" if title else 'video.mp4'

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
                    db_log_entry(log_id, f'Пустой ответ CDN (попытка {attempt+1}/3)', level='warn')
                time.sleep(5)
                continue
            up_data = up.json()
            if 'response' not in up_data:
                if log_id:
                    db_log_entry(log_id, f'Неожиданный ответ CDN: {up.text[:200]}', level='error')
                return None
            upload_result = up_data['response']['upload_result']
            break
        except Exception as e:
            if log_id:
                db_log_entry(log_id, f'Ошибка загрузки (попытка {attempt+1}/3): {e}', level='warn')
            time.sleep(5)
    else:
        if log_id:
            db_log_entry(log_id, 'Все попытки загрузки истории провалились', level='error')
        return None

    save = requests.post(f'{_VK_API}/stories.save', data={
        'upload_results': upload_result,
        'access_token': _VK_TOKEN,
        'v': _VK_VER,
    }, timeout=15).json()

    if 'response' in save:
        story_id = save['response']['items'][0]['id']
        if log_id:
            db_log_entry(log_id, f'История опубликована: id={story_id}')
        return story_id

    if log_id:
        db_log_entry(log_id, f"stories.save: {save.get('error', save)}", level='error')
    return None


def publish_wall(video_data: bytes, group_id: int, log_id, title: str = '') -> int | None:
    """Публикует видео на стену сообщества ВКонтакте. Возвращает post_id или None."""
    save_resp = requests.post(f'{_VK_API}/video.save', data={
        'group_id':     group_id,
        'name':         title or '',
        'description':  '',
        'wallpost':     0,
        'access_token': _VK_TOKEN,
        'v': _VK_VER,
    }, timeout=15).json()

    if 'error' in save_resp:
        if log_id:
            db_log_entry(log_id, f"video.save: {save_resp['error']}", level='error')
        return None

    upload_url = save_resp['response']['upload_url']
    video_id   = save_resp['response']['video_id']
    owner_id   = save_resp['response']['owner_id']
    filename = f"{_safe_filename(title)}.mp4" if title else 'video.mp4'

    up = requests.post(
        upload_url,
        files={'video_file': (filename, io.BytesIO(video_data), 'video/mp4')},
        timeout=300,
    )
    up.raise_for_status()

    post_resp = requests.post(f'{_VK_API}/wall.post', data={
        'owner_id':     -group_id,
        'from_group':   1,
        'attachments':  f'video{owner_id}_{video_id}',
        'access_token': _VK_TOKEN,
        'v': _VK_VER,
    }, timeout=15).json()

    if 'response' in post_resp:
        post_id = post_resp['response']['post_id']
        if log_id:
            db_log_entry(log_id, f'Пост на стене: post_id={post_id}')
        return post_id

    if log_id:
        db_log_entry(log_id, f"wall.post: {post_resp.get('error', post_resp)}", level='error')
    return None


def is_configured() -> bool:
    """Возвращает True если токен задан."""
    return bool(_VK_TOKEN)
