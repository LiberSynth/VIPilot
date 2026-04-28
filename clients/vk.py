"""
VK API-клиент.
Отвечает за публикацию видео в историю и на стену сообщества ВКонтакте.
Принимает видео в виде байт (bytes) — никаких файлов на диске.
"""

import io
import os
import time

import requests

from log import write_log_entry
from utils.utils import fmt_id_msg
from routes.api import build_publication_title, publication_file_name

_VK_TOKEN = os.environ.get('VK_USER_TOKEN', '')
_VK_API   = 'https://api.vk.com/method'
_VK_VER   = '5.131'


def publish_story(video_data: bytes, group_id: int, log_id) -> int | None:
    """Публикует видео как историю ВКонтакте. Возвращает story_id или None."""
    r = requests.post(f'{_VK_API}/stories.getVideoUploadServer', data={
        'group_id':    group_id,
        'add_to_news': 1,
        'access_token': _VK_TOKEN,
        'v': _VK_VER,
    }, timeout=15).json()

    if 'error' in r:
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
                write_log_entry(log_id, f'Пустой ответ CDN (попытка {attempt+1}/3)', level='warn')
                time.sleep(5)
                continue
            up_data = up.json()
            if 'response' not in up_data:
                write_log_entry(log_id, f'Неожиданный ответ CDN (попытка {attempt+1}/3): {up.text[:200]}', level='warn')
                time.sleep(5)
                continue
            upload_result = up_data['response']['upload_result']
            break
        except Exception as e:
            write_log_entry(log_id, f'Ошибка загрузки (попытка {attempt+1}/3): {e}', level='warn')
            time.sleep(5)
    else:
        write_log_entry(log_id, 'Все попытки загрузки истории провалились', level='error')
        return None

    save = requests.post(f'{_VK_API}/stories.save', data={
        'upload_results': upload_result,
        'access_token': _VK_TOKEN,
        'v': _VK_VER,
    }, timeout=15).json()

    if 'response' in save:
        story_id = save['response']['items'][0]['id']
        write_log_entry(log_id, fmt_id_msg('История опубликована: id={}', story_id))
        return story_id

    write_log_entry(log_id, f"stories.save: {save.get('error', save)}", level='error')
    return None


def _clip_url_to_attachment(clip_url: str) -> str:
    """Преобразует ссылку VK Видео в attachment-строку для wall.post.

    https://vkvideo.ru/clip-236929597_456239776  →  video-236929597_456239776

    VK API wall.post принимает тип «video», а не «clip».
    """
    if not clip_url:
        return ""
    for prefix in ("https://vkvideo.ru/clip", "http://vkvideo.ru/clip", "vkvideo.ru/clip"):
        if clip_url.startswith(prefix):
            return "video" + clip_url[len(prefix):]
    return ""


def publish_clip_wall(clip_url: str, title: str, group_id: int, log_id) -> int | None:
    """Публикует пост на стену сообщества со ссылкой на существующий клип VK Видео.

    Не загружает видео — только создаёт wall.post с attachment клипа.
    Возвращает post_id или None.
    """
    attachment = _clip_url_to_attachment(clip_url)
    if not attachment:
        write_log_entry(log_id, f"VK: Не удалось получить attachment из ссылки «{clip_url}»", level='error')
        return None

    write_log_entry(log_id, fmt_id_msg("VK: Публикую пост с клипом: attachment={}, title={}", attachment, title))

    post_resp = requests.post(f'{_VK_API}/wall.post', data={
        'owner_id':     -group_id,
        'from_group':   1,
        'message':      title,
        'attachments':  attachment,
        'access_token': _VK_TOKEN,
        'v': _VK_VER,
    }, timeout=15).json()

    if 'response' in post_resp:
        post_id = post_resp['response']['post_id']
        write_log_entry(log_id, fmt_id_msg('VK: Пост с клипом опубликован: post_id={}', post_id))
        return post_id

    write_log_entry(log_id, f"VK: wall.post: {post_resp.get('error', post_resp)}", level='error')
    return None


def publish_wall(video_data: bytes, group_id: int, log_id) -> int | None:
    """Публикует видео на стену сообщества ВКонтакте. Возвращает post_id или None."""
    pub_title = build_publication_title()
    save_resp = requests.post(f'{_VK_API}/video.save', data={
        'group_id':     group_id,
        'name':         pub_title,
        'description':  '',
        'wallpost':     0,
        'access_token': _VK_TOKEN,
        'v': _VK_VER,
    }, timeout=15).json()

    if 'error' in save_resp:
        write_log_entry(log_id, f"video.save: {save_resp['error']}", level='error')
        return None

    upload_url = save_resp['response']['upload_url']
    video_id   = save_resp['response']['video_id']
    owner_id   = save_resp['response']['owner_id']
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
                write_log_entry(log_id, f'Пустой ответ CDN wall (попытка {attempt+1}/3)', level='warn')
                time.sleep(5)
                continue
            up_data = up.json()
            if 'response' not in up_data:
                write_log_entry(log_id, f'Неожиданный ответ CDN wall (попытка {attempt+1}/3): {up.text[:200]}', level='warn')
                time.sleep(5)
                continue
            break
        except Exception as e:
            write_log_entry(log_id, f'Ошибка загрузки wall (попытка {attempt+1}/3): {e}', level='warn')
            time.sleep(5)
    else:
        write_log_entry(log_id, 'Все попытки загрузки видео на стену провалились', level='error')
        return None

    post_resp = requests.post(f'{_VK_API}/wall.post', data={
        'owner_id':     -group_id,
        'from_group':   1,
        'attachments':  f'video{owner_id}_{video_id}',
        'access_token': _VK_TOKEN,
        'v': _VK_VER,
    }, timeout=15).json()

    if 'response' in post_resp:
        post_id = post_resp['response']['post_id']
        write_log_entry(log_id, f'Пост на стене: post_id={post_id}')
        return post_id

    write_log_entry(log_id, f"wall.post: {post_resp.get('error', post_resp)}", level='error')
    return None
