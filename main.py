import os
import time
import atexit
import threading
import requests
import subprocess
from collections import deque
from datetime import datetime, timezone, timedelta
from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    session,
    flash,
    jsonify,
)
import hashlib
import psycopg2.extras

from db import (
    init_db,
    run_upgrades,
    db_get,
    db_set,
    db_get_schedule,
    db_add_schedule_slot,
    db_delete_schedule_slot,
    db_save_cycle,
    db_load_cycles,
    db_trim_cycles,
    db_trim_cycles_by_age,
    db_clear_old_entries,
    db_save_video_url,
    db_get_random_video_url,
    db_save_story,
    get_active_model,
    get_active_text_model,
)
from log import db_log_root, db_get_log, db_get_monitor
from db.init import get_db
from pipelines import planning, story, video, transcode, publish

FAL_KEY = os.environ["FAL_API_KEY"]
VK_TOKEN = os.environ["VK_USER_TOKEN"]
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "admin")
GROUP_ID = 236929597

FAL_QUEUE_BASE = "https://queue.fal.run/fal-ai"
FAL_HEADERS = {"Authorization": f"Key {FAL_KEY}", "Content-Type": "application/json"}

OPENROUTER_KEY = os.environ.get("OPENROUTER_API_KEY", "")
OPENROUTER_HEADERS = {
    "Authorization": f"Bearer {OPENROUTER_KEY}",
    "Content-Type": "application/json",
}

VIDEO_PATH = "/tmp/story_raw.mp4"
VIDEO_VK_PATH = "/tmp/story_vk.mp4"

MSK_OFFSET = timedelta(hours=3)



def parse_hhmm(s):
    try:
        h, m = s.strip().split(":")
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


def parse_short_log_days(s):
    try:
        return max(1, min(3650, int(s)))
    except Exception:
        return 365


def to_msk(h, m):
    total = (h * 60 + m + 180) % 1440
    return total // 60, total % 60


def to_utc_from_msk(h, m):
    total = (h * 60 + m - 180) % 1440
    return total // 60, total % 60



app_state = {
    "running": False,
    "last_published": None,
    "last_ok": False,
    "current_prompt": None,
    "current_cycle": None,  # dict with 'started', 'status', 'entries' (list)
    "cycles": deque(maxlen=20),  # completed cycles, newest first
}


def msk_ts():
    return (datetime.now(timezone.utc) + MSK_OFFSET).strftime("%d.%m.%Y %H:%M:%S МСК")


def start_cycle():
    cycle = {
        "started": msk_ts(),
        "started_ts": time.time(),
        "status": "running",
        "entries": [],
        "summary": {
            "story_model": None,
            "story_generated_at": None,
            "video_model": None,
            "video_generated_at": None,
            "transcoding_method": None,
            "transcoded_at": None,
            "published_at": None,
        },
    }
    app_state["current_cycle"] = cycle
    return cycle


def end_cycle(ok):
    cycle = app_state["current_cycle"]
    if cycle is None:
        return
    cycle["status"] = "ok" if ok else "error"
    completed = dict(cycle)
    app_state["cycles"].appendleft(completed)
    app_state["current_cycle"] = None
    db_save_cycle(completed)
    db_trim_cycles(keep=20)
    db_trim_cycles_by_age(app_state["cycles"])
    db_clear_old_entries(app_state["cycles"])


def log_msg(msg, level="info"):
    ts = (datetime.now(timezone.utc) + MSK_OFFSET).strftime("%d.%m %H:%M:%S")
    entry = {"ts": ts, "msg": msg, "level": level}
    if app_state["current_cycle"] is not None:
        app_state["current_cycle"]["entries"].append(entry)
    print(f"[{ts} МСК] {msg}")


def is_emulation():
    return db_get("emulation_mode", "0") == "1"



def build_fal_body(body_tpl, prompt):
    """Fill body template with current settings values using template format strings."""
    body = dict(body_tpl)
    if "prompt" in body:
        body["prompt"] = str(body["prompt"]).format(prompt)
    if "duration" in body:
        try:
            dur = max(1, min(60, int(db_get("video_duration", "6"))))
        except (ValueError, TypeError):
            dur = 6
        if body["duration"] == "{int}":
            body["duration"] = dur
        else:
            body["duration"] = str(body["duration"]).format(dur)
    if "aspect_ratio" in body:
        try:
            ar_x = int(db_get("aspect_ratio_x", "9"))
            ar_y = int(db_get("aspect_ratio_y", "16"))
        except (ValueError, TypeError):
            ar_x, ar_y = 9, 16
        body["aspect_ratio"] = str(body["aspect_ratio"]).format(ar_x, ar_y)
    return body


def fal_request_id_to_url(request_id, response_url=None, platform_url=None):
    """Resolve a fal.ai request_id to a video URL.
    Uses response_url if provided, otherwise falls back to a best-effort guess."""
    try:
        base = platform_url or FAL_QUEUE_BASE
        url = response_url or f"{base}/requests/{request_id}"
        r = requests.get(url, headers={"Authorization": f"Key {FAL_KEY}"}, timeout=10)
        r.raise_for_status()
        return r.json().get("video", {}).get("url")
    except Exception:
        return None


def transcode_video():
    raw_size = os.path.getsize(VIDEO_PATH)
    log_msg(
        f"Транскодирую в H.264... (исходник: {round(raw_size / 1024 / 1024, 1)} МБ)"
    )
    result = subprocess.run(
        [
            "ffmpeg",
            "-t",
            "8",
            "-i",
            VIDEO_PATH,
            "-f",
            "lavfi",
            "-i",
            "anullsrc=r=44100:cl=stereo",
            "-t",
            "8",
            "-c:v",
            "libx264",
            "-profile:v",
            "baseline",
            "-preset",
            "ultrafast",
            "-crf",
            "26",
            "-pix_fmt",
            "yuv420p",
            "-r",
            "30",
            "-c:a",
            "aac",
            "-b:a",
            "96k",
            "-movflags",
            "+faststart",
            VIDEO_VK_PATH,
            "-y",
        ],
        capture_output=True,
        timeout=600,
    )
    if result.returncode != 0:
        err = result.stderr.decode(errors="replace")[-600:]
        log_msg(f"ffmpeg ошибка (код {result.returncode}): {err}", "error")
        return False
    log_msg("Транскодирование завершено")
    if app_state["current_cycle"] is not None:
        app_state["current_cycle"]["summary"]["transcoding_method"] = "ffmpeg/libx264"
        app_state["current_cycle"]["summary"]["transcoded_at"] = msk_ts()
    return True



def generate_story():
    """Generate a story via active OpenRouter text model.
    Returns (True, story_text) on success, (False, None) on any failure.
    """
    api_url, model_id, body_tpl, model_name, model_uuid = get_active_text_model()
    if not api_url:
        log_msg(
            "Нет активной текстовой модели — генерация сюжета пропущена, "
            "будет использован случайный промпт.",
            "warn",
        )
        return False, None

    if not OPENROUTER_KEY:
        log_msg(
            "OPENROUTER_API_KEY не задан — генерация сюжета пропущена.",
            "error",
        )
        return False, None

    system_prompt = db_get("system_prompt", "")
    user_prompt = db_get("metaprompt", "")
    try:
        dur = max(1, min(60, int(db_get("video_duration", "6"))))
    except (ValueError, TypeError):
        dur = 6
    user_prompt = f"{user_prompt}\n\nПродолжительность {dur:d} секунд."
    app_state["current_prompt"] = user_prompt
    if app_state["current_cycle"] is not None:
        app_state["current_cycle"]["summary"]["prompt"] = user_prompt

    body = dict(body_tpl)
    if "messages" in body:
        messages = []
        for msg in body["messages"]:
            m = dict(msg)
            if m.get("role") == "system":
                m["content"] = str(m["content"]).format(system_prompt)
            elif m.get("role") == "user":
                m["content"] = str(m["content"]).format(user_prompt)
            messages.append(m)
        body["messages"] = messages
    body["model"] = model_id

    log_msg(
        f"[СЮЖЕТ] Запрос к OpenRouter: модель={model_name}, "
        f"промпт={user_prompt[:80]}{'...' if len(user_prompt) > 80 else ''}"
    )

    try:
        resp = requests.post(
            api_url, headers=OPENROUTER_HEADERS, json=body, timeout=60
        )
    except requests.exceptions.Timeout:
        log_msg("[СЮЖЕТ] Таймаут запроса к OpenRouter (60 сек)", "error")
        return False, None
    except requests.exceptions.RequestException as e:
        log_msg(f"[СЮЖЕТ] Ошибка соединения с OpenRouter: {e}", "error")
        return False, None

    try:
        data = resp.json()
    except ValueError:
        log_msg(
            f"[СЮЖЕТ] OpenRouter вернул не-JSON ответ "
            f"(HTTP {resp.status_code}): {resp.text[:500]}",
            "error",
        )
        return False, None

    if resp.status_code >= 400:
        err_msg = data.get("error", {})
        if isinstance(err_msg, dict):
            err_msg = err_msg.get("message", data)
        log_msg(f"[СЮЖЕТ] OpenRouter HTTP {resp.status_code}: {err_msg}", "error")
        return False, None

    choices = data.get("choices")
    if not choices:
        log_msg(f"[СЮЖЕТ] OpenRouter: нет поля choices в ответе: {data}", "error")
        return False, None

    story = (choices[0].get("message") or {}).get("content", "").strip()
    if not story:
        log_msg(f"[СЮЖЕТ] OpenRouter вернул пустой текст: {data}", "error")
        return False, None

    log_msg(
        f"[СЮЖЕТ] Получен: {story[:120]}{'...' if len(story) > 120 else ''}"
    )
    if app_state["current_cycle"] is not None:
        app_state["current_cycle"]["summary"]["story"] = story
        app_state["current_cycle"]["summary"]["story_model"] = model_name
        app_state["current_cycle"]["summary"]["story_generated_at"] = msk_ts()

    db_save_story(model_uuid, story)
    return True, story


def generate_video(prompt):
    app_state["running"] = True

    if is_emulation():
        try:
            log_msg("[ЭМУЛЯЦИЯ] Пропускаю генерацию, беру случайное видео из базы...")
            url = db_get_random_video_url()
            if not url:
                log_msg(
                    "[ЭМУЛЯЦИЯ] В базе нет видео — добавьте ID запросов fal.ai через панель",
                    "error",
                )
                return False
            log_msg("[ЭМУЛЯЦИЯ] Видео выбрано, скачиваю...")
            return download_and_transcode(url)
        finally:
            app_state["running"] = False

    try:
        submit_url, body_tpl, platform_url, video_model_name = get_active_model()
        if not submit_url:
            log_msg(
                'Нет активной модели в базе. Выберите модель на вкладке "Запрос".',
                "error",
            )
            return False

        if app_state["current_cycle"] is not None:
            app_state["current_cycle"]["summary"]["video_model"] = video_model_name

        body = build_fal_body(body_tpl, prompt)
        log_msg(f"Отправляю запрос: {submit_url}  тело: {body}")

        resp = requests.post(submit_url, headers=FAL_HEADERS, json=body, timeout=30)
        try:
            data = resp.json()
        except ValueError:
            log_msg(
                f"fal.ai вернул не-JSON ответ (HTTP {resp.status_code}): {resp.text[:500]}",
                "error",
            )
            return False

        if resp.status_code >= 400:
            log_msg(f"fal.ai HTTP {resp.status_code}: {data}", "error")
            return False

        if "request_id" not in data:
            log_msg(f"Ошибка запроса к fal.ai: {data}", "error")
            return False

        request_id = data["request_id"]
        status_url = data.get("status_url")
        response_url = data.get("response_url")
        log_msg(f"Генерация запущена. ID: {request_id}")

        for attempt in range(240):
            time.sleep(30)
            try:
                s = requests.get(
                    status_url, headers={"Authorization": f"Key {FAL_KEY}"}, timeout=10
                ).json()
                status = s.get("status")
                log_msg(f"Статус [{attempt + 1}]: {status}")

                if status == "COMPLETED":
                    try:
                        result_url = (
                            response_url or f"{platform_url}/requests/{request_id}"
                        )
                        result = requests.get(
                            result_url,
                            headers={"Authorization": f"Key {FAL_KEY}"},
                            timeout=10,
                        ).json()
                        # Проверка на нарушение политики контента
                        detail = result.get("detail")
                        if detail:
                            types = [d.get("type", "") for d in detail] if isinstance(detail, list) else []
                            if any("content_policy" in t for t in types):
                                log_msg(
                                    "fal.ai отклонил промпт: нарушение политики контента. "
                                    "Будет сгенерирован новый сюжет.",
                                    "warn",
                                )
                                return "content_policy"
                        video_url = result.get("video", {}).get("url")
                        if not video_url:
                            log_msg(f"Нет URL видео в ответе: {result}", "error")
                            return False
                        log_msg(f"URL получен, сохраняю в базу: {video_url[:60]}...")
                        db_save_video_url(video_url)
                        if app_state["current_cycle"] is not None:
                            app_state["current_cycle"]["summary"]["video_generated_at"] = msk_ts()
                        return download_and_transcode(video_url)
                    except Exception as e:
                        log_msg(f"Ошибка обработки готового видео: {e}", "error")
                        return False

                elif status == "FAILED":
                    log_msg(f"Генерация провалилась: {s}", "error")
                    return False
            except Exception as e:
                log_msg(f"Ошибка опроса статуса: {e}", "error")

        log_msg("Таймаут генерации (2 часа)", "error")
        return False
    finally:
        app_state["running"] = False


def download_and_transcode(video_url):
    log_msg("Скачиваю видео...")
    ok = False
    for attempt in range(3):
        try:
            r = requests.get(
                video_url,
                stream=True,
                timeout=120,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            if r.status_code == 200:
                with open(VIDEO_PATH, "wb") as f:
                    for chunk in r.iter_content(chunk_size=65536):
                        f.write(chunk)
                size = os.path.getsize(VIDEO_PATH)
                if size > 10000:
                    log_msg(f"Видео скачано: {round(size / 1024 / 1024, 1)} МБ")
                    ok = True
                    break
            else:
                log_msg(
                    f"HTTP {r.status_code} при скачивании, попытка {attempt + 1}/3",
                    "error",
                )
        except Exception as e:
            log_msg(f"Ошибка скачивания (попытка {attempt + 1}/3): {e}", "error")
        time.sleep(10)
    if not ok:
        return False

    result = transcode_video()
    return result


def publish_story():
    log_msg("Публикую историю в VK...")
    try:
        r = requests.post(
            "https://api.vk.com/method/stories.getVideoUploadServer",
            data={
                "group_id": GROUP_ID,
                "add_to_news": 1,
                "access_token": VK_TOKEN,
                "v": "5.131",
            },
            timeout=15,
        )
        r.raise_for_status()
        server_data = r.json()
        if "error" in server_data:
            log_msg(f"Ошибка getVideoUploadServer: {server_data['error']}", "error")
            return False
        upload_url = server_data["response"]["upload_url"]
        log_msg("Upload URL получен, загружаю...")

        for attempt in range(3):
            try:
                with open(VIDEO_VK_PATH, "rb") as f:
                    up = requests.post(
                        upload_url,
                        files={"video_file": ("video.mp4", f, "video/mp4")},
                        timeout=300,
                    )
                up.raise_for_status()
                if not up.text.strip():
                    log_msg(f"Пустой ответ от CDN, попытка {attempt + 1}/3", "error")
                    time.sleep(5)
                    continue
                up_data = up.json()
                if "response" not in up_data:
                    log_msg(f"Неожиданный ответ CDN: {up.text[:200]}", "error")
                    return False
                upload_result = up_data["response"]["upload_result"]
                break
            except Exception as e:
                log_msg(
                    f"Ошибка загрузки видео (попытка {attempt + 1}/3): {e}", "error"
                )
                time.sleep(5)
        else:
            log_msg("Все попытки загрузки провалились", "error")
            return False

        log_msg("Видео загружено, сохраняю историю...")
        save = requests.post(
            "https://api.vk.com/method/stories.save",
            data={
                "upload_results": upload_result,
                "access_token": VK_TOKEN,
                "v": "5.131",
            },
            timeout=15,
        ).json()

        if "response" in save:
            story_id = save["response"]["items"][0]["id"]
            ts = msk_ts()
            log_msg(f"✓ История опубликована. ID: {story_id}", "ok")
            app_state["last_published"] = ts
            app_state["last_ok"] = True
            if app_state["current_cycle"] is not None:
                app_state["current_cycle"]["summary"]["published_at"] = ts
            return True
        else:
            log_msg(f"Ошибка stories.save: {save}", "error")
            return False
    except Exception as e:
        log_msg(f"Исключение при публикации истории: {e}", "error")
        return False


def publish_to_wall():
    log_msg("Публикую видео на стену сообщества...")
    try:
        save_resp = requests.post(
            "https://api.vk.com/method/video.save",
            data={
                "group_id": GROUP_ID,
                "name": "",
                "description": "",
                "wallpost": 0,
                "access_token": VK_TOKEN,
                "v": "5.131",
            },
            timeout=15,
        ).json()

        if "error" in save_resp:
            log_msg(f"Ошибка video.save: {save_resp['error']}", "error")
            return False

        upload_url = save_resp["response"]["upload_url"]
        video_id = save_resp["response"]["video_id"]
        owner_id = save_resp["response"]["owner_id"]
        log_msg("video.save OK, загружаю файл...")

        with open(VIDEO_VK_PATH, "rb") as f:
            up = requests.post(upload_url, files={"video_file": f}, timeout=300)
        up.raise_for_status()
        log_msg("Видео загружено. Публикую пост...")

        post_resp = requests.post(
            "https://api.vk.com/method/wall.post",
            data={
                "owner_id": -GROUP_ID,
                "from_group": 1,
                "attachments": f"video{owner_id}_{video_id}",
                "access_token": VK_TOKEN,
                "v": "5.131",
            },
            timeout=15,
        ).json()

        if "response" in post_resp:
            post_id = post_resp["response"]["post_id"]
            log_msg(f"✓ Видео опубликовано на стене. post_id: {post_id}", "ok")
            return True
        else:
            log_msg(f"Ошибка wall.post: {post_resp}", "error")
            return False
    except Exception as e:
        log_msg(f"Исключение при публикации на стену: {e}", "error")
        return False


def send_failure_email(message, log_entries=None, partial=False):
    import smtplib
    from email.mime.text import MIMEText

    to_addr = db_get("notify_email", "").strip()
    smtp_host = os.environ.get("SMTP_HOST", "").strip()
    smtp_user = os.environ.get("SMTP_USER", "").strip()
    smtp_pass = os.environ.get("SMTP_PASSWORD", "").strip()
    if not all([to_addr, smtp_host, smtp_user, smtp_pass]):
        log_msg(
            "[УВЕДОМЛЕНИЕ] Email не отправлен: не заданы SMTP-настройки или адрес",
            "warn",
        )
        return
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_from = os.environ.get("SMTP_FROM", smtp_user)
    subject_prefix = "Частично" if partial else "Сбой"
    try:
        body = message
        if log_entries:
            lines = "\n".join(f"[{e['ts']}] {e['msg']}" for e in log_entries)
            body += f"\n\n--- Подробный лог ---\n{lines}"
        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = f"Red Brick Core: {subject_prefix.lower()} в пайплайне"
        msg["From"] = smtp_from
        msg["To"] = to_addr
        with smtplib.SMTP(smtp_host, smtp_port, timeout=15) as s:
            s.starttls()
            s.login(smtp_user, smtp_pass)
            s.send_message(msg)
        log_msg(f"[УВЕДОМЛЕНИЕ] Email отправлен на {to_addr}")
    except Exception as e:
        log_msg(f"[УВЕДОМЛЕНИЕ] Ошибка отправки email: {e}", "error")


def send_failure_sms(message):
    phone = db_get("notify_phone", "").strip()
    smsc_login = os.environ.get("SMSC_LOGIN", "").strip()
    smsc_pass = os.environ.get("SMSC_PASS", "").strip()
    if not all([phone, smsc_login, smsc_pass]):
        return
    try:
        r = requests.get(
            "https://smsc.ru/sys/send.php",
            params={
                "login": smsc_login,
                "psw": smsc_pass,
                "phones": phone,
                "mes": message[:160],
                "charset": "utf-8",
                "fmt": 3,
            },
            timeout=10,
        )
        data = r.json()
        if data.get("error_code"):
            log_msg(f"[УВЕДОМЛЕНИЕ] SMSC ошибка: {data}", "error")
        else:
            log_msg(f"[УВЕДОМЛЕНИЕ] SMS отправлено на {phone}")
    except Exception as e:
        log_msg(f"[УВЕДОМЛЕНИЕ] Ошибка отправки SMS: {e}", "error")


def notify_failure(reason, log_entries=None, partial=False):
    prefix = "Частично" if partial else "Сбой"
    msg = f"{prefix} {msk_ts()}: {reason}"
    log_msg(f"[УВЕДОМЛЕНИЕ] Отправляю уведомление [{prefix}]: {reason}")
    send_failure_email(msg, log_entries=log_entries or [], partial=partial)
    send_failure_sms(msg)


def run_full_cycle():
    start_cycle()
    try:
        story_ok, story_text = generate_story()
        gen_ok = False
        content_policy_exhausted = False
        _MAX_CP_RETRIES = 2
        for _cp_attempt in range(_MAX_CP_RETRIES + 1):
            if not story_ok:
                break
            _result = generate_video(prompt=story_text)
            if _result == "content_policy":
                if _cp_attempt < _MAX_CP_RETRIES:
                    log_msg(
                        f"[СЮЖЕТ] Перегенерация сюжета "
                        f"(попытка {_cp_attempt + 2} из {_MAX_CP_RETRIES + 1})...",
                        "warn",
                    )
                    story_ok, story_text = generate_story()
                    continue
                else:
                    content_policy_exhausted = True
                    break
            else:
                gen_ok = bool(_result)
                break
        pub_ok = False
        do_story = do_wall = story_ok_vk = wall_ok = False
        if gen_ok:
            do_story = db_get("vk_publish_story", "1") == "1"
            do_wall = db_get("vk_publish_wall", "1") == "1"
            story_ok_vk = publish_story() if do_story else False
            wall_ok = publish_to_wall() if do_wall else False
            pub_ok = story_ok_vk or wall_ok
        story_partial_fail = gen_ok and do_story and not story_ok_vk and wall_ok
        success = pub_ok if gen_ok else False
        entries = (
            list(app_state["current_cycle"]["entries"])
            if app_state["current_cycle"]
            else []
        )
        if not success:
            if not story_ok:
                reason = "ошибка генерации сюжета"
            elif content_policy_exhausted:
                reason = f"fal.ai отклоняет промпт (нарушение политики контента, {_MAX_CP_RETRIES + 1} попытки исчерпаны)"
            elif not gen_ok:
                reason = "ошибка генерации видео"
            else:
                reason = "ошибка публикации в VK"
            if app_state["current_cycle"] is not None:
                app_state["current_cycle"]["summary"]["failed"] = reason
        end_cycle(success)
        if not success:
            notify_failure(reason, log_entries=entries)
        elif story_partial_fail:
            notify_failure(
                "ошибка публикации истории в VK (стена опубликована успешно)",
                log_entries=entries,
                partial=True,
            )
        return gen_ok, pub_ok
    except Exception as _exc:
        cycle = app_state.get("current_cycle")
        if cycle and cycle.get("status") == "running":
            cycle["summary"]["failed"] = str(_exc) or "неизвестная ошибка"
            end_cycle(False)
        raise


flask_app = Flask(__name__, static_folder=".")
flask_app.secret_key = os.environ.get("FLASK_SECRET", os.urandom(24).hex())


@flask_app.before_request
def log_request():
    method = request.method
    path = request.full_path if request.query_string else request.path
    remote = request.remote_addr
    headers = dict(request.headers)
    body = ""
    if request.content_length and request.content_length > 0:
        try:
            body = request.get_data(as_text=True)
        except Exception:
            body = "<не удалось прочитать тело>"
    msg = (
        f"[HTTP] {method} {path} | IP: {remote} | "
        f"Headers: {headers}"
    )
    if body:
        msg += f" | Body: {body}"
    print(msg)


@flask_app.route("/favicon.ico")
def favicon():
    from flask import send_file

    return send_file("generated-icon.png", mimetype="image/png")


def password_fingerprint():
    return hashlib.sha256(ADMIN_PASSWORD.encode()).hexdigest()[:16]


def is_authenticated():
    if not (session.get("auth") is True and session.get("pw_fp") == password_fingerprint()):
        return False
    # Сессия живёт не более 8 часов
    auth_ts = session.get("auth_ts", 0)
    return (time.time() - auth_ts) < 28800


@flask_app.route("/", methods=["GET", "POST"])
def login():
    if is_authenticated():
        return redirect(url_for("admin"))

    error = False
    if request.method == "POST":
        if request.form.get("password") == ADMIN_PASSWORD:
            session["auth"] = True
            session["pw_fp"] = password_fingerprint()
            session["auth_ts"] = time.time()
            session.permanent = False
            return redirect(url_for("admin"))
        error = True
    return render_template("login.html", error=error)


@flask_app.route("/admin")
def admin():
    if not is_authenticated():
        return redirect(url_for("login"))
    metaprompt = db_get("metaprompt", "")
    system_prompt = db_get("system_prompt", "")
    lead_mins = parse_lead_mins(db_get("lead_time_mins", "120"))

    history_days = parse_history_days(db_get("history_days", "7"))
    short_log_days = parse_short_log_days(db_get("short_log_days", "365"))
    emulation_mode = db_get("emulation_mode", "0") == "1"
    notify_email = db_get("notify_email", "")
    notify_phone = db_get("notify_phone", "")
    vk_publish_story = db_get("vk_publish_story", "1") == "1"
    vk_publish_wall = db_get("vk_publish_wall", "1") == "1"
    aspect_ratio_x = int(db_get("aspect_ratio_x", "9"))
    aspect_ratio_y = int(db_get("aspect_ratio_y", "16"))
    video_duration = max(1, min(60, int(db_get("video_duration", "6"))))

    return render_template(
        "admin.html",
        metaprompt=metaprompt,
        system_prompt=system_prompt,
        lead_time_mins=lead_mins,
        history_days=history_days,
        short_log_days=short_log_days,
        emulation_mode=emulation_mode,
        notify_email=notify_email,
        notify_phone=notify_phone,
        vk_publish_story=vk_publish_story,
        vk_publish_wall=vk_publish_wall,
        aspect_ratio_x=aspect_ratio_x,
        aspect_ratio_y=aspect_ratio_y,
        video_duration=video_duration,
        status=app_state,
    )


@flask_app.route("/save", methods=["POST"])
def save():
    if not is_authenticated():
        return redirect(url_for("login"))
    system_prompt_val = request.form.get("system_prompt")
    if system_prompt_val is not None:
        db_set("system_prompt", system_prompt_val)

    metaprompt = request.form.get("metaprompt", "").strip()
    active_tab = request.form.get("active_tab", "pipeline")
    if not metaprompt:
        if active_tab == "story":
            log_msg("[SAVE] Попытка сохранить пустой мета-промпт — отклонено", "error")
            flash("Мета-промпт не может быть пустым", "error")
            return redirect(url_for("admin"))
        # для остальных вкладок просто не перезаписываем метапромпт
    else:
        db_set("metaprompt", metaprompt)

    lead_raw = request.form.get("lead_time_mins", "").strip()
    if lead_raw:
        db_set("lead_time_mins", str(parse_lead_mins(lead_raw)))

    history_raw = request.form.get("history_days", "").strip()
    if history_raw:
        db_set("history_days", str(parse_history_days(history_raw)))

    short_log_raw = request.form.get("short_log_days", "").strip()
    if short_log_raw:
        db_set("short_log_days", str(parse_short_log_days(short_log_raw)))

    emulation_raw = request.form.get("emulation_mode", "0")
    db_set("emulation_mode", "1" if emulation_raw == "1" else "0")

    db_set("notify_email", request.form.get("notify_email", "").strip())
    db_set("notify_phone", request.form.get("notify_phone", "").strip())

    vk_story_raw = request.form.get("vk_publish_story", "0")
    vk_wall_raw = request.form.get("vk_publish_wall", "0")
    # хотя бы одно должно быть включено
    if vk_story_raw != "1" and vk_wall_raw != "1":
        vk_story_raw = "1"
    db_set("vk_publish_story", "1" if vk_story_raw == "1" else "0")
    db_set("vk_publish_wall", "1" if vk_wall_raw == "1" else "0")

    try:
        ar_x = max(1, min(99, int(request.form.get("aspect_ratio_x", "9"))))
    except (ValueError, TypeError):
        ar_x = 9
    try:
        ar_y = max(1, min(99, int(request.form.get("aspect_ratio_y", "16"))))
    except (ValueError, TypeError):
        ar_y = 16
    db_set("aspect_ratio_x", str(ar_x))
    db_set("aspect_ratio_y", str(ar_y))

    vid_dur_str = request.form.get("video_duration")
    if vid_dur_str is not None:
        try:
            vid_dur = max(1, min(60, int(vid_dur_str)))
        except (ValueError, TypeError):
            vid_dur = 6
        db_set("video_duration", str(vid_dur))

    return redirect(url_for("admin") + f"?tab={active_tab}")


@flask_app.route("/log-data")
def log_data():
    if not is_authenticated():
        return jsonify({})

    def serialize_cycle(c):
        return {
            "started": c["started"],
            "started_ts": c.get("started_ts", 0),
            "status": c["status"],
            "summary": c.get("summary", {}),
            "entries": c["entries"],
        }

    cycles = [serialize_cycle(c) for c in app_state["cycles"]]
    current = app_state["current_cycle"]
    if current:
        cycles = [serialize_cycle(current)] + cycles

    return jsonify(
        {
            "running": app_state["running"],
            "current_prompt": app_state["current_prompt"],
            "cycles": cycles,
        }
    )


@flask_app.route("/api/log")
def api_log():
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(db_get_log())


@flask_app.route("/api/monitor")
def api_monitor():
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(db_get_monitor())


@flask_app.route("/run-now", methods=["POST"])
def run_now():
    if not is_authenticated():
        return redirect(url_for("login"))
    if app_state["running"]:
        flash("Генерация уже запущена", "error")
        return redirect(url_for("admin"))

    def run():
        try:
            run_full_cycle()
        except Exception as e:
            entries = (
                list(app_state["current_cycle"]["entries"])
                if app_state["current_cycle"]
                else []
            )
            log_msg(f"Критическая ошибка цикла: {e}", "error")
            notify_failure(f"необработанное исключение: {e}", log_entries=entries)

    t = threading.Thread(target=run, daemon=True)
    t.start()
    flash("Цикл запущен — смотрите логи", "success")
    return redirect(url_for("admin"))


@flask_app.route("/test-notify", methods=["POST"])
def test_notify():
    if not is_authenticated():
        return redirect(url_for("login"))
    notify_failure("тестовый сбой (проверка уведомлений)")
    flash("Тестовое уведомление отправлено", "success")
    return redirect(url_for("admin"))


@flask_app.route("/api/schedule", methods=["GET"])
def api_get_schedule():
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    times = db_get_schedule()
    result = []
    for t in times:
        h, m = parse_hhmm(t["time_utc"])
        mh, mm = to_msk(h, m)
        result.append({"id": t["id"], "time_msk": f"{mh:02d}:{mm:02d}"})
    return jsonify(result)


@flask_app.route("/api/schedule", methods=["POST"])
def api_add_schedule_slot():
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json()
    time_msk = (data or {}).get("time", "").strip()
    if not time_msk:
        return jsonify({"error": "time required"}), 400
    h_msk, m_msk = parse_hhmm(time_msk)
    h_utc, m_utc = to_utc_from_msk(h_msk, m_msk)
    time_utc = f"{h_utc:02d}:{m_utc:02d}"
    new_id = db_add_schedule_slot(time_utc)
    if new_id is None:
        return jsonify({"error": "db error"}), 500
    return jsonify({"id": new_id, "time_msk": f"{h_msk:02d}:{m_msk:02d}"})


@flask_app.route("/api/schedule/<slot_id>", methods=["DELETE"])
def api_delete_schedule_slot(slot_id):
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    ok = db_delete_schedule_slot(slot_id)
    return jsonify({"ok": ok})


@flask_app.route("/api/models")
def api_models():
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT vm.id, vm.name, vm.url, vm.body, vm."order", vm.active,
                           p.name AS platform_name
                    FROM models vm
                    LEFT JOIN ai_platforms p ON p.id = vm.ai_platform_id
                    WHERE vm.type = 0
                    ORDER BY vm."order" ASC
                """)
                rows = cur.fetchall()
        return jsonify([dict(r) for r in rows])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@flask_app.route("/api/models/<string:model_id>/activate", methods=["POST"])
def api_model_activate(model_id):
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE models SET active = FALSE WHERE type = 0")
                cur.execute(
                    "UPDATE models SET active = TRUE WHERE id = %s", (model_id,)
                )
            conn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@flask_app.route("/api/models/reorder", methods=["POST"])
def api_models_reorder():
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(silent=True) or {}
    ids = data.get("ids", [])
    if not ids or not isinstance(ids, list):
        return jsonify({"error": "ids required"}), 400
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                for idx, model_id in enumerate(ids, start=1):
                    cur.execute(
                        'UPDATE models SET "order" = %s WHERE id = %s', (idx, model_id)
                    )
            conn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@flask_app.route("/api/text-models", methods=["GET"])
def api_text_models():
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT m.id, m.name, m.url, m.body, m."order", m.active,
                           p.name AS platform_name
                    FROM models m
                    LEFT JOIN ai_platforms p ON p.id = m.ai_platform_id
                    WHERE m.type = 1
                    ORDER BY m."order" ASC
                """)
                rows = cur.fetchall()
        return jsonify([dict(r) for r in rows])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@flask_app.route("/api/text-models/<model_id>/activate", methods=["POST"])
def api_text_model_activate(model_id):
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE models SET active = FALSE WHERE type = 1")
                cur.execute(
                    "UPDATE models SET active = TRUE WHERE id = %s", (model_id,)
                )
            conn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@flask_app.route("/api/text-models/reorder", methods=["POST"])
def api_text_models_reorder():
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(silent=True) or {}
    ids = data.get("ids", [])
    if not ids or not isinstance(ids, list):
        return jsonify({"error": "ids required"}), 400
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                for idx, model_id in enumerate(ids, start=1):
                    cur.execute(
                        'UPDATE models SET "order" = %s WHERE id = %s', (idx, model_id)
                    )
            conn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@flask_app.route("/healthz")
def healthz():
    return "ok", 200


@flask_app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


def main_loop():
    _threads = {
        'planning':   None,
        'story':      None,   # Pipeline 2 — генерация сюжетов
        'video':      None,   # Pipeline 3 — генерация видео
        'transcode':  None,   # Pipeline 4 — транскодирование
        'publish':    None,   # Pipeline 5 — публикация
        # 'cleanup':    None,  # Pipeline 6 — сборщик мусора
    }

    while True:
        try:
            interval = int(db_get('loop_interval', '5'))

            # Pipeline 1: Планирование
            if _threads['planning'] is None or not _threads['planning'].is_alive():
                _threads['planning'] = threading.Thread(
                    target=planning.run, daemon=True
                )
                _threads['planning'].start()

            # Pipeline 2: Генерация сюжетов
            if _threads['story'] is None or not _threads['story'].is_alive():
                _threads['story'] = threading.Thread(
                    target=story.run, daemon=True
                )
                _threads['story'].start()

            # Pipeline 3: Генерация видео
            if _threads['video'] is None or not _threads['video'].is_alive():
                _threads['video'] = threading.Thread(target=video.run, daemon=True)
                _threads['video'].start()

            # Pipeline 4: Транскодирование
            if _threads['transcode'] is None or not _threads['transcode'].is_alive():
                _threads['transcode'] = threading.Thread(target=transcode.run, daemon=True)
                _threads['transcode'].start()

            # Pipeline 5: Публикация
            if _threads['publish'] is None or not _threads['publish'].is_alive():
                _threads['publish'] = threading.Thread(target=publish.run, daemon=True)
                _threads['publish'].start()

            # Pipeline 6: Сборщик мусора (заготовка)
            # Очистка устаревших записей log, log_entries и буферных данных batches/stories.
            # if _threads['cleanup'] is None or not _threads['cleanup'].is_alive():
            #     _threads['cleanup'] = threading.Thread(target=cleanup.run, daemon=True)
            #     _threads['cleanup'].start()

        except Exception as e:
            db_log_root(f"Ошибка главного цикла: {e}", status='error')
            print(f"[main_loop] Ошибка: {e}")

        time.sleep(interval)


_main_loop_started = False


def _on_exit():
    db_log_root("Приложение остановлено", status='info')
    print("[main] Приложение остановлено")


def start_main_loop():
    global _main_loop_started
    if not _main_loop_started:
        _main_loop_started = True
        init_db()
        run_upgrades()
        saved = db_load_cycles()
        for c in saved:
            app_state["cycles"].append(c)
        if saved:
            last = saved[0]
            if last.get("summary", {}).get("published_at"):
                app_state["last_published"] = last["summary"]["published_at"]
                app_state["last_ok"] = last["status"] == "ok"
        print(f"[DB] Загружено циклов из БД: {len(saved)}")
        db_log_root("Приложение запущено", status='info')
        atexit.register(_on_exit)
        t = threading.Thread(target=main_loop, daemon=True)
        t.start()


start_main_loop()

app = flask_app

if __name__ == "__main__":
    flask_app.run(host="0.0.0.0", port=5000, debug=False)
