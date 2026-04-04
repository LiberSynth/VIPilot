import os
import threading
import time
import uuid
import requests as _requests
from flask import Blueprint, jsonify, request, Response

# Хранилище фоновых probe-задач: job_id → {"events": [...], "done": bool, "ts": float}
_probe_jobs = {}
_probe_lock = threading.Lock()

def _probe_append(job_id, text, level="info"):
    with _probe_lock:
        if job_id in _probe_jobs:
            _probe_jobs[job_id]["events"].append({"text": text, "level": level})

def _probe_finish(job_id, result=None):
    with _probe_lock:
        if job_id in _probe_jobs:
            _probe_jobs[job_id]["done"] = True
            _probe_jobs[job_id]["result"] = result


from log import db_log_root

from db import (
    db_get_schedule,
    db_add_schedule_slot,
    db_delete_schedule_slot,
    db_get_models,
    db_activate_model,
    db_toggle_model,
    db_reorder_models,
    init_db,
    run_upgrades,
    db_clear_all_history,
    env_get,
    env_set,
    db_create_adhoc_batch,
    db_get_active_targets,
    db_reset_batch_pipeline,
    db_get_story_text,
    db_get_batch_video_data,
    db_get_text_model_by_id,
    db_get_video_model_by_id,
    db_get_active_text_models,
    db_create_story,
    db_create_probe_batch,
    db_get,
)
from log import db_get_log, db_get_monitor, db_log_pipeline, db_log_entry
from utils.auth import is_authenticated
from utils.utils import parse_hhmm, to_msk, to_utc_from_msk
import utils.workflow_state as wf_state

bp = Blueprint("api", __name__, url_prefix="/api")


@bp.route("/time")
def api_time():
    return jsonify({"utc_ms": int(time.time() * 1000)})


@bp.route("/run-now", methods=["POST"])
def api_run_now():
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    targets = db_get_active_targets()
    if not targets:
        return jsonify({"error": "Нет активных таргетов"}), 400
    target = targets[0]
    target_id = str(target['id'])
    batch_id = db_create_adhoc_batch(target_id)
    if not batch_id:
        return jsonify({"error": "Не удалось создать батч"}), 500
    log_id = db_log_pipeline(
        'planning',
        'Оперативный запуск',
        status='ok',
        batch_id=batch_id,
    )
    if log_id:
        db_log_entry(log_id, "Запуск по запросу пользователя (внеплановый)")
        db_log_entry(log_id, f"Таргет: {target['name']}  ({target['aspect_ratio_x']}:{target['aspect_ratio_y']})")
    return jsonify({"ok": True, "batch_id": batch_id})


@bp.route("/log")
def api_log():
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(db_get_log())


@bp.route("/monitor")
def api_monitor():
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(db_get_monitor())


@bp.route("/schedule", methods=["GET"])
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


@bp.route("/schedule", methods=["POST"])
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


@bp.route("/schedule/<slot_id>", methods=["DELETE"])
def api_delete_schedule_slot(slot_id):
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    ok = db_delete_schedule_slot(slot_id)
    return jsonify({"ok": ok})


@bp.route("/models")
def api_models():
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(db_get_models("text-to-video"))


@bp.route("/models/<string:model_id>/activate", methods=["POST"])
def api_model_activate(model_id):
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    ok = db_toggle_model(model_id)
    return jsonify({"ok": ok})


@bp.route("/models/reorder", methods=["POST"])
def api_models_reorder():
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(silent=True) or {}
    ids = data.get("ids", [])
    if not ids or not isinstance(ids, list):
        return jsonify({"error": "ids required"}), 400
    ok = db_reorder_models(ids)
    return jsonify({"ok": ok})


@bp.route("/text-models", methods=["GET"])
def api_text_models():
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(db_get_models("text"))


@bp.route("/text-models/<model_id>/activate", methods=["POST"])
def api_text_model_activate(model_id):
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    ok = db_toggle_model(model_id)
    return jsonify({"ok": ok})


@bp.route("/text-models/<model_id>/grade", methods=["POST"])
def api_text_model_grade(model_id):
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(silent=True) or {}
    grade = data.get("grade", "good")
    if grade not in ("good", "limited", "poor", "fallback", "rejected"):
        return jsonify({"error": "invalid grade"}), 400
    from db import db_set_model_grade
    ok = db_set_model_grade(model_id, grade)
    return jsonify({"ok": ok})


@bp.route("/text-models/reorder", methods=["POST"])
def api_text_models_reorder():
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(silent=True) or {}
    ids = data.get("ids", [])
    if not ids or not isinstance(ids, list):
        return jsonify({"error": "ids required"}), 400
    ok = db_reorder_models(ids)
    return jsonify({"ok": ok})


@bp.route("/text-models/<model_id>/probe", methods=["POST"])
def api_text_model_probe(model_id):
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401

    m = db_get_text_model_by_id(model_id)
    if not m:
        return jsonify({"error": "Модель не найдена"}), 404

    api_key = os.environ.get("OPENROUTER_API_KEY", "")
    if not api_key:
        return jsonify({"error": "OPENROUTER_API_KEY не задан"}), 500

    try:
        fails_to_next = max(1, int(db_get("story_fails_to_next", "3")))
    except (ValueError, TypeError):
        fails_to_next = 3

    system_prompt = db_get("system_prompt", "")
    user_prompt   = db_get("metaprompt", "")
    model_name    = m["name"]

    body_tpl = m["body_tpl"]
    body = dict(body_tpl)
    if "messages" in body:
        messages = []
        for msg in body["messages"]:
            msg = dict(msg)
            if msg.get("role") == "system":
                msg["content"] = str(msg["content"]).format(system_prompt)
            elif msg.get("role") == "user":
                msg["content"] = str(msg["content"]).format(user_prompt)
            messages.append(msg)
        body["messages"] = messages
    body["model"] = m["model_url"]

    req_headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    platform_url = m["platform_url"]

    job_id = str(uuid.uuid4())
    with _probe_lock:
        _probe_jobs[job_id] = {"events": [], "done": False, "result": None, "ts": time.time()}

    def _run():
        import json as _json
        _probe_append(job_id, f"Модель: {model_name}", "info")
        _probe_append(job_id, f"URL: {platform_url}", "info")
        _probe_append(job_id, f"Попыток: {fails_to_next}", "info")

        # Показываем сам запрос
        req_preview = _json.dumps(body, ensure_ascii=False, indent=2)
        _probe_append(job_id, f"→ Запрос:\n{req_preview}", "info")

        result = None
        for attempt in range(fails_to_next):
            _probe_append(job_id, f"[попытка {attempt + 1}/{fails_to_next}] отправляю…", "info")
            try:
                resp = _requests.post(platform_url, headers=req_headers, json=body, timeout=60)
            except _requests.exceptions.Timeout:
                _probe_append(job_id, f"[попытка {attempt + 1}/{fails_to_next}] таймаут (60 с)", "warn")
                continue
            except _requests.exceptions.RequestException as e:
                _probe_append(job_id, f"[попытка {attempt + 1}/{fails_to_next}] ошибка соединения: {e}", "warn")
                continue

            # Показываем сырой ответ
            raw_preview = " ".join(resp.text.split())[:500]
            _probe_append(job_id, f"← HTTP {resp.status_code}: {raw_preview}", "info")

            try:
                data = resp.json()
            except ValueError:
                _probe_append(job_id, f"[попытка {attempt + 1}/{fails_to_next}] не-JSON ответ", "warn")
                continue
            if resp.status_code >= 400:
                err = data.get("error", {})
                if isinstance(err, dict):
                    err = err.get("message", str(data))
                _probe_append(job_id, f"[попытка {attempt + 1}/{fails_to_next}] ошибка: {err}", "warn")
                continue
            choices = data.get("choices")
            if not choices:
                _probe_append(job_id, f"[попытка {attempt + 1}/{fails_to_next}] нет поля choices в ответе", "warn")
                continue
            text = ((choices[0].get("message") or {}).get("content") or "").strip()
            if not text:
                _probe_append(job_id, f"[попытка {attempt + 1}/{fails_to_next}] пустой текст", "warn")
                continue
            result = text
            _probe_append(job_id, f"[попытка {attempt + 1}/{fails_to_next}] успех", "ok")
            break
        if result is None:
            _probe_append(job_id, f"все попытки исчерпаны", "error")
        _probe_finish(job_id, result)

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"job_id": job_id})


@bp.route("/text-models/probe/<job_id>", methods=["GET"])
def api_text_model_probe_poll(job_id):
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    with _probe_lock:
        job = _probe_jobs.get(job_id)
        if not job:
            return jsonify({"error": "not found"}), 404
        cursor = request.args.get("cursor", 0, type=int)
        new_events = job["events"][cursor:]
        done = job["done"]
        result = job["result"] if done else None
        if done and (time.time() - job["ts"]) > 120:
            del _probe_jobs[job_id]
    return jsonify({"events": new_events, "cursor": cursor + len(new_events), "done": done, "result": result})


@bp.route("/video-models/<model_id>/grade", methods=["POST"])
def api_video_model_grade(model_id):
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(silent=True) or {}
    grade = data.get("grade", "good")
    if grade not in ("good", "limited", "poor", "fallback", "rejected"):
        return jsonify({"error": "invalid grade"}), 400
    from db import db_set_model_grade
    ok = db_set_model_grade(model_id, grade)
    return jsonify({"ok": ok})


@bp.route("/video-models/<model_id>/probe", methods=["POST"])
def api_video_model_probe(model_id):
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401

    m = db_get_video_model_by_id(model_id)
    if not m:
        return jsonify({"error": "Модель не найдена"}), 404

    fal_api_key = os.environ.get("FAL_API_KEY", "")
    if not fal_api_key:
        return jsonify({"error": "FAL_API_KEY не задан"}), 500

    model_name = m["name"]
    body_tpl   = m["body_tpl"]
    submit_url = m["submit_url"]

    req_headers = {
        "Authorization": f"Key {fal_api_key}",
        "Content-Type": "application/json",
    }

    job_id = str(uuid.uuid4())
    with _probe_lock:
        _probe_jobs[job_id] = {"events": [], "done": False, "result": None, "ts": time.time()}

    def _run():
        import json as _json

        # --- Шаг 1: генерация сюжета через text-модели (как в story.py) ---
        _probe_append(job_id, "Шаг 1: генерация сюжета через text-модель…", "info")
        openrouter_key = os.environ.get("OPENROUTER_API_KEY", "")
        if not openrouter_key:
            _probe_append(job_id, "OPENROUTER_API_KEY не задан — генерация сюжета невозможна", "error")
            _probe_finish(job_id)
            return

        text_models = db_get_active_text_models()
        if not text_models:
            _probe_append(job_id, "Нет активных text-моделей", "error")
            _probe_finish(job_id)
            return

        system_prompt = db_get("system_prompt", "")
        user_prompt   = db_get("metaprompt", "")
        _probe_append(job_id, f"Промпт:\n{user_prompt}", "info")

        story_text = None
        used_text_model_id = None
        for tm in text_models:
            tm_name = tm["name"]
            _probe_append(job_id, f"Text-модель: {tm_name}", "info")
            try:
                t_body = dict(tm["body_tpl"])
                if "messages" in t_body:
                    messages = []
                    for msg in t_body["messages"]:
                        mm = dict(msg)
                        if mm.get("role") == "system":
                            mm["content"] = str(mm["content"]).format(system_prompt)
                        elif mm.get("role") == "user":
                            mm["content"] = str(mm["content"]).format(user_prompt)
                        messages.append(mm)
                    t_body["messages"] = messages
                t_body["model"] = tm["model_url"]
                t_headers = {
                    "Authorization": f"Bearer {openrouter_key}",
                    "Content-Type": "application/json",
                }
                t_req_preview = _json.dumps(t_body, ensure_ascii=False, indent=2)
                _probe_append(job_id, f"→ Запрос (text):\n{t_req_preview}", "info")
                t_resp = _requests.post(tm["platform_url"], headers=t_headers, json=t_body, timeout=60)
                _probe_append(job_id, f"← HTTP {t_resp.status_code}", "info")
                t_data = t_resp.json()
                if t_resp.status_code >= 400:
                    err = t_data.get("error") or {}
                    if isinstance(err, dict):
                        err = err.get("message", t_data)
                    _probe_append(job_id, f"[{tm_name}] HTTP {t_resp.status_code}: {err}", "warn")
                    continue
                choices = t_data.get("choices")
                if not choices:
                    _probe_append(job_id, f"[{tm_name}] нет поля choices", "warn")
                    continue
                story_text = ((choices[0].get("message") or {}).get("content") or "").strip()
                if not story_text:
                    _probe_append(job_id, f"[{tm_name}] пустой текст", "warn")
                    story_text = None
                    continue
                used_text_model_id = tm["id"]
                _probe_append(job_id, f"Сюжет:\n{story_text}", "ok")
                break
            except _requests.exceptions.Timeout:
                _probe_append(job_id, f"[{tm_name}] таймаут (60 с)", "warn")
                continue
            except Exception as e:
                _probe_append(job_id, f"[{tm_name}] ошибка: {e}", "warn")
                continue

        if not story_text:
            _probe_append(job_id, "Не удалось получить сюжет ни от одной text-модели", "error")
            _probe_finish(job_id)
            return

        # Сохраняем сюжет в БД (до добавления video_post_prompt)
        pure_story_text = story_text
        probe_story_id = db_create_story(used_text_model_id, pure_story_text)

        # --- Шаг 2: применяем video_post_prompt и строим тело запроса (как в video.py) ---
        try:
            video_duration = max(1, min(60, int(db_get("video_duration", "6"))))
        except (ValueError, TypeError):
            video_duration = 6

        video_post_prompt = db_get("video_post_prompt", "").strip()
        if video_post_prompt:
            video_post_prompt = video_post_prompt.replace("{продолжительность}", str(video_duration))
            story_text = story_text + "\n\n" + video_post_prompt

        targets = db_get_active_targets()
        if targets:
            ar_x = targets[0]["aspect_ratio_x"]
            ar_y = targets[0]["aspect_ratio_y"]
        else:
            ar_x, ar_y = 9, 16

        _probe_append(job_id, f"Шаг 2: отправка в видео-модель: {model_name}", "info")
        _probe_append(job_id, f"URL: {submit_url}", "info")
        _probe_append(job_id, f"Соотношение сторон: {ar_x}:{ar_y}, длительность: {video_duration} с", "info")

        body = dict(body_tpl)
        if "prompt" in body:
            body["prompt"] = str(body["prompt"]).format(story_text)
        if "duration" in body:
            if body["duration"] == "{int}":
                body["duration"] = video_duration
            else:
                body["duration"] = str(body["duration"]).format(video_duration)
        if "aspect_ratio" in body:
            body["aspect_ratio"] = str(body["aspect_ratio"]).format(ar_x, ar_y)

        req_preview = _json.dumps(body, ensure_ascii=False, indent=2)
        _probe_append(job_id, f"→ Запрос (video):\n{req_preview}", "info")

        try:
            resp = _requests.post(submit_url, headers=req_headers, json=body, timeout=60)
        except _requests.exceptions.Timeout:
            _probe_append(job_id, "таймаут при отправке запроса (60 с)", "error")
            _probe_finish(job_id)
            return
        except _requests.exceptions.RequestException as e:
            _probe_append(job_id, f"ошибка соединения: {e}", "error")
            _probe_finish(job_id)
            return

        raw_preview = " ".join(resp.text.split())[:500]
        _probe_append(job_id, f"← HTTP {resp.status_code}: {raw_preview}", "info")

        if resp.status_code >= 400:
            _probe_append(job_id, f"Ошибка HTTP {resp.status_code}", "error")
            _probe_finish(job_id)
            return

        try:
            submit_data = resp.json()
        except ValueError:
            _probe_append(job_id, "не-JSON ответ от сервера", "error")
            _probe_finish(job_id)
            return

        request_id = submit_data.get("request_id")
        if not request_id:
            _probe_append(job_id, f"нет request_id в ответе: {submit_data}", "error")
            _probe_finish(job_id)
            return

        _probe_append(job_id, f"request_id: {request_id}", "ok")

        _default_status = f"https://queue.fal.run/{m['model_url']}/requests/{request_id}/status"
        _default_result = f"https://queue.fal.run/{m['model_url']}/requests/{request_id}"
        status_url = submit_data.get("status_url") or _default_status
        result_url = submit_data.get("response_url") or _default_result

        for attempt in range(18):
            time.sleep(10)
            _probe_append(job_id, f"[опрос {attempt + 1}/18] проверяю статус…", "info")
            try:
                s_resp = _requests.get(status_url, headers=req_headers, timeout=30)
                s_data = s_resp.json()
            except Exception as e:
                _probe_append(job_id, f"[опрос {attempt + 1}/18] ошибка опроса: {e}", "warn")
                continue

            status = s_data.get("status", "")
            _probe_append(job_id, f"[опрос {attempt + 1}/18] статус: {status}", "info")

            if status == "COMPLETED":
                try:
                    r_resp = _requests.get(result_url, headers=req_headers, timeout=30)
                    r_data = r_resp.json()
                except Exception as e:
                    _probe_append(job_id, f"ошибка получения результата: {e}", "error")
                    _probe_finish(job_id)
                    return
                video = r_data.get("video") or {}
                video_url = video.get("url") if isinstance(video, dict) else None
                if not video_url:
                    video_url = str(r_data)
                _probe_append(job_id, f"Готово! Скачиваю видео…", "ok")
                try:
                    dl = _requests.get(video_url, timeout=120, stream=True)
                    dl.raise_for_status()
                    video_bytes = b"".join(dl.iter_content(chunk_size=256 * 1024))
                    mb = round(len(video_bytes) / 1024 / 1024, 1)
                    with _probe_lock:
                        if job_id in _probe_jobs:
                            _probe_jobs[job_id]["video_bytes"] = video_bytes
                    _probe_append(job_id, f"Видео загружено ({mb} МБ)", "ok")
                    batch_id_db = db_create_probe_batch(
                        probe_story_id,
                        used_text_model_id,
                        m["id"],
                        video_url,
                        video_bytes,
                    )
                    if batch_id_db:
                        _probe_append(job_id, f"Батч сохранён: {batch_id_db[:8]}…", "ok")
                    _probe_finish(job_id, f"/api/video-models/probe/{job_id}/video")
                except Exception as e:
                    _probe_append(job_id, f"Ошибка загрузки видео: {e}", "error")
                    _probe_finish(job_id)
                return
            elif status == "FAILED":
                err = s_data.get("error") or s_data.get("detail") or str(s_data)
                _probe_append(job_id, f"Задача завершилась с ошибкой: {err}", "error")
                _probe_finish(job_id)
                return

        _probe_append(job_id, "все попытки опроса исчерпаны (3 мин)", "error")
        _probe_finish(job_id)

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"job_id": job_id})


@bp.route("/video-models/probe/<job_id>", methods=["GET"])
def api_video_model_probe_poll(job_id):
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    with _probe_lock:
        job = _probe_jobs.get(job_id)
        if not job:
            return jsonify({"error": "not found"}), 404
        cursor = request.args.get("cursor", 0, type=int)
        new_events = job["events"][cursor:]
        done = job["done"]
        result = job["result"] if done else None
        if done and (time.time() - job["ts"]) > 120:
            del _probe_jobs[job_id]
    return jsonify({"events": new_events, "cursor": cursor + len(new_events), "done": done, "result": result})


@bp.route("/video-models/probe/<job_id>/video", methods=["GET"])
def api_video_model_probe_video(job_id):
    if not is_authenticated():
        return Response("Unauthorized", status=401)
    with _probe_lock:
        job = _probe_jobs.get(job_id)
        data = job.get("video_bytes") if job else None
    if not data:
        return Response("Not found", status=404)
    data = bytes(data)
    total = len(data)
    range_header = request.headers.get("Range")
    if range_header:
        try:
            ranges = range_header.strip().replace("bytes=", "").split("-")
            start = int(ranges[0])
            end   = int(ranges[1]) if ranges[1] else total - 1
        except (IndexError, ValueError):
            start, end = 0, total - 1
        end = min(end, total - 1)
        chunk = data[start:end + 1]
        resp = Response(chunk, status=206, mimetype="video/mp4", direct_passthrough=True)
        resp.headers["Content-Range"]  = f"bytes {start}-{end}/{total}"
        resp.headers["Accept-Ranges"]  = "bytes"
        resp.headers["Content-Length"] = str(len(chunk))
    else:
        resp = Response(data, status=200, mimetype="video/mp4")
        resp.headers["Accept-Ranges"]  = "bytes"
        resp.headers["Content-Length"] = str(total)
    resp.headers["Cache-Control"] = "no-store"
    return resp


@bp.route("/reseed", methods=["POST"])
def api_reseed():
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    try:
        init_db()
        run_upgrades()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/clear_history", methods=["POST"])
def api_clear_history():
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    try:
        result = db_clear_all_history()
        return jsonify({"ok": True, "deleted": result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/workflow/state", methods=["GET"])
def api_workflow_state():
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    state = env_get("workflow_state", "running")
    return jsonify({"state": state})


@bp.route("/workflow/start", methods=["POST"])
def api_workflow_start():
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    env_set("workflow_state", "running")
    wf_state.set_running()
    db_log_root("Движок запущен вручную", status='info')
    return jsonify({"ok": True, "state": "running"})


@bp.route("/workflow/pause", methods=["POST"])
def api_workflow_pause():
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    env_set("workflow_state", "pause")
    wf_state.set_paused()
    db_log_root("Движок приостановлен вручную", status='info')
    return jsonify({"ok": True, "state": "pause"})


@bp.route("/workflow/emulation", methods=["POST"])
def api_workflow_emulation():
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    body = request.get_json(silent=True) or {}
    val = "1" if body.get("enabled") == "1" else "0"
    env_set("emulation_mode", val)
    label = "включена" if val == "1" else "выключена"
    db_log_root(f"Эмуляция {label}", status='info')
    return jsonify({"ok": True, "emulation_mode": val})


@bp.route("/batch/<batch_id>/reset/<pipeline>", methods=["POST"])
def api_reset_batch_pipeline(batch_id, pipeline):
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    ok = db_reset_batch_pipeline(batch_id, pipeline)
    if not ok:
        return jsonify({"error": "Неизвестный пайплайн или батч не найден"}), 400
    return jsonify({"ok": True})


@bp.route("/workflow/restart", methods=["POST"])
def api_workflow_restart():
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    db_log_root("Перезапуск приложения вручную", status='info')
    def _do_restart():
        import time as _time
        import sys as _sys
        _time.sleep(0.8)
        # Close all inherited file descriptors (including the Flask socket)
        # so the new process can bind port 5000 cleanly.
        try:
            os.closerange(3, 4096)
        except Exception:
            pass
        os.execv(_sys.executable, [_sys.executable] + _sys.argv)
    threading.Thread(target=_do_restart, daemon=True).start()
    return jsonify({"ok": True})


@bp.route("/batch/<batch_id>/video", methods=["GET"])
def api_get_batch_video(batch_id):
    if not is_authenticated():
        return Response("Unauthorized", status=401)
    data = db_get_batch_video_data(batch_id)
    if data is None:
        return Response("Not found", status=404)
    data = bytes(data)
    total = len(data)
    range_header = request.headers.get("Range")
    if range_header:
        try:
            ranges = range_header.strip().replace("bytes=", "").split("-")
            start = int(ranges[0])
            end   = int(ranges[1]) if ranges[1] else total - 1
        except (IndexError, ValueError):
            start, end = 0, total - 1
        end = min(end, total - 1)
        chunk = data[start:end + 1]
        resp = Response(
            chunk,
            status=206,
            mimetype="video/mp4",
            direct_passthrough=True,
        )
        resp.headers["Content-Range"]  = f"bytes {start}-{end}/{total}"
        resp.headers["Accept-Ranges"]  = "bytes"
        resp.headers["Content-Length"] = str(len(chunk))
    else:
        resp = Response(data, status=200, mimetype="video/mp4")
        resp.headers["Accept-Ranges"]  = "bytes"
        resp.headers["Content-Length"] = str(total)
    resp.headers["Cache-Control"] = "no-store"
    return resp


@bp.route("/story/<story_id>", methods=["GET"])
def api_get_story(story_id):
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    text = db_get_story_text(story_id)
    if text is None:
        return jsonify({"error": "not found"}), 404
    return jsonify({"text": text})


