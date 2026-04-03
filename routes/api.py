import os
import threading
import time
import requests as _requests
from flask import Blueprint, jsonify, request, Response
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
    import json as _json
    from flask import Response as _Response

    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401

    m = db_get_text_model_by_id(model_id)
    if not m:
        def _err():
            yield "data: " + _json.dumps({"type": "fail", "msg": "Модель не найдена"}) + "\n\n"
        return _Response(_err(), mimetype="text/event-stream",
                         headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    api_key = os.environ.get("OPENROUTER_API_KEY", "")
    if not api_key:
        def _err():
            yield "data: " + _json.dumps({"type": "fail", "msg": "OPENROUTER_API_KEY не задан"}) + "\n\n"
        return _Response(_err(), mimetype="text/event-stream",
                         headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    try:
        fails_to_next = max(1, int(db_get("story_fails_to_next", "3")))
    except (ValueError, TypeError):
        fails_to_next = 3

    system_prompt = db_get("system_prompt", "")
    user_prompt   = db_get("metaprompt", "")

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

    def _generate():
        yield ": padding " + " " * 2048 + "\n\n"
        last_error = "Неизвестная ошибка"
        for attempt in range(fails_to_next):
            yield "data: " + _json.dumps({
                "type": "attempt",
                "attempt": attempt + 1,
                "total": fails_to_next
            }) + "\n\n"

            try:
                resp = _requests.post(platform_url, headers=req_headers, json=body, timeout=60)
            except _requests.exceptions.Timeout:
                last_error = "Таймаут 60с"
                yield "data: " + _json.dumps({
                    "type": "error", "attempt": attempt + 1, "total": fails_to_next,
                    "msg": last_error
                }) + "\n\n"
                continue
            except _requests.exceptions.RequestException as e:
                last_error = f"Ошибка соединения: {e}"
                yield "data: " + _json.dumps({
                    "type": "error", "attempt": attempt + 1, "total": fails_to_next,
                    "msg": last_error
                }) + "\n\n"
                continue

            try:
                data = resp.json()
            except ValueError:
                last_error = f"Не-JSON ответ (HTTP {resp.status_code})"
                yield "data: " + _json.dumps({
                    "type": "error", "attempt": attempt + 1, "total": fails_to_next,
                    "msg": last_error
                }) + "\n\n"
                continue

            if resp.status_code >= 400:
                err = data.get("error", {})
                if isinstance(err, dict):
                    err = err.get("message", str(data))
                last_error = f"HTTP {resp.status_code}: {err}"
                yield "data: " + _json.dumps({
                    "type": "error", "attempt": attempt + 1, "total": fails_to_next,
                    "msg": last_error
                }) + "\n\n"
                continue

            choices = data.get("choices")
            if not choices:
                last_error = "Нет поля choices в ответе"
                yield "data: " + _json.dumps({
                    "type": "error", "attempt": attempt + 1, "total": fails_to_next,
                    "msg": last_error
                }) + "\n\n"
                continue

            result = ((choices[0].get("message") or {}).get("content") or "").strip()
            if not result:
                last_error = "Пустой текст в ответе"
                yield "data: " + _json.dumps({
                    "type": "error", "attempt": attempt + 1, "total": fails_to_next,
                    "msg": last_error
                }) + "\n\n"
                continue

            yield "data: " + _json.dumps({
                "type": "success", "attempt": attempt + 1, "total": fails_to_next,
                "result": result
            }) + "\n\n"
            return

        yield "data: " + _json.dumps({"type": "fail", "msg": last_error}) + "\n\n"

    return _Response(_generate(), mimetype="text/event-stream",
                     headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


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


@bp.route("/sse-test")
def api_sse_test():
    import json as _j
    def _gen():
        yield ": padding " + " " * 2048 + "\n\n"
        for i in range(1, 4):
            yield "data: " + _j.dumps({"n": i, "t": time.time()}) + "\n\n"
            time.sleep(1)
        yield "data: " + _j.dumps({"n": "done"}) + "\n\n"
    return Response(_gen(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
