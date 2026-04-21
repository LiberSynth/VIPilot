"""
API-маршруты браузерного виджета авторизации Дзен.

Endpoints:
    POST /api/dzen-browser/start          — запустить браузер
    GET  /api/dzen-browser/stream         — SSE-поток JPEG-кадров
    POST /api/dzen-browser/event          — событие мыши/клавиатуры
    POST /api/dzen-browser/save-session   — сохранить сессию в БД
    POST /api/dzen-browser/stop           — остановить браузер
    GET  /api/dzen-browser/status         — текущий статус + дата сессии
"""

from flask import Blueprint, Response, jsonify, request, stream_with_context

from utils.auth import is_authenticated
import services.dzen_browser as browser_svc

bp = Blueprint("dzen_browser", __name__, url_prefix="/api/dzen-browser")


def _auth_required():
    if not is_authenticated():
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    return None


@bp.route("/start", methods=["POST"])
def start():
    err = _auth_required()
    if err:
        return err

    target_id = request.json.get("target_id", "") if request.is_json else request.form.get("target_id", "")
    if not target_id:
        return jsonify({"ok": False, "error": "target_id required"}), 400

    result = browser_svc.start(target_id)
    return jsonify(result)


@bp.route("/stream")
def stream():
    if not is_authenticated():
        return Response("Unauthorized", status=401)

    def generate():
        yield from browser_svc.frame_generator()

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@bp.route("/event", methods=["POST"])
def event():
    err = _auth_required()
    if err:
        return err

    if not request.is_json:
        return jsonify({"ok": False, "error": "JSON required"}), 400

    ev = request.json
    ok = browser_svc.send_event(ev)
    return jsonify({"ok": ok})


@bp.route("/save-session", methods=["POST"])
def save_session():
    err = _auth_required()
    if err:
        return err

    target_id = request.json.get("target_id", "") if request.is_json else request.form.get("target_id", "")
    if not target_id:
        return jsonify({"ok": False, "error": "target_id required"}), 400

    result = browser_svc.request_save(target_id)
    return jsonify(result)


@bp.route("/stop", methods=["POST"])
def stop():
    err = _auth_required()
    if err:
        return err

    result = browser_svc.stop()
    return jsonify(result)


@bp.route("/status")
def status():
    err = _auth_required()
    if err:
        return err

    target_id = request.args.get("target_id", "")
    info = browser_svc.get_status()

    saved_at = browser_svc.get_session_saved_at(target_id or None)

    return jsonify({
        "browser": info,
        "session_saved_at": saved_at,
    })
