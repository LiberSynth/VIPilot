"""
Единый Blueprint браузерного виджета авторизации для всех платформ.

Endpoints (slug = dzen | rutube | vkvideo):
    POST /api/<slug>-browser/start          — запустить браузер
    GET  /api/<slug>-browser/stream         — SSE-поток JPEG-кадров
    POST /api/<slug>-browser/event          — событие мыши/клавиатуры
    POST /api/<slug>-browser/save-session   — сохранить сессию в БД
    POST /api/<slug>-browser/stop           — остановить браузер
    GET  /api/<slug>-browser/status         — текущий статус + дата сессии
"""

from flask import Blueprint, Response, jsonify, request, stream_with_context

from utils.auth import is_authenticated
from services.browser_registry import get_browser, SLUGS

bp = Blueprint("browser_widget", __name__, url_prefix="/api")


def _auth_required():
    if not is_authenticated():
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    return None


def _resolve(slug):
    """Возвращает (browser, None) или (None, error_response)."""
    if slug not in SLUGS:
        return None, (jsonify({"ok": False, "error": f"Unknown platform: {slug}"}), 404)
    return get_browser(slug), None


@bp.route("/<slug>-browser/start", methods=["POST"])
def start(slug):
    err = _auth_required()
    if err:
        return err

    svc, err = _resolve(slug)
    if err:
        return err

    target_id = request.json.get("target_id", "") if request.is_json else request.form.get("target_id", "")
    if not target_id:
        return jsonify({"ok": False, "error": "target_id required"}), 400

    return jsonify(svc.start(target_id))


@bp.route("/<slug>-browser/stream")
def stream(slug):
    if not is_authenticated():
        return Response("Unauthorized", status=401)

    svc, err = _resolve(slug)
    if err:
        return err

    def generate():
        yield from svc.frame_generator()

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@bp.route("/<slug>-browser/event", methods=["POST"])
def event(slug):
    err = _auth_required()
    if err:
        return err

    svc, err = _resolve(slug)
    if err:
        return err

    if not request.is_json:
        return jsonify({"ok": False, "error": "JSON required"}), 400

    return jsonify({"ok": svc.send_event(request.json)})


@bp.route("/<slug>-browser/save-session", methods=["POST"])
def save_session(slug):
    err = _auth_required()
    if err:
        return err

    svc, err = _resolve(slug)
    if err:
        return err

    target_id = request.json.get("target_id", "") if request.is_json else request.form.get("target_id", "")
    if not target_id:
        return jsonify({"ok": False, "error": "target_id required"}), 400

    return jsonify(svc.request_save(target_id))


@bp.route("/<slug>-browser/stop", methods=["POST"])
def stop(slug):
    err = _auth_required()
    if err:
        return err

    svc, err = _resolve(slug)
    if err:
        return err

    return jsonify(svc.stop())


@bp.route("/<slug>-browser/status")
def status(slug):
    err = _auth_required()
    if err:
        return err

    svc, err = _resolve(slug)
    if err:
        return err

    target_id = request.args.get("target_id", "")
    return jsonify({
        "browser": svc.get_status(),
        "session_saved_at": svc.get_session_saved_at(target_id or None),
    })
