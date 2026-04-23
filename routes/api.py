import os
import threading
import time
from flask import Blueprint, jsonify, request, Response

from db import (
    db_get_schedule,
    db_add_schedule_slot,
    db_delete_schedule_slot,
    db_get_models,
    db_toggle_model,
    db_reorder_models,
    init_db,
    db_clear_all_history,
    db_clear_all_data,
    env_get,
    env_set,
    db_create_adhoc_batch,
    db_reset_batch_pipeline,
    db_get_story_text,
    db_get_story_title,
    db_get_story_model_info,
    db_get_batch_video_data,
    db_get_movie_video_data,
    db_get_text_model_by_id,
    db_get_video_model_by_id,
    db_create_video_batch,
    db_create_story_probe_batch,
    db_get_batch_logs,
    settings_get,
    db_get_stories_list,
    db_get_stories_pool,
    db_count_good_pool,
    db_set_story_grade,
    db_set_story_pinned,
    db_get_movies_list,
    db_set_movie_grade,
    db_upsert_story_draft,
    db_create_story_autogenerate_batch,
    settings_set,
    db_purge_unused_stories,
    db_delete_bad_movies,
    db_set_model_grade,
    db_set_model_note,
    db_set_model_body,
    db_delete_batch,
    db_get_batch_status,
    db_delete_story,
    db_delete_movie,
    db_get_movies_with_video_meta,
)
from log import db_get_monitor, log_batch_planned, write_log_entry
from utils.auth import is_authenticated
from utils.prompt_params import apply_prompt_params
from utils.utils import parse_hhmm, to_msk, to_utc_from_msk
import common.environment as environment
from common.statuses import batch_is_active

bp = Blueprint("api", __name__, url_prefix="/api")


def _build_video_response(data: bytes) -> Response:
    """Формирует ответ для видео с поддержкой HTTP Range-запросов."""
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


@bp.route("/time")
def api_time():
    return jsonify({"utc_ms": int(time.time() * 1000)})


@bp.route("/run-now", methods=["POST"])
def api_run_now():
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    batch_id = db_create_adhoc_batch()
    if not batch_id:
        return jsonify({"error": "Не удалось создать батч"}), 500
    log_batch_planned(batch_id, 'Оперативный запуск', "Запуск по запросу пользователя (внеплановый)")
    environment.wakeup_loop()
    return jsonify({"ok": True, "batch_id": batch_id})


@bp.route("/monitor")
def api_monitor():
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    data = db_get_monitor()
    data['active_batch_ids'] = list(environment.get_active_batch_ids())
    return jsonify(data)


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
    environment.wakeup_loop()
    return jsonify({"id": new_id, "time_msk": f"{h_msk:02d}:{m_msk:02d}"})


@bp.route("/schedule/<slot_id>", methods=["DELETE"])
def api_delete_schedule_slot(slot_id):
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    ok = db_delete_schedule_slot(slot_id)
    environment.wakeup_loop()
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

    batch_id = db_create_story_probe_batch(model_id)
    if not batch_id:
        return jsonify({"error": "Не удалось создать батч"}), 500
    log_batch_planned(batch_id, 'Пробный запуск текстовой модели', f"Модель: {m['name']}")
    environment.wakeup_loop()
    return jsonify({"batch_id": batch_id})


@bp.route("/video-models/<model_id>/grade", methods=["POST"])
def api_video_model_grade(model_id):
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(silent=True) or {}
    grade = data.get("grade", "good")
    if grade not in ("good", "limited", "poor", "fallback", "rejected"):
        return jsonify({"error": "invalid grade"}), 400
    ok = db_set_model_grade(model_id, grade)
    return jsonify({"ok": ok})


@bp.route("/video-models/<model_id>/note", methods=["POST"])
def api_video_model_note(model_id):
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(silent=True) or {}
    note = data.get("note", "") or ""
    ok = db_set_model_note(model_id, str(note))
    return jsonify({"ok": ok})


@bp.route("/video-models/<model_id>/body", methods=["POST"])
def api_video_model_body(model_id):
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(silent=True) or {}
    body = data.get("body")
    if not isinstance(body, dict):
        return jsonify({"error": "body must be a JSON object"}), 400
    ok = db_set_model_body(model_id, body)
    return jsonify({"ok": ok})


@bp.route("/video-models/<model_id>/probe", methods=["POST"])
def api_video_model_probe(model_id):
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401

    m = db_get_video_model_by_id(model_id)
    if not m:
        return jsonify({"error": "Модель не найдена"}), 404

    body = request.get_json(silent=True) or {}
    story_id = body.get("story_id") or None

    batch_id = db_create_video_batch('movie_probe', movie_model_id=model_id, story_id=story_id)
    if not batch_id:
        return jsonify({"error": "Не удалось создать батч"}), 500
    log_batch_planned(batch_id, 'Пробный запуск видеомодели', f"Модель: {m['name']}")
    environment.wakeup_loop()
    return jsonify({"batch_id": batch_id})


@bp.route("/batch/<batch_id>/logs", methods=["GET"])
def api_get_batch_logs(batch_id):
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    data = db_get_batch_logs(batch_id)
    if data is None:
        return jsonify({"error": "not found"}), 404
    return jsonify(data)


@bp.route("/reseed", methods=["POST"])
def api_reseed():
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    try:
        init_db()
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


@bp.route("/clear_all_data", methods=["POST"])
def api_clear_all_data():
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    try:
        tables = db_clear_all_data()
        write_log_entry(None, f"[api] Очистка всех данных: таблиц={len(tables)}, список={', '.join(tables)}")
        return jsonify({"ok": True, "tables": tables})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/workflow/state", methods=["GET"])
def api_workflow_state():
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    state = env_get("workflow_state", "running")
    return jsonify({"state": state, "active_threads": environment.get_active_threads()})


@bp.route("/workflow/start", methods=["POST"])
def api_workflow_start():
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    env_set("workflow_state", "running")
    environment.set_running()
    write_log_entry(None, "[api] Движок запущен вручную")
    return jsonify({"ok": True, "state": "running"})


@bp.route("/workflow/pause", methods=["POST"])
def api_workflow_pause():
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    env_set("workflow_state", "pause")
    environment.set_paused()
    write_log_entry(None, "[api] Движок приостановлен вручную")
    return jsonify({"ok": True, "state": "pause"})


@bp.route("/workflow/deep_debugging", methods=["POST"])
def api_workflow_deep_debugging():
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    body = request.get_json(silent=True) or {}
    val = "1" if body.get("enabled") == "1" else "0"
    env_set("deep_debugging", val)
    label = "включена" if val == "1" else "выключена"
    write_log_entry(None, f"[api] Глубокая отладка {label}")
    return jsonify({"ok": True, "deep_debugging": val})


@bp.route("/workflow/use_donor", methods=["POST"])
def api_workflow_use_donor():
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    body = request.get_json(silent=True) or {}
    val = "1" if body.get("enabled") == "1" else "0"
    env_set("use_donor", val)
    label = "включен" if val == "1" else "выключен"
    write_log_entry(None, f"[api] Подбирать видео из пула: {label}")
    return jsonify({"ok": True, "use_donor": val})


@bp.route("/donors/count", methods=["GET"])
def api_donors_count():
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    from db import db_get_donor_count
    good_only = request.args.get("good_only") == "1"
    return jsonify({"count": db_get_donor_count(good_only=good_only)})


@bp.route("/workflow/approve_movies", methods=["POST"])
def api_workflow_approve_movies():
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    body = request.get_json(silent=True) or {}
    val = "1" if body.get("enabled") == "1" else "0"
    settings_set("approve_movies", val)
    if val == "1":
        env_set("use_donor", "1")
    label = "включено" if val == "1" else "выключено"
    write_log_entry(None, f"[api] Утверждать видео: {label}")
    return jsonify({"ok": True, "approve_movies": val})


@bp.route("/workflow/emulation", methods=["POST"])
def api_workflow_emulation():
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    body = request.get_json(silent=True) or {}
    val = "1" if body.get("enabled") == "1" else "0"
    env_set("emulation_mode", val)
    label = "включена" if val == "1" else "выключена"
    write_log_entry(None, f"[api] Эмуляция {label}")
    return jsonify({"ok": True, "emulation_mode": val})


@bp.route("/monitor/batch/<batch_id>/delete", methods=["POST"])
def api_delete_batch(batch_id):
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    if batch_id in environment.get_active_batch_ids():
        return jsonify({"ok": False, "error": "Батч сейчас активно обрабатывается пайплайном"}), 409
    batch_status = db_get_batch_status(batch_id)
    if batch_is_active(batch_status):
        return jsonify({"ok": False, "error": "Нельзя удалить батч в активном статусе"}), 409
    try:
        ok = db_delete_batch(batch_id)
        if not ok:
            return jsonify({"error": "not found"}), 404
        return jsonify({"ok": True})
    except Exception as e:
        write_log_entry(None, f"[api] Ошибка удаления батча: {e}", level='silent')
        return jsonify({"ok": False, "error": "internal error"}), 500


@bp.route("/batch/<batch_id>/reset/<pipeline>", methods=["POST"])
def api_reset_batch_pipeline(batch_id, pipeline):
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    ok = db_reset_batch_pipeline(batch_id, pipeline)
    if not ok:
        return jsonify({"error": "Неизвестный пайплайн или батч не найден"}), 400
    environment.wakeup_loop()
    return jsonify({"ok": True})


@bp.route("/workflow/restart", methods=["POST"])
def api_workflow_restart():
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    write_log_entry(None, "[api] Перезапуск приложения вручную")
    def _do_restart():
        import time as _time
        import sys as _sys
        _time.sleep(0.8)
        # Закрываем все унаследованные файловые дескрипторы (в т.ч. сокет Flask),
        # чтобы новый процесс мог занять порт 5000 без конфликтов.
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
    return _build_video_response(bytes(data))


@bp.route("/story/<story_id>", methods=["GET"])
def api_get_story(story_id):
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    text = db_get_story_text(story_id)
    if text is None:
        return jsonify({"error": "not found"}), 404
    title = db_get_story_title(story_id) or ''
    format_prompt = settings_get("format_prompt", "") or ""
    user_prompt = settings_get("text_prompt", "") or ""
    user_prompt = apply_prompt_params(user_prompt)
    format_prompt = apply_prompt_params(format_prompt)
    model_info = db_get_story_model_info(story_id)
    platform_name = model_info["platform_name"] if model_info else ""
    model_name = model_info["model_name"] if model_info else ""
    model_body = model_info["body"] if model_info else None
    return jsonify({"text": text, "title": title, "format_prompt": format_prompt, "user_prompt": user_prompt, "platform_name": platform_name, "model_name": model_name, "model_body": model_body})


@bp.route("/batch/<batch_id>/publish-frame")
def api_batch_publish_frame(batch_id):
    """Возвращает последний JPEG-скриншот браузера публикации для батча."""
    if not is_authenticated():
        return Response("Unauthorized", status=401)
    from services.dzen_browser import get_frame_for_batch
    img = get_frame_for_batch(batch_id)
    if img is None:
        return Response("", status=204)
    return Response(
        img,
        mimetype="image/jpeg",
        headers={"Cache-Control": "no-store, no-cache"},
    )


production_bp = Blueprint("production_api", __name__)


def _production_auth_check():
    from flask import session
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401
    roles = session.get("roles", [])
    slugs = {r["slug"] for r in roles}
    if not (slugs & {"producer", "root"}):
        return jsonify({"error": "forbidden"}), 403
    return None


@production_bp.route("/production/stories", methods=["GET"])
def api_production_stories():
    err = _production_auth_check()
    if err:
        return err
    show_used = request.args.get("show_used", "1") != "0"
    show_bad = request.args.get("show_bad", "1") != "0"
    for_approval = request.args.get("for_approval", "0") == "1"
    only_pinned = request.args.get("only_pinned", "0") == "1"
    pin_id = request.args.get("pin_id") or None
    approve_movies = settings_get("approve_movies", "0") == "1"
    stories = db_get_stories_list(show_used=show_used, show_bad=show_bad, for_approval=for_approval, pin_id=pin_id, approve_movies=approve_movies, only_pinned=only_pinned)
    return jsonify(stories)


@production_bp.route("/production/story/<story_id>/pin", methods=["POST"])
def api_production_story_pin(story_id):
    err = _production_auth_check()
    if err:
        return err
    data = request.get_json(silent=True) or {}
    value = data.get("pinned")
    if not isinstance(value, bool):
        return jsonify({"error": "pinned must be boolean"}), 400
    ok = db_set_story_pinned(story_id, value)
    if not ok:
        return jsonify({"error": "not found"}), 404
    return jsonify({"ok": True})


@production_bp.route("/production/stories/pool", methods=["GET"])
def api_production_stories_pool():
    err = _production_auth_check()
    if err:
        return err
    approve_stories = settings_get("approve_stories", "0") == "1"
    approve_movies = settings_get("approve_movies", "0") == "1"
    stories = db_get_stories_pool(grade_required=approve_stories, approve_movies=approve_movies)
    return jsonify(stories)


@production_bp.route("/production/stories/good_pool_count", methods=["GET"])
def api_production_good_pool_count():
    err = _production_auth_check()
    if err:
        return err
    approve_stories = settings_get("approve_stories", "0") == "1"
    return jsonify({"count": db_count_good_pool(grade_required=approve_stories)})


@production_bp.route("/production/env", methods=["POST"])
def api_production_env_set():
    err = _production_auth_check()
    if err:
        return err
    data = request.get_json(silent=True) or {}
    key = data.get("key", "")
    value = data.get("value", "")
    allowed_keys = {"screenwriter_show_used", "screenwriter_only_good", "screenwriter_for_approval", "screenwriter_only_pinned"}
    if key not in allowed_keys:
        return jsonify({"error": "invalid key"}), 400
    env_set(key, str(value))
    return jsonify({"ok": True})


_PRODUCTION_GRADE_CYCLE = ["good", "bad", None]


@production_bp.route("/production/movies", methods=["GET"])
def api_production_movies():
    err = _production_auth_check()
    if err:
        return err
    for_approval = request.args.get("for_approval") == "1"
    show_published = request.args.get("show_published", "1") != "0"
    show_bad = request.args.get("show_bad", "1") != "0"
    movies = db_get_movies_list(
        show_published=show_published,
        show_bad=show_bad,
        for_approval=for_approval,
    )
    return jsonify(movies)


@production_bp.route("/production/movie/<movie_id>/video", methods=["GET"])
def api_production_movie_video(movie_id):
    err = _production_auth_check()
    if err:
        return err
    data = db_get_movie_video_data(movie_id)
    if data is None:
        return Response("Not found", status=404)
    return _build_video_response(bytes(data))


@production_bp.route("/production/movie/<movie_id>/grade", methods=["POST"])
def api_production_movie_grade(movie_id):
    err = _production_auth_check()
    if err:
        return err
    data = request.get_json(silent=True) or {}
    grade = data.get("grade", "good")
    if grade not in _PRODUCTION_GRADE_CYCLE:
        return jsonify({"error": "invalid_grade"}), 400
    ok = db_set_movie_grade(movie_id, grade)
    if not ok:
        return jsonify({"error": "not_found"}), 404
    return jsonify({"ok": True, "grade": grade})


@production_bp.route("/production/story/<story_id>/grade", methods=["POST"])
def api_production_story_grade(story_id):
    err = _production_auth_check()
    if err:
        return err
    data = request.get_json(silent=True) or {}
    grade = data.get("grade", "good")
    if grade not in _PRODUCTION_GRADE_CYCLE:
        return jsonify({"error": "invalid_grade"}), 400
    ok = db_set_story_grade(story_id, grade)
    if ok is None or ok is False:
        return jsonify({"error": "not_found"}), 404
    return jsonify({"ok": True, "grade": grade})


@production_bp.route("/production/story/<story_id>/delete", methods=["DELETE"])
def api_production_story_delete(story_id):
    err = _production_auth_check()
    if err:
        return err
    from db.connection import get_db
    from common.statuses import FINAL_BATCH_STATUSES
    final_statuses_sql = ', '.join(f"'{s}'" for s in FINAL_BATCH_STATUSES)
    conflict_error = None
    not_found = False
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pinned FROM stories WHERE id = %s", (story_id,))
            row = cur.fetchone()
            if not row:
                not_found = True
            elif row[0]:
                conflict_error = "Сюжет закреплён и не может быть удалён"
            else:
                cur.execute(
                    "SELECT 1 FROM batches WHERE story_id = %s AND movie_id IS NOT NULL LIMIT 1",
                    (story_id,),
                )
                if cur.fetchone():
                    conflict_error = "К сюжету привязано готовое видео"
                else:
                    cur.execute(
                        f"SELECT 1 FROM batches WHERE story_id = %s AND status NOT IN ({final_statuses_sql}) LIMIT 1",
                        (story_id,),
                    )
                    if cur.fetchone():
                        conflict_error = "Сюжет участвует в активном батче"
    if not_found:
        return jsonify({"error": "not found"}), 404
    if conflict_error:
        return jsonify({"error": conflict_error}), 409
    result = db_delete_story(story_id)
    if result["stories"] == 0:
        return jsonify({"error": "not found"}), 404
    return jsonify({"ok": True, "deleted": result})


@production_bp.route("/production/movie/<movie_id>/delete", methods=["DELETE"])
def api_production_movie_delete(movie_id):
    err = _production_auth_check()
    if err:
        return err
    result = db_delete_movie(movie_id)
    if not result:
        return jsonify({"error": "not found"}), 404
    return jsonify({"ok": True, "deleted": result})


@production_bp.route("/production/story/draft", methods=["POST"])
def api_production_story_draft():
    err = _production_auth_check()
    if err:
        return err
    data = request.get_json(silent=True) or {}
    title = data.get("title", "")
    content = data.get("content", "")
    story_id = data.get("story_id") or None
    new_id = db_upsert_story_draft(story_id, title, content)
    if new_id is None:
        return jsonify({"error": "db_error"}), 500
    return jsonify({"story_id": new_id})


@production_bp.route("/production/stories/delete_bad", methods=["POST"])
def api_production_delete_bad_stories():
    err = _production_auth_check()
    if err:
        return err
    try:
        deleted = db_purge_unused_stories()
        return jsonify({"ok": True, "deleted": deleted})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@production_bp.route("/production/movies/delete_bad", methods=["POST"])
def api_production_delete_bad_movies():
    err = _production_auth_check()
    if err:
        return err
    try:
        deleted = db_delete_bad_movies()
        return jsonify({"ok": True, "deleted": deleted})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@production_bp.route("/production/movies/good_meta", methods=["GET"])
def api_production_good_movies_meta():
    err = _production_auth_check()
    if err:
        return err
    return jsonify(db_get_movies_with_video_meta())


@production_bp.route("/production/movies/<movie_id>/download", methods=["GET"])
def api_production_movie_download(movie_id):
    err = _production_auth_check()
    if err:
        return err
    data = db_get_movie_video_data(movie_id)
    if data is None:
        return jsonify({"error": "not_found"}), 404
    return Response(data, mimetype="video/mp4")


@production_bp.route("/production/movies/upload", methods=["POST"])
def api_production_movies_upload():
    err = _production_auth_check()
    if err:
        return err
    if 'file' not in request.files:
        return jsonify({"error": "Файл не передан"}), 400
    f = request.files['file']
    if not f.filename:
        return jsonify({"error": "Имя файла пустое"}), 400
    filename = f.filename
    if filename.lower().endswith('.mp4'):
        filename = filename[:-4]
    words = filename.split()[:4]
    title = ' '.join(words) or 'Без названия'
    video_data = f.read()
    from db import db_create_manual_movie
    batch_id = db_create_manual_movie(title, video_data)
    environment.wakeup_loop()
    return jsonify({"ok": True, "batch_id": batch_id})


@production_bp.route("/production/video/generate", methods=["POST"])
def api_production_video_generate():
    err = _production_auth_check()
    if err:
        return err
    data = request.get_json(silent=True) or {}
    model_id = data.get("model_id") or None
    story_id = data.get("story_id") or None
    if model_id:
        batch_id = db_create_video_batch('movie_probe', movie_model_id=model_id, story_id=story_id)
    else:
        batch_id = db_create_video_batch('movie_probe', story_id=story_id)
    if not batch_id:
        return jsonify({"error": "db_error"}), 500
    environment.wakeup_loop()
    log_batch_planned(batch_id, "Генерация видео вручную", "Запуск по запросу пользователя")
    return jsonify({"batch_id": batch_id})


@production_bp.route("/production/story/generate", methods=["POST"])
def api_production_story_generate():
    err = _production_auth_check()
    if err:
        return err
    data = request.get_json(silent=True) or {}
    model_id = data.get("model_id") or None
    if model_id:
        batch_id = db_create_story_probe_batch(model_id)
    else:
        batch_id = db_create_story_autogenerate_batch()
    if not batch_id:
        return jsonify({"error": "db_error"}), 500
    environment.wakeup_loop()
    log_batch_planned(batch_id, 'Генерация сюжета вручную', "Запуск по запросу пользователя")
    return jsonify({"batch_id": batch_id})


