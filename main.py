import os
import time
import atexit
import threading
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
)
from log import db_log_root, db_get_log, db_get_monitor
from db.init import get_db
from pipelines import planning, story, video, transcode, publish

ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "admin")



def parse_hhmm(s):
    try:
        h, m = s.strip().split(":")
        return int(h) % 24, int(m) % 60
    except Exception:
        return 6, 0



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
    history_days = parse_history_days(db_get("history_days", "7"))
    short_log_days = parse_short_log_days(db_get("short_log_days", "365"))
    emulation_mode = db_get("emulation_mode", "0") == "1"
    notify_email = db_get("notify_email", "")
    notify_phone = db_get("notify_phone", "")
    vk_publish_story = db_get("vk_publish_story", "1") == "1"
    vk_publish_wall = db_get("vk_publish_wall", "1") == "1"
    video_duration = max(1, min(60, int(db_get("video_duration", "6"))))

    return render_template(
        "admin.html",
        metaprompt=metaprompt,
        system_prompt=system_prompt,
        history_days=history_days,
        short_log_days=short_log_days,
        emulation_mode=emulation_mode,
        notify_email=notify_email,
        notify_phone=notify_phone,
        vk_publish_story=vk_publish_story,
        vk_publish_wall=vk_publish_wall,
        video_duration=video_duration,
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
            print("[SAVE] Попытка сохранить пустой мета-промпт — отклонено")
            flash("Мета-промпт не может быть пустым", "error")
            return redirect(url_for("admin"))
        # для остальных вкладок просто не перезаписываем метапромпт
    else:
        db_set("metaprompt", metaprompt)

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

    vid_dur_str = request.form.get("video_duration")
    if vid_dur_str is not None:
        try:
            vid_dur = max(1, min(60, int(vid_dur_str)))
        except (ValueError, TypeError):
            vid_dur = 6
        db_set("video_duration", str(vid_dur))

    return redirect(url_for("admin") + f"?tab={active_tab}")


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
        db_log_root("Приложение запущено", status='info')
        atexit.register(_on_exit)
        t = threading.Thread(target=main_loop, daemon=True)
        t.start()


start_main_loop()

app = flask_app

if __name__ == "__main__":
    flask_app.run(host="0.0.0.0", port=5000, debug=False)
