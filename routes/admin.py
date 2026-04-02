import time
from flask import (
    Blueprint,
    render_template,
    request,
    redirect,
    url_for,
    session,
    flash,
    send_file,
)

from db import (
    db_get,
    db_set,
)
from utils.consts import ADMIN_PASSWORD
from utils.auth import is_authenticated, password_fingerprint
from utils.utils import (
    parse_batch_lifetime,
    parse_entries_lifetime,
    parse_log_lifetime,
    parse_file_lifetime,
)

bp = Blueprint("admin", __name__)


@bp.route("/favicon.ico")
def favicon():
    return send_file("generated-icon.png", mimetype="image/png")


@bp.route("/healthz")
def healthz():
    return "ok", 200


@bp.route("/", methods=["GET", "POST"])
def login():
    if is_authenticated():
        return redirect(url_for("admin.admin_page"))

    error = False
    if request.method == "POST":
        if request.form.get("password") == ADMIN_PASSWORD:
            session["auth"] = True
            session["pw_fp"] = password_fingerprint()
            session["auth_ts"] = time.time()
            session.permanent = False
            return redirect(url_for("admin.admin_page"))
        error = True
    return render_template("login.html", error=error)


@bp.route("/admin")
def admin_page():
    if not is_authenticated():
        return redirect(url_for("admin.login"))
    metaprompt      = db_get("metaprompt", "")
    system_prompt   = db_get("system_prompt", "")
    batch_lifetime     = parse_batch_lifetime(db_get("batch_lifetime", "7"))
    log_lifetime       = parse_log_lifetime(db_get("log_lifetime", "365"))
    entries_lifetime   = parse_entries_lifetime(db_get("entries_lifetime", "30"))
    file_lifetime      = parse_file_lifetime(db_get("file_lifetime", "7"))
    emulation_mode     = db_get("emulation_mode", "0") == "1"
    notify_email       = db_get("notify_email", "")
    notify_phone       = db_get("notify_phone", "")
    vk_publish_story   = db_get("vk_publish_story", "1") == "1"
    vk_publish_wall    = db_get("vk_publish_wall",  "1") == "1"
    video_duration     = max(1, min(60, int(db_get("video_duration", "6"))))
    buffer_hours       = max(1, min(720, int(db_get("buffer_hours", "24"))))
    loop_interval      = max(1, min(3600, int(db_get("loop_interval", "5"))))

    return render_template(
        "admin.html",
        metaprompt=metaprompt,
        system_prompt=system_prompt,
        batch_lifetime=batch_lifetime,
        log_lifetime=log_lifetime,
        entries_lifetime=entries_lifetime,
        file_lifetime=file_lifetime,
        emulation_mode=emulation_mode,
        notify_email=notify_email,
        notify_phone=notify_phone,
        vk_publish_story=vk_publish_story,
        vk_publish_wall=vk_publish_wall,
        video_duration=video_duration,
        buffer_hours=buffer_hours,
        loop_interval=loop_interval,
    )


@bp.route("/save", methods=["POST"])
def save():
    if not is_authenticated():
        return redirect(url_for("admin.login"))

    system_prompt_val = request.form.get("system_prompt")
    if system_prompt_val is not None:
        db_set("system_prompt", system_prompt_val)

    metaprompt = request.form.get("metaprompt", "").strip()
    active_tab = request.form.get("active_tab", "pipeline")
    if not metaprompt:
        if active_tab == "story":
            print("[SAVE] Попытка сохранить пустой мета-промпт — отклонено")
            flash("Мета-промпт не может быть пустым", "error")
            return redirect(url_for("admin.admin_page"))
    else:
        db_set("metaprompt", metaprompt)

    entries_lifetime_raw = request.form.get("entries_lifetime", "").strip()
    log_lifetime_raw     = request.form.get("log_lifetime",     "").strip()
    batch_lifetime_raw   = request.form.get("batch_lifetime",   "").strip()
    file_lifetime_raw    = request.form.get("file_lifetime",    "").strip()

    if entries_lifetime_raw or log_lifetime_raw or batch_lifetime_raw:
        el  = parse_entries_lifetime(entries_lifetime_raw or db_get("entries_lifetime", "30"))
        ll  = parse_log_lifetime(log_lifetime_raw         or db_get("log_lifetime",     "365"))
        bl  = parse_batch_lifetime(batch_lifetime_raw     or db_get("batch_lifetime",   "7"))
        if el <= ll <= bl:
            db_set("entries_lifetime", str(el))
            db_set("log_lifetime",     str(ll))
            db_set("batch_lifetime",   str(bl))
        else:
            flash("Сроки хранения нарушают иерархию: подробный ≤ краткий ≤ история батчей", "error")

    if file_lifetime_raw:
        db_set("file_lifetime", str(parse_file_lifetime(file_lifetime_raw)))

    emulation_raw = request.form.get("emulation_mode", "0")
    db_set("emulation_mode", "1" if emulation_raw == "1" else "0")

    db_set("notify_email", request.form.get("notify_email", "").strip())
    db_set("notify_phone", request.form.get("notify_phone", "").strip())

    vk_story_raw = request.form.get("vk_publish_story", "0")
    vk_wall_raw  = request.form.get("vk_publish_wall",  "0")
    if vk_story_raw != "1" and vk_wall_raw != "1":
        vk_story_raw = "1"
    db_set("vk_publish_story", "1" if vk_story_raw == "1" else "0")
    db_set("vk_publish_wall",  "1" if vk_wall_raw  == "1" else "0")

    vid_dur_str = request.form.get("video_duration")
    if vid_dur_str is not None:
        try:
            vid_dur = max(1, min(60, int(vid_dur_str)))
        except (ValueError, TypeError):
            vid_dur = 6
        db_set("video_duration", str(vid_dur))

    buf_str = request.form.get("buffer_hours", "").strip()
    if buf_str:
        try:
            db_set("buffer_hours", str(max(1, min(720, int(buf_str)))))
        except (ValueError, TypeError):
            pass

    loop_str = request.form.get("loop_interval", "").strip()
    if loop_str:
        try:
            db_set("loop_interval", str(max(1, min(3600, int(loop_str)))))
        except (ValueError, TypeError):
            pass

    return redirect(url_for("admin.admin_page") + f"?tab={active_tab}")


@bp.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("admin.login"))
