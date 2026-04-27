import time
from utils.version import VERSION as APP_VERSION
from flask import (
    Blueprint,
    render_template,
    request,
    redirect,
    url_for,
    session,
    flash,
    send_file,
    make_response,
)

from db import (
    settings_get,
    settings_set,
    env_get,
    env_set,
    cycle_config_get,
    cycle_config_set,
    db_get_active_targets,
    db_get_all_targets,
    db_update_target_aspect_ratio,
    db_get_target_by_name,
    db_update_target_publish_method_by_slug,
    db_get_user_by_login,
    db_get_role_modules,
)
import common.environment as environment
from utils.auth import is_authenticated
from utils.limiter import limiter
from utils.utils import (
    parse_batch_lifetime,
    parse_long_lifetime,
    parse_file_lifetime,
)

bp = Blueprint("web", __name__)

RESERVED_SLUGS = {"web", "save", "logout", "select-module", "healthz", "favicon.ico", "icon-preview", "root", "production"}


def _get_session_roles():
    return session.get("roles", [])


def _has_slug(slug):
    return any(r["slug"] == slug for r in _get_session_roles())


_SLUG_TO_URL = {"producer": "/production"}


def _role_url(role):
    if role["slug"] == "root":
        return url_for("web.root_page")
    return _SLUG_TO_URL.get(role["slug"], f"/{role['slug']}")


_URL_TO_SLUG = {v.lstrip("/"): k for k, v in _SLUG_TO_URL.items()}


def _nav_modules(current_slug):
    roles = _get_session_roles()
    if len(roles) < 2:
        return []
    effective_slug = _URL_TO_SLUG.get(current_slug, current_slug)
    return [r for r in roles if r["slug"] != effective_slug]


def _save_last_page():
    qs = request.query_string.decode()
    session["last_page"] = request.path + ("?" + qs if qs else "")


def _get_last_page():
    last = session.get("last_page")
    if not last:
        return None
    path = last.split("?")[0].rstrip("/")
    if path == "/web":
        if _has_slug("root"):
            return last
    elif path == "/production":
        if _has_slug("producer"):
            return last
    else:
        slug = path.lstrip("/")
        if slug and _has_slug(slug):
            return last
    return None


def _redirect_after_login():
    roles = _get_session_roles()
    if len(roles) == 1:
        return redirect(_role_url(roles[0]))
    if len(roles) > 1:
        last = _get_last_page()
        if last:
            return redirect(last)
        return redirect(url_for("web.select_module"))
    session.clear()
    return redirect(url_for("web.login", reason="no_roles"))


@bp.route("/favicon.ico")
def favicon():
    return send_file("generated-icon.png", mimetype="image/png")


@bp.route("/healthz")
def healthz():
    return "ok", 200


@bp.route("/icon-preview")
def icon_preview():
    return render_template("icon_preview.html")


@bp.route("/", methods=["GET", "POST"])
@limiter.limit("10 per minute")
def login():
    if is_authenticated():
        return _redirect_after_login()

    no_roles = request.args.get("reason") == "no_roles"
    error = False
    if request.method == "POST":
        login_val = request.form.get("login", "").strip()
        password_val = request.form.get("password", "")
        user = db_get_user_by_login(login_val)
        if user and user["password"] == password_val:
            session["auth"] = True
            session["auth_ts"] = time.time()
            session["roles"] = user["roles"]
            session.permanent = True
            return _redirect_after_login()
        error = True
        no_roles = False
    return render_template("login.html", error=error, no_roles=no_roles)


@bp.route("/web")
def root_page():
    if not is_authenticated():
        return redirect(url_for("web.login"))
    if not _has_slug("root"):
        roles = _get_session_roles()
        non_root = [r for r in roles if r["slug"] != "root"]
        if len(non_root) == 1:
            return redirect(_role_url(non_root[0]))
        if non_root:
            return redirect(url_for("web.select_module"))
        return redirect(url_for("web.login"))
    text_prompt     = cycle_config_get("text_prompt")
    format_prompt   = cycle_config_get("format_prompt")
    batch_lifetime     = parse_batch_lifetime(settings_get("batch_lifetime", "7"))
    log_lifetime       = parse_long_lifetime(settings_get("log_lifetime", "365"))
    entries_lifetime   = parse_long_lifetime(settings_get("entries_lifetime", "30"), default=30)
    file_lifetime      = parse_file_lifetime(settings_get("file_lifetime", "7"))
    emulation_mode     = environment.emulation_mode
    use_donor          = environment.use_donor
    notify_email       = settings_get("notify_email", "")
    notify_phone       = settings_get("notify_phone", "")
    vk_target  = db_get_target_by_name("VKontakte")
    vk_active  = bool(vk_target.get("active")) if vk_target else False
    _vk_pm     = (vk_target.get("config") or {}).get("publish_method", {}) if vk_target else {}
    vk_publish_story     = bool(_vk_pm.get("story",     0))
    vk_publish_wall      = bool(_vk_pm.get("wall",      0))
    vk_publish_clip_story = bool(_vk_pm.get("clip_story", 0))
    vk_publish_clip_wall  = bool(_vk_pm.get("clip_wall",  0))
    video_duration     = max(1, min(60, cycle_config_get("video_duration")))
    video_post_prompt  = cycle_config_get("video_post_prompt")
    buffer_hours       = max(1, min(720, int(settings_get("buffer_hours", "24"))))
    loop_interval       = environment.loop_interval
    max_batch_threads   = environment.max_threads
    max_model_passes    = environment.max_model_passes
    story_fails_to_next = max(1, int(settings_get("story_fails_to_next", "3")))
    words_per_second    = float(cycle_config_get("words_per_second") or 8.0)
    good_samples_count  = max(1, int(cycle_config_get("good_samples_count") or 25))
    video_fails_to_next = max(1, int(settings_get("video_fails_to_next", "3")))
    approve_stories     = cycle_config_get("approve_stories")
    approve_movies      = cycle_config_get("approve_movies")
    deep_debugging      = environment.deep_debugging

    workflow_state = env_get("workflow_state", "running")

    dzen_target     = db_get_target_by_name("Дзен")
    dzen_config     = dzen_target.get("config") or {} if dzen_target else {}
    dzen_publisher_id = dzen_config.get("publisher_id", "")
    dzen_target_id = dzen_target["id"] if dzen_target else None
    dzen_active    = bool(dzen_target.get("active")) if dzen_target else False

    rutube_target     = db_get_target_by_name("Rutube")
    rutube_config     = rutube_target.get("config") or {} if rutube_target else {}
    rutube_person_id  = rutube_config.get("person_id", "")
    rutube_target_id  = rutube_target["id"] if rutube_target else None
    rutube_active     = bool(rutube_target.get("active")) if rutube_target else False

    vkvideo_target    = db_get_target_by_name("VK Видео")
    vkvideo_config    = vkvideo_target.get("config") or {} if vkvideo_target else {}
    vkvideo_club_id   = vkvideo_config.get("club_id", "")
    vkvideo_target_id = vkvideo_target["id"] if vkvideo_target else None
    vkvideo_active    = bool(vkvideo_target.get("active")) if vkvideo_target else False

    active_targets  = db_get_active_targets()
    target          = active_targets[0] if active_targets else None
    target_id       = target["id"] if target else None
    aspect_ratio_x  = target["aspect_ratio_x"] if target else 9
    aspect_ratio_y  = target["aspect_ratio_y"] if target else 16
    all_targets     = db_get_all_targets()
    publish_order   = [t["slug"] for t in all_targets if t.get("slug")]

    _save_last_page()
    resp = make_response(render_template(
        "root.html",
        text_prompt=text_prompt,
        format_prompt=format_prompt,
        batch_lifetime=batch_lifetime,
        log_lifetime=log_lifetime,
        entries_lifetime=entries_lifetime,
        file_lifetime=file_lifetime,
        emulation_mode=emulation_mode,
        use_donor=use_donor,
        notify_email=notify_email,
        notify_phone=notify_phone,
        vk_publish_story=vk_publish_story,
        vk_publish_wall=vk_publish_wall,
        vk_publish_clip_story=vk_publish_clip_story,
        vk_publish_clip_wall=vk_publish_clip_wall,
        video_duration=video_duration,
        video_post_prompt=video_post_prompt,
        buffer_hours=buffer_hours,
        loop_interval=loop_interval,
        max_batch_threads=max_batch_threads,
        max_model_passes=max_model_passes,
        story_fails_to_next=story_fails_to_next,
        words_per_second=words_per_second,
        good_samples_count=good_samples_count,
        video_fails_to_next=video_fails_to_next,
        approve_stories=approve_stories,
        approve_movies=approve_movies,
        deep_debugging=deep_debugging,
        workflow_state=workflow_state,
        target_id=target_id,
        aspect_ratio_x=aspect_ratio_x,
        aspect_ratio_y=aspect_ratio_y,
        dzen_target_id=dzen_target_id,
        dzen_publisher_id=dzen_publisher_id,
        vk_active=vk_active,
        dzen_active=dzen_active,
        rutube_target_id=rutube_target_id,
        rutube_person_id=rutube_person_id,
        rutube_active=rutube_active,
        vkvideo_target_id=vkvideo_target_id,
        vkvideo_club_id=vkvideo_club_id,
        vkvideo_active=vkvideo_active,
        publish_order=publish_order,
        app_version=APP_VERSION,
        nav_modules=_nav_modules("root"),
    ))
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


@bp.route("/production")
def production_page():
    if not is_authenticated():
        return redirect(url_for("web.login"))
    if not _has_slug("producer"):
        roles = _get_session_roles()
        other = [r for r in roles if r["slug"] != "producer"]
        if len(other) == 1:
            return redirect(_role_url(other[0]))
        if other:
            return redirect(url_for("web.select_module"))
        return redirect(url_for("web.login"))
    format_prompt       = cycle_config_get("format_prompt")
    text_prompt         = cycle_config_get("text_prompt")
    video_post_prompt   = cycle_config_get("video_post_prompt")
    story_fails_to_next = max(1, int(settings_get("story_fails_to_next", "3")))
    video_duration      = max(1, min(60, cycle_config_get("video_duration")))
    words_per_second    = cycle_config_get("words_per_second")
    good_samples_count  = max(1, int(cycle_config_get("good_samples_count") or 25))
    video_fails_to_next = max(1, int(settings_get("video_fails_to_next", "3")))
    approve_stories_prod = cycle_config_get("approve_stories")
    approve_movies_prod  = cycle_config_get("approve_movies")
    use_donor_prod       = environment.use_donor
    screenwriter_show_used = env_get("screenwriter_show_used", "0") == "1"
    screenwriter_only_good = env_get("screenwriter_only_good", "0") == "1"
    screenwriter_for_approval = env_get("screenwriter_for_approval", "0") == "1"
    screenwriter_only_pinned = env_get("screenwriter_only_pinned", "0") == "1"
    screenwriter_only_bad = env_get("screenwriter_only_bad", "0") == "1"
    autoplay_movie = env_get("producer_autoplay_movie", "0") == "1"
    _save_last_page()
    resp = make_response(render_template(
        "production.html",
        format_prompt=format_prompt,
        text_prompt=text_prompt,
        video_post_prompt=video_post_prompt,
        story_fails_to_next=story_fails_to_next,
        video_duration=video_duration,
        words_per_second=words_per_second,
        good_samples_count=good_samples_count,
        video_fails_to_next=video_fails_to_next,
        approve_stories=approve_stories_prod,
        approve_movies=approve_movies_prod,
        use_donor=use_donor_prod,
        app_version=APP_VERSION,
        nav_modules=_nav_modules("production"),
        screenwriter_show_used=screenwriter_show_used,
        screenwriter_only_good=screenwriter_only_good,
        screenwriter_for_approval=screenwriter_for_approval,
        screenwriter_only_pinned=screenwriter_only_pinned,
        screenwriter_only_bad=screenwriter_only_bad,
        autoplay_movie=autoplay_movie,
    ))
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


@bp.route("/save", methods=["POST"])
def save():
    if not is_authenticated():
        return redirect(url_for("web.login"))

    format_prompt_val = request.form.get("format_prompt")
    if format_prompt_val is not None:
        cycle_config_set("format_prompt", format_prompt_val)

    text_prompt = request.form.get("text_prompt", "").strip()
    active_tab = request.form.get("active_tab", "pipeline")
    if not text_prompt:
        if active_tab == "story":
            flash("Текстовый промпт не может быть пустым", "error")
            return redirect(url_for("web.root_page"))
    else:
        cycle_config_set("text_prompt", text_prompt)

    entries_lifetime_raw = request.form.get("entries_lifetime", "").strip()
    log_lifetime_raw     = request.form.get("log_lifetime",     "").strip()
    batch_lifetime_raw   = request.form.get("batch_lifetime",   "").strip()
    file_lifetime_raw    = request.form.get("file_lifetime",    "").strip()

    if entries_lifetime_raw or log_lifetime_raw or batch_lifetime_raw:
        el  = parse_long_lifetime(entries_lifetime_raw or settings_get("entries_lifetime", "30"), default=30)
        ll  = parse_long_lifetime(log_lifetime_raw     or settings_get("log_lifetime",     "365"))
        bl  = parse_batch_lifetime(batch_lifetime_raw     or settings_get("batch_lifetime",   "7"))
        if el <= ll <= bl:
            settings_set("entries_lifetime", str(el))
            settings_set("log_lifetime",     str(ll))
            settings_set("batch_lifetime",   str(bl))
        else:
            flash("Сроки хранения нарушают иерархию: подробный ≤ краткий ≤ история батчей", "error")

    if file_lifetime_raw:
        settings_set("file_lifetime", str(parse_file_lifetime(file_lifetime_raw)))

    if "notify_email" in request.form:
        settings_set("notify_email", request.form.get("notify_email", "").strip())
    if "notify_phone" in request.form:
        settings_set("notify_phone", request.form.get("notify_phone", "").strip())

    if any(k in request.form for k in ("vk_publish_story", "vk_publish_wall", "vk_publish_clip_story", "vk_publish_clip_wall")):
        vk_story_raw      = request.form.get("vk_publish_story",      "0")
        vk_wall_raw       = request.form.get("vk_publish_wall",       "0")
        vk_clip_story_raw = request.form.get("vk_publish_clip_story", "0")
        vk_clip_wall_raw  = request.form.get("vk_publish_clip_wall",  "0")
        db_update_target_publish_method_by_slug("vk", {
            "story":      1 if vk_story_raw      == "1" else 0,
            "wall":       1 if vk_wall_raw       == "1" else 0,
            "clip_story": 1 if vk_clip_story_raw == "1" else 0,
            "clip_wall":  1 if vk_clip_wall_raw  == "1" else 0,
        })

    ar_target_id = request.form.get("target_id", "").strip()
    ar_x_raw = request.form.get("aspect_ratio_x", "").strip()
    ar_y_raw = request.form.get("aspect_ratio_y", "").strip()
    if ar_target_id and ar_x_raw and ar_y_raw:
        try:
            ax, ay = int(ar_x_raw), int(ar_y_raw)
            if ax > 0 and ay > 0:
                db_update_target_aspect_ratio(ar_target_id, ax, ay)
        except (ValueError, TypeError):
            pass

    vid_dur_str = request.form.get("video_duration")
    if vid_dur_str is not None:
        try:
            vid_dur = max(1, min(60, int(vid_dur_str)))
        except (ValueError, TypeError):
            vid_dur = 6
        cycle_config_set("video_duration", vid_dur)

    video_post_prompt_val = request.form.get("video_post_prompt")
    if video_post_prompt_val is not None:
        cycle_config_set("video_post_prompt", video_post_prompt_val)

    buf_str = request.form.get("buffer_hours", "").strip()
    if buf_str:
        try:
            settings_set("buffer_hours", str(max(1, min(720, int(buf_str)))))
        except (ValueError, TypeError):
            pass

    loop_str = request.form.get("loop_interval", "").strip()
    if loop_str:
        try:
            settings_set("loop_interval", str(max(1, min(3600, int(loop_str)))))
        except (ValueError, TypeError):
            pass

    story_fails_str = request.form.get("story_fails_to_next", "").strip()
    if story_fails_str:
        try:
            settings_set("story_fails_to_next", str(max(1, int(story_fails_str))))
        except (ValueError, TypeError):
            pass

    video_fails_str = request.form.get("video_fails_to_next", "").strip()
    if video_fails_str:
        try:
            settings_set("video_fails_to_next", str(max(1, int(video_fails_str))))
        except (ValueError, TypeError):
            pass

    if "approve_stories" in request.form:
        cycle_config_set("approve_stories", request.form.get("approve_stories") == "1")

    if "producer_autoplay_movie" in request.form:
        env_set("producer_autoplay_movie", "1" if request.form.get("producer_autoplay_movie") == "1" else "0")

    max_threads_str = request.form.get("max_batch_threads", "").strip()
    if max_threads_str:
        try:
            settings_set("max_batch_threads", str(max(1, min(32, int(max_threads_str)))))
        except (ValueError, TypeError):
            pass

    max_model_passes_str = request.form.get("max_model_passes", "").strip()
    if max_model_passes_str:
        try:
            settings_set("max_model_passes", str(max(1, min(20, int(max_model_passes_str)))))
        except (ValueError, TypeError):
            pass

    return redirect(url_for("web.root_page") + f"?tab={active_tab}")


@bp.route("/select-module")
def select_module():
    if not is_authenticated():
        return redirect(url_for("web.login"))
    roles = _get_session_roles()
    if not roles:
        return redirect(url_for("web.login"))
    if len(roles) == 1:
        return redirect(_role_url(roles[0]))
    fresh_modules = db_get_role_modules()
    roles_display = [dict(r, url=_role_url(r), module=fresh_modules.get(r["slug"], r["module"])) for r in roles]
    return render_template("select_module.html", roles=roles_display)


@bp.route("/<slug>")
def module_page(slug):
    if slug in RESERVED_SLUGS:
        return redirect(url_for("web.login"))
    if not is_authenticated():
        return redirect(url_for("web.login"))
    if not _has_slug(slug):
        roles = _get_session_roles()
        non_root = [r for r in roles if r["slug"] != "root"]
        if len(non_root) > 1:
            return redirect(url_for("web.select_module"))
        return redirect(url_for("web.login"))
    from jinja2 import TemplateNotFound
    try:
        resp = make_response(render_template(f"{slug}.html", app_version=APP_VERSION, nav_modules=_nav_modules(slug)))
    except TemplateNotFound:
        return redirect(url_for("web.select_module"))
    _save_last_page()
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


@bp.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("web.login"))
