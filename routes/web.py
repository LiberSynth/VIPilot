import json
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
    db_get_target_by_name,
    db_get_user_by_login,
    db_get_role_modules,
)
import common.environment as environment
from utils.auth import is_authenticated
from utils.limiter import limiter
from utils.utils import (
    parse_batch_lifetime,
    parse_long_lifetime,
)

bp = Blueprint("web", __name__)

RESERVED_SLUGS = {"web", "save", "logout", "select-module", "favicon.ico", "icon-preview", "root", "production"}
failed_logins = {}

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

@bp.route("/icon-preview")
def icon_preview():
    return render_template("icon_preview.html")

@bp.route("/", methods=["GET", "POST"])
@limiter.limit("10 per minute", exempt_when=lambda: request.method != "POST")
def login():
    def _render_login():
        resp = make_response(render_template("login.html", error=error, error_text=error_text, no_roles=no_roles))
        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        resp.headers["Pragma"] = "no-cache"
        resp.headers["Expires"] = "0"
        return resp

    if is_authenticated():
        return _redirect_after_login()

    no_roles = request.args.get("reason") == "no_roles"
    error = False
    error_text = "Неверный логин или пароль"
    if request.method == "POST":
        login_val = request.form.get("login", "").strip()
        login_key = login_val.lower()
        password_val = request.form.get("password", "")
        now = time.time()
        state = failed_logins.get(login_key, {"count": 0, "blocked_until": 0})
        if state["blocked_until"] and state["blocked_until"] <= now:
            state = {"count": 0, "blocked_until": 0}
        if state["blocked_until"] > now:
            error = True
            no_roles = False
            error_text = "Слишком много попыток. Повторите через 5 минут."
            return _render_login()
        user = db_get_user_by_login(login_val)
        if user and user["password"] == password_val:
            failed_logins.pop(login_key, None)
            session["auth"] = True
            session["auth_ts"] = time.time()
            session["roles"] = user["roles"]
            session.permanent = True
            return _redirect_after_login()
        state["count"] += 1
        state["blocked_until"] = now + 300 if state["count"] >= 3 else 0
        failed_logins[login_key] = state
        error = True
        no_roles = False
        if state["blocked_until"] > now:
            error_text = "Слишком много попыток. Повторите через 5 минут."
        else:
            left = max(0, 3 - state["count"])
            error_text = f"Неверный логин или пароль. Осталось попыток: {left}"
    return _render_login()

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
    environment.refresh_environment()
    text_prompt     = cycle_config_get("text_prompt")
    format_prompt   = cycle_config_get("format_prompt")
    batch_lifetime     = parse_batch_lifetime(settings_get("batch_lifetime", "7"))
    log_lifetime       = parse_long_lifetime(settings_get("log_lifetime", "365"))
    entries_lifetime   = parse_long_lifetime(settings_get("entries_lifetime", "30"), default=30)
    publication_counter = int(env_get("publication_counter", "0") or "0")
    notify_email       = settings_get("notify_email", "")
    notify_phone       = settings_get("notify_phone", "")
    vk_target  = db_get_target_by_name("VKontakte")
    vk_active  = bool(vk_target.get("active")) if vk_target else False
    vk_target_id = vk_target["id"] if vk_target else None
    _vk_tc = vk_target.get("config") if vk_target else None
    vk_targets_config_json = json.dumps(_vk_tc, ensure_ascii=False, indent=2) if _vk_tc is not None else ""
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
    deep_debugging      = environment.deep_debugging

    workflow_state = env_get("workflow_state", "running")

    dzen_target     = db_get_target_by_name("Дзен")
    dzen_config     = dzen_target.get("config") or {} if dzen_target else {}
    dzen_publisher_id = dzen_config.get("publisher_id", "")
    dzen_target_id = dzen_target["id"] if dzen_target else None
    dzen_active    = bool(dzen_target.get("active")) if dzen_target else False
    _dzen_tc = dzen_target.get("config") if dzen_target else None
    dzen_targets_config_json = json.dumps(_dzen_tc, ensure_ascii=False, indent=2) if _dzen_tc is not None else ""

    rutube_target     = db_get_target_by_name("Rutube")
    rutube_config     = rutube_target.get("config") or {} if rutube_target else {}
    rutube_person_id  = rutube_config.get("person_id", "")
    rutube_target_id  = rutube_target["id"] if rutube_target else None
    rutube_active     = bool(rutube_target.get("active")) if rutube_target else False
    _rutube_tc = rutube_target.get("config") if rutube_target else None
    rutube_targets_config_json = json.dumps(_rutube_tc, ensure_ascii=False, indent=2) if _rutube_tc is not None else ""

    vkvideo_target    = db_get_target_by_name("VK Видео")
    vkvideo_config    = vkvideo_target.get("config") or {} if vkvideo_target else {}
    vkvideo_club_id   = vkvideo_config.get("club_id", "")
    vkvideo_target_id = vkvideo_target["id"] if vkvideo_target else None
    vkvideo_active    = bool(vkvideo_target.get("active")) if vkvideo_target else False
    _vkvideo_tc = vkvideo_target.get("config") if vkvideo_target else None
    vkvideo_targets_config_json = json.dumps(_vkvideo_tc, ensure_ascii=False, indent=2) if _vkvideo_tc is not None else ""

    active_targets  = db_get_active_targets()
    target          = active_targets[0] if active_targets else None
    target_id       = target["id"] if target else None
    _known_slugs    = {"vk", "dzen", "rutube", "vkvideo"}
    publish_order   = [
        t["slug"] for t in db_get_all_targets()
        if t["slug"] in _known_slugs
    ]

    browser_targets = {
        "dzen": {
            "slug": "dzen",
            "title": "Публикация в Дзен",
            "account": "Яндекса",
            "active": dzen_active,
            "target_id": dzen_target_id,
            "studio_url": f"https://dzen.ru/profile/editor/id/{dzen_publisher_id}/" if dzen_publisher_id else "",
            "config_json": dzen_targets_config_json,
        },
        "rutube": {
            "slug": "rutube",
            "title": "Публикация в Рутьюб",
            "account": "Рутьюба",
            "active": rutube_active,
            "target_id": rutube_target_id,
            "studio_url": "https://studio.rutube.ru/" if rutube_person_id else "",
            "config_json": rutube_targets_config_json,
        },
        "vkvideo": {
            "slug": "vkvideo",
            "title": "Публикация в VK Видео",
            "account": "VK",
            "active": vkvideo_active,
            "target_id": vkvideo_target_id,
            "studio_url": f"https://cabinet.vkvideo.ru/dashboard/@club{vkvideo_club_id}" if vkvideo_club_id else "",
            "config_json": vkvideo_targets_config_json,
        },
    }

    _save_last_page()
    resp = make_response(render_template(
        "root.html",
        text_prompt=text_prompt,
        format_prompt=format_prompt,
        batch_lifetime=batch_lifetime,
        log_lifetime=log_lifetime,
        entries_lifetime=entries_lifetime,
        notify_email=notify_email,
        notify_phone=notify_phone,
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
        deep_debugging=deep_debugging,
        workflow_state=workflow_state,
        target_id=target_id,
        vk_target_id=vk_target_id,
        vk_targets_config_json=vk_targets_config_json,
        dzen_target_id=dzen_target_id,
        dzen_publisher_id=dzen_publisher_id,
        dzen_targets_config_json=dzen_targets_config_json,
        vk_active=vk_active,
        dzen_active=dzen_active,
        rutube_target_id=rutube_target_id,
        rutube_person_id=rutube_person_id,
        rutube_active=rutube_active,
        rutube_targets_config_json=rutube_targets_config_json,
        vkvideo_target_id=vkvideo_target_id,
        vkvideo_club_id=vkvideo_club_id,
        vkvideo_active=vkvideo_active,
        vkvideo_targets_config_json=vkvideo_targets_config_json,
        publish_order=publish_order,
        browser_targets=browser_targets,
        publication_counter=publication_counter,
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
    screenwriter_show_used = env_get("screenwriter_show_used", "0") == "1"
    screenwriter_only_good = env_get("screenwriter_only_good", "0") == "1"
    screenwriter_for_approval = env_get("screenwriter_for_approval", "0") == "1"
    screenwriter_only_pinned = env_get("screenwriter_only_pinned", "0") == "1"
    screenwriter_only_bad = env_get("screenwriter_only_bad", "0") == "1"
    autoplay_movie = env_get("producer_autoplay_movie", "0") == "1"
    director_filter_for_approval = env_get("director_filter_for_approval", "0") == "1"
    director_filter_only_good = env_get("director_filter_only_good", "0") == "1"
    director_filter_show_published = env_get("director_filter_show_published", "1") == "1"
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
        app_version=APP_VERSION,
        nav_modules=_nav_modules("production"),
        screenwriter_show_used=screenwriter_show_used,
        screenwriter_only_good=screenwriter_only_good,
        screenwriter_for_approval=screenwriter_for_approval,
        screenwriter_only_pinned=screenwriter_only_pinned,
        screenwriter_only_bad=screenwriter_only_bad,
        autoplay_movie=autoplay_movie,
        director_filter_for_approval=director_filter_for_approval,
        director_filter_only_good=director_filter_only_good,
        director_filter_show_published=director_filter_show_published,
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

    if "notify_email" in request.form:
        settings_set("notify_email", request.form.get("notify_email", "").strip())
    if "notify_phone" in request.form:
        settings_set("notify_phone", request.form.get("notify_phone", "").strip())

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

    environment.refresh_environment()
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
