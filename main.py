import os
import time
import atexit
import threading
from datetime import timedelta
from flask import Flask, request

from db import init_db, run_upgrades, db_get, db_recover_story_generating, db_recover_video_generating, env_get, env_set
from log import db_log_root
from pipelines import planning, story, video, transcode, publish, cleanup
from routes.admin import bp as admin_bp
from routes.api import bp as api_bp
from utils.consts import FLASK_SECRET
from utils.limiter import limiter
import utils.workflow_state as wf_state

flask_app = Flask(__name__, static_folder="static", static_url_path="/static")
flask_app.secret_key = FLASK_SECRET
flask_app.permanent_session_lifetime = timedelta(days=7)
limiter.init_app(flask_app)

flask_app.register_blueprint(admin_bp)
flask_app.register_blueprint(api_bp)


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
    msg = f"[HTTP] {method} {path} | IP: {remote} | Headers: {headers}"
    if body:
        msg += f" | Body: {body}"
    print(msg)


def _wrap(module):
    def _runner():
        module.run()
        wf_state.wakeup_loop()
    return _runner


def main_loop():
    _threads = {
        'planning':  None,
        'story':     None,
        'video':     None,
        'transcode': None,
        'publish':   None,
        'cleanup':   None,
    }

    while True:
        interval = 5
        try:
            wf_state.wait_if_paused()

            interval = int(db_get('loop_interval', '5'))

            for name, module in [
                ('planning',  planning),
                ('story',     story),
                ('video',     video),
                ('transcode', transcode),
                ('publish',   publish),
                ('cleanup',   cleanup),
            ]:
                if _threads[name] is None or not _threads[name].is_alive():
                    _threads[name] = threading.Thread(target=_wrap(module), daemon=True)
                    _threads[name].start()

        except Exception as e:
            db_log_root(f"Ошибка главного цикла: {e}", status='error')
            print(f"[main_loop] Ошибка: {e}")

        wf_state.wait_for_wakeup(interval)


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
        db_recover_story_generating()
        db_recover_video_generating()
        if env_get('workflow_state', 'running') == 'pause':
            wf_state.set_paused()
        else:
            wf_state.set_running()
        db_log_root("Приложение запущено", status='info')
        atexit.register(_on_exit)
        t = threading.Thread(target=main_loop, daemon=True)
        t.start()


start_main_loop()

app = flask_app

if __name__ == "__main__":
    flask_app.run(host="0.0.0.0", port=5000, debug=False)
