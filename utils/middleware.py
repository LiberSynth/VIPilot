from flask import request
from log.log import write_log_entry

_SILENT_GET_PATHS = {
    '/api/monitor',
    '/api/workflow/state',
    '/api/donors/count',
    '/healthz',
}


def log_request():
    method = request.method
    path = request.path
    if method == 'GET' and path in _SILENT_GET_PATHS:
        return
    full_path = request.full_path if request.query_string else path
    remote = request.remote_addr
    headers = dict(request.headers)
    body = ""
    if request.content_length and request.content_length > 0:
        try:
            body = request.get_data(as_text=True)
        except Exception:
            body = "<не удалось прочитать тело>"
    msg = f"[HTTP] {method} {full_path} | IP: {remote} | Headers: {headers}"
    if body:
        msg += f" | Body: {body}"
    write_log_entry(None, msg, level='silent')


def register_middleware(app):
    app.before_request(log_request)
