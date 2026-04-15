from flask import request
from log.log import write_log_entry

_SILENT_GET_PATHS = {
    '/api/monitor',
    '/api/workflow/state',
    '/api/donors/count',
    '/healthz',
}

_SILENT_GET_PREFIXES = (
    '/api/batch/',
)


def log_request():
    method = request.method
    path = request.path
    if method == 'GET' and (
        path in _SILENT_GET_PATHS or
        path.startswith(_SILENT_GET_PREFIXES)
    ):
        return
    full_path = request.full_path if request.query_string else path
    remote = request.remote_addr
    msg = f"[HTTP] {method} {full_path} | IP: {remote}"
    write_log_entry(None, msg, level='silent')


def register_middleware(app):
    app.before_request(log_request)
