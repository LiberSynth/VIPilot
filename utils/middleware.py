from flask import request


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


def register_middleware(app):
    app.before_request(log_request)
