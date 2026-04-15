import os
import time

from log.log import write_log_entry


def loop():
    import requests as req
    domain = os.environ.get('REPLIT_DOMAINS', '').split(',')[0].strip()
    if not domain:
        return
    url = f"https://{domain}/healthz"
    write_log_entry(None, f"[keepalive] Запущен → {url}", level='silent')
    while True:
        time.sleep(4 * 60)
        try:
            req.get(url, timeout=10, verify=False)
        except Exception as e:
            write_log_entry(None, f"[keepalive] Ошибка: {e}", level='silent')
