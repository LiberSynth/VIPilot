import os
import time


def loop():
    import requests as req
    domain = os.environ.get('REPLIT_DOMAINS', '').split(',')[0].strip()
    if not domain:
        return
    url = f"https://{domain}/healthz"
    print(f"[keepalive] Запущен → {url}")
    while True:
        time.sleep(4 * 60)
        try:
            req.get(url, timeout=10)
        except Exception as e:
            print(f"[keepalive] Ошибка: {e}")
