import os
import requests
from db import db_get
from log import write_log_entry


def _msk_ts() -> str:
    from datetime import datetime, timezone, timedelta
    msk = timezone(timedelta(hours=3))
    return datetime.now(msk).strftime("%d.%m.%Y %H:%M:%S")


def send_failure_email(message: str, log_entries=None, partial: bool = False):
    import smtplib
    from email.mime.text import MIMEText

    to_addr   = db_get("notify_email", "").strip()
    smtp_host = os.environ.get("SMTP_HOST", "").strip()
    smtp_user = os.environ.get("SMTP_USER", "").strip()
    smtp_pass = os.environ.get("SMTP_PASSWORD", "").strip()

    if not all([to_addr, smtp_host, smtp_user, smtp_pass]):
        write_log_entry(None, "[УВЕДОМЛЕНИЕ] Email не отправлен: не заданы SMTP-настройки или адрес", level='silent')
        return

    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_from = os.environ.get("SMTP_FROM", smtp_user)
    subject_prefix = "Частично" if partial else "Сбой"

    try:
        body = message
        if log_entries:
            lines = "\n".join(f"[{e['ts']}] {e['msg']}" for e in log_entries)
            body += f"\n\n--- Подробный лог ---\n{lines}"

        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = f"VIPilot: {subject_prefix.lower()} в пайплайне"
        msg["From"]    = smtp_from
        msg["To"]      = to_addr

        with smtplib.SMTP(smtp_host, smtp_port, timeout=15) as s:
            s.starttls()
            s.login(smtp_user, smtp_pass)
            s.send_message(msg)

    except Exception as e:
        write_log_entry(None, f"[УВЕДОМЛЕНИЕ] Ошибка отправки email: {e}", level='silent')


def send_failure_sms(message: str):
    phone      = db_get("notify_phone", "").strip()
    smsc_login = os.environ.get("SMSC_LOGIN", "").strip()
    smsc_pass  = os.environ.get("SMSC_PASS", "").strip()

    if not all([phone, smsc_login, smsc_pass]):
        return

    try:
        r = requests.get(
            "https://smsc.ru/sys/send.php",
            params={
                "login":   smsc_login,
                "psw":     smsc_pass,
                "phones":  phone,
                "mes":     message[:160],
                "charset": "utf-8",
                "fmt":     3,
            },
            timeout=10,
        )
        data = r.json()
        if data.get("error_code"):
            write_log_entry(None, f"[УВЕДОМЛЕНИЕ] SMSC ошибка: {data}", level='silent')
        else:
            write_log_entry(None, f"[УВЕДОМЛЕНИЕ] SMS отправлено на {phone}", level='silent')
    except Exception as e:
        write_log_entry(None, f"[УВЕДОМЛЕНИЕ] Ошибка отправки SMS: {e}", level='silent')


def notify_failure(reason: str, log_entries=None, partial: bool = False):
    try:
        prefix = "Частично" if partial else "Сбой"
        msg = f"{prefix} {_msk_ts()}: {reason}"
        write_log_entry(None, f"[УВЕДОМЛЕНИЕ] Отправляю уведомление [{prefix}]: {reason}", level='silent')
        send_failure_email(msg, log_entries=log_entries or [], partial=partial)
        send_failure_sms(msg)
    except Exception as e:
        write_log_entry(None, f"[УВЕДОМЛЕНИЕ] Ошибка notify_failure: {e}", level='warn')
