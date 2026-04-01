import os
from datetime import timezone, timedelta

ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "admin")
FLASK_SECRET   = os.environ.get("FLASK_SECRET", os.urandom(24).hex())

MSK = timezone(timedelta(hours=3))
