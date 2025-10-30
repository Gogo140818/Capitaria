import os
from datetime import datetime, timezone

STATE_FILE = "data/last_sync.txt"

def get_last_sync_time():
    """Lee la última fecha de sincronización guardada."""
    if not os.path.exists(STATE_FILE):
        return None
    with open(STATE_FILE, "r") as f:
        timestamp = f.read().strip()
        return datetime.fromisoformat(timestamp) if timestamp else None

def update_last_sync_time():
    """Guarda la fecha y hora actual como última sincronización."""
    os.makedirs("data", exist_ok=True)
    with open(STATE_FILE, "w") as f:
        now = datetime.now(timezone.utc).isoformat()
        f.write(now)
    print(f"🕒 Última sincronización actualizada a {now}")
