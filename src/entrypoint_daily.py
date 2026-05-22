"""Daily parser entrypoint — processes any unparsed days in a rolling window.

Uses RUN_ID='daily' so checkpoint accumulates. Skips already-parsed days.
"""
import os
import sys
import subprocess
from datetime import datetime, timedelta, timezone

def main():
    now = datetime.now(timezone.utc)
    window_start = (now - timedelta(days=60)).strftime("%Y-%m-%d")
    window_end = now.strftime("%Y-%m-%d")

    env = {
        **os.environ,
        "WINDOW_START": window_start,
        "WINDOW_END": window_end,
        "GCS_BUCKET": os.getenv("GCS_BUCKET", "sondre_brreg_data"),
    }

    print(f"[{datetime.now():%H:%M:%S}] daily parser: {window_start}→{window_end}", flush=True)
    r = subprocess.run([sys.executable, "/app/entrypoint.py"], env=env)
    print(f"[{datetime.now():%H:%M:%S}] parser exit={r.returncode}", flush=True)
    return r.returncode

if __name__ == "__main__":
    sys.exit(main())
