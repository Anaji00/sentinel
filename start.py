import subprocess
import sys
import time
import os
from dotenv import load_dotenv

# 1. Manually load the .env variables so all services have the passwords
load_dotenv()

PROCESSES = [
    ("AIS", "python services/collector-ais/main.py"),
    ("ADSB", "python services/collector-adsb/main.py"),
    ("NEWS", "python services/collector-news/main.py"),
    ("ENRICH", "python services/enrichment/main.py"),
    ("CORREL", "python services/correlation/main.py"),
    ("REASON", "python services/reasoning/main.py"),
    ("ALERTS", "python services/alert_manager/main.py"),
    ("DLQ", "python services/dlq-worker/main.py"),
    ("API", "uvicorn services.api_gateway.routes.main:app --host 0.0.0.0 --port 8000")
]

procs = []
try:
    print("Initializing Sentinel Backend (Windows Native Mode)...")
    for name, cmd in PROCESSES:
        # 2. Inject the loaded environment variables into the subprocesses
        p = subprocess.Popen(cmd.split(), env=os.environ)
        procs.append((name, p))
    
    print("\nAll systems GO. Press CTRL+C to safely shutdown.\n")
    while True:
        time.sleep(1)

except KeyboardInterrupt:
    print("\nInitiating graceful shutdown...")
    for name, p in procs:
        p.kill() # Instantly terminates the process tree on Windows
    print("Sentinel offline.")
    sys.exit(0)