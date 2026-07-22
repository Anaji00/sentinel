import time
import subprocess
import json
import os
from datetime import datetime

LOG_DIR = r"C:\Users\najia\.gemini\antigravity-ide\brain\5833d14d-50e2-4dd2-8a72-db70f76af5b7\scratch"
LOG_FILE = os.path.join(LOG_DIR, "monitored_logs.json")

CONTAINERS = [
    "sentinel-agents",
    "sentinel-reasoning",
    "sentinel-macro",
    "sentinel-tradfi",
    "sentinel-ais",
    "sentinel-adsb",
    "sentinel-correlation",
    "sentinel-prediction",
    "sentinel-crypto",
    "sentinel-news",
    "sentinel-enrichment",
    "sentinel-cyber",
    "sentinel-radar",
    "sentinel-api_gateway",
    "sentinel-frontend",
    "sentinel-timescaledb",
    "sentinel-kafka",
    "sentinel-redis",
    "sentinel-qdrant",
    "sentinel-neo4j",
    "sentinel-ollama"
]

def capture_logs(iteration):
    os.makedirs(LOG_DIR, exist_ok=True)
    timestamp = datetime.now().isoformat()
    iteration_data = {
        "iteration": iteration,
        "timestamp": timestamp,
        "logs": {}
    }
    
    for container in CONTAINERS:
        try:
            res = subprocess.run(
                ["docker", "logs", "--tail", "50", container],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=10
            )
            stdout = res.stdout.strip()
            stderr = res.stderr.strip()
            combined = (stdout + "\n" + stderr).strip()
            iteration_data["logs"][container] = combined
        except Exception as e:
            iteration_data["logs"][container] = f"Error capturing logs: {str(e)}"
            
    existing = []
    if os.path.exists(LOG_FILE):
        try:
            with open(LOG_FILE, "r", encoding="utf-8", errors="replace") as f:
                existing = json.load(f)
        except Exception:
            existing = []
            
    existing.append(iteration_data)
    with open(LOG_FILE, "w", encoding="utf-8", errors="replace") as f:
        json.dump(existing, f, indent=2)

def main():
    print("Starting 10-minute log monitor (every 20 seconds, 30 iterations)...")
    for i in range(1, 31):
        print(f"Iteration {i}/30 at {datetime.now().strftime('%H:%M:%S')}")
        capture_logs(i)
        if i < 30:
            time.sleep(20)
    print("Log monitoring completed.")

if __name__ == "__main__":
    main()
