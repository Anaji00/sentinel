import sys
import logging
from pathlib import Path
from shared.db import get_timescale


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("db.migrate")

def apply_migrations():
    db = get_timescale()
    # Read cannonical schema
    sql_path = Path(__file__).resolve().parent / "init.sql"
    with open(sql_path, "r", encoding="utf-8") as f:
        # Split by ';' to execute statements individually, bypassing param parsing
        commands = [cmd.strip() for cmd in f.read().split(';') if cmd.strip()]

    for cmd in commands:
        try:
            db.execute(cmd)
            logger.info("Applied migration step successfully.")
        except Exception as e:
            logger.error(f"Migration step failed at: {cmd[:50]}... | Error: {e}", exc_info=True)
            if "already exists" not in str(e).lower():
                raise

if __name__ == "__main__":
    apply_migrations()