import asyncio
import sys
import logging
from pathlib import Path
from shared.db import get_timescale


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("db.migrate")

async def apply_migrations():
    db = await get_timescale()
    # Read cannonical schema
    sql_path = Path(__file__).resolve().parent / "init.sql"
    if not sql_path.exists():
        logger.critical(f"Migration file not found at {sql_path}")
        sys.exit(1)

    with open(sql_path, "r", encoding="utf-8") as f:
        sql_script = f.read()

    try:
        await db.execute(sql_script)
        logger.info("Applied migration step successfully.")
    except Exception as e:
        logger.error(f"🚨 Migration failed | Error: {e}", exc_info=True)
        error_str = str(e).lower()
        if "already exists" not in error_str and "duplicate" not in error_str:
            raise

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())