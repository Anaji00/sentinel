# shared/db/bootstrap.py

import os
import logging
from pathlib import Path
from shared.db import get_timescale

logger = logging.getLogger("db.bootstrap")

async def bootstrap_database():
    """
    Idempotent Application-Level Schema Migration.
    Reads init.sql and executes it directly against the DB.
    """
    db = await get_timescale()
    
    # Locate the init.sql file relative to this script
    sql_path = Path(__file__).resolve().parent / "init.sql"
    
    if not sql_path.exists():
        logger.error(f"FATAL: Schema file not found at {sql_path}")
        return

    logger.info("Verifying and applying database schema migrations...")
    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()

    try:
        # Execute the entire SQL block
        await db.execute(sql)
        logger.info("✅ Database schema successfully verified and initialized.")
    except Exception as e:
        logger.error(f"🚨 Schema initialization failed: {e}")
        logger.error("If the error mentions 'vector', comment out the vector extension in init.sql.")
        raise