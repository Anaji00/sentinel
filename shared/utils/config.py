import yaml
import logging
from pathlib import Path

logger = logging.getLogger("sentinel.config")

class SentinelConfig:
    _config = None

    @classmethod
    def load(cls):
        if cls._config is None:
            config_path = Path(__file__).resolve().parents[2] / "sentinel_config.yaml"
            try:
                with open(config_path, "r") as f:
                    cls._config = yaml.safe_load(f)
                logger.info(f"Config loaded from {config_path}")
            except Exception as e:
                logger.error(f"Error loading config: {e}")
                cls._config = {}
        return cls._config
    
# Export a ready-to-use dictionary
config = SentinelConfig.load()