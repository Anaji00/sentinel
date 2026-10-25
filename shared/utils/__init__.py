"""
shared/utils/__init__.py
Exposes utility packages directly from 'shared.utils'
"""

import importlib
from typing import Any

# Loaded on first use, not at package import. Importing quant_calc eagerly
# pulled numpy and scipy (~75 MB) into every service that imported any small
# helper from shared.utils -- a quiet-failure logger, a heartbeat. The
# attribute access below keeps `shared.utils.quant_calc` and
# `from shared.utils import metrics` working as before.
_LAZY = ("quant_calc", "metrics")


def __getattr__(name: str) -> Any:
    if name in _LAZY:
        return importlib.import_module(f"{__name__}.{name}")
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
