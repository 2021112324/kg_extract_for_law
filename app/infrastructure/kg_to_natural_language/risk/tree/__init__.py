"""Risk-tree resources and optional spreadsheet synchronization tools."""

from importlib import import_module
from typing import Any

__all__ = [
    "DEFAULT_TREE_PATH",
    "IndicatorDescriptionSyncError",
    "sync_indicator_descriptions",
]


def __getattr__(name: str) -> Any:
    if name not in __all__:
        raise AttributeError(name)
    module = import_module(f"{__name__}.indicator_description_sync")
    return getattr(module, name)
