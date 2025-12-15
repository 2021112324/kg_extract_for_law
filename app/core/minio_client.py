"""
DEPRECATED: This module is deprecated and will be removed in a future version.
Use app.infrastructure.storage.object_storage instead.

This file now serves as a compatibility layer forwarding to the new architecture.
"""

import warnings

# Issue deprecation warning
warnings.warn(
    "app.core.minio_client is deprecated. Use app.infrastructure.storage.object_storage instead.",
    DeprecationWarning,
    stacklevel=2
)

# Import all functions from the compatibility module
from .minio_client_compat import *