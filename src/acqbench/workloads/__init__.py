"""Workload registry.

Importing this package registers every built-in workload as a side effect, so
`available()` and `get()` see them without the caller importing each module.
"""

from .base import Context, Workload, available, get, register

# Import for registration side effects.
from . import startup  # noqa: F401
from . import write  # noqa: F401
from . import read  # noqa: F401
from . import graph  # noqa: F401
from . import query  # noqa: F401
from . import ingest_query  # noqa: F401
from . import driver_tick  # noqa: F401
from . import app_scale  # noqa: F401

__all__ = ["Context", "Workload", "available", "get", "register"]
