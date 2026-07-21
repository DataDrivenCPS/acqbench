"""Workload contract and registry.

A workload is a named, parameterized measurement executed against a live server.
Workloads must be *identical* across cells — that is what makes the ref-to-ref
and topology-to-topology comparisons meaningful. Anything cell-specific belongs
in the config/topology layer, not in here.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from ..spec import Cell
from ..server import ServerHandle


@dataclass
class Context:
    """Everything a workload is allowed to know about its run."""

    cell: Cell
    server: ServerHandle
    workdir: Path
    repetition: int
    params: dict[str, Any]
    #: The provisioned install this cell is running. Present so a workload can
    #: exec a script with *this ref's* interpreter — which is the only way to
    #: exercise acquirium's own client API without the harness importing it.
    install: Any = None

    @property
    def base_url(self) -> str:
        return self.server.base_url

    @property
    def ref_python(self) -> Path:
        """The interpreter with this cell's acquirium installed."""
        if self.install is None:
            raise RuntimeError("no install on context; cannot exec in the ref's venv")
        return self.install.python


class Workload(ABC):
    """One measurement.

    Lifecycle per repetition: setup() -> run() -> teardown(). Only run() is
    timed; setup work (registering streams, generating data) must not leak into
    the measurement.
    """

    name: str = ""
    #: Whether this workload needs a server that was *just* started (cold).
    #: The runner keeps a warm server between workloads otherwise.
    requires_cold_server: bool = False

    def __init__(self, **params: Any):
        self.params = params

    def setup(self, ctx: Context) -> None:
        """Prepare state. Not timed."""

    @abstractmethod
    def run(self, ctx: Context) -> dict[str, Any]:
        """Perform the measurement and return its metrics."""

    def teardown(self, ctx: Context) -> None:
        """Release state. Not timed."""

    def describe(self) -> dict[str, Any]:
        return dict(self.params)


_REGISTRY: dict[str, Callable[..., Workload]] = {}


def register(name: str) -> Callable[[type[Workload]], type[Workload]]:
    def deco(cls: type[Workload]) -> type[Workload]:
        cls.name = name
        if name in _REGISTRY:
            raise ValueError(f"workload {name!r} is already registered")
        _REGISTRY[name] = cls
        return cls

    return deco


def get(name: str) -> Callable[..., Workload]:
    if name not in _REGISTRY:
        raise KeyError(
            f"unknown workload {name!r}; available: {', '.join(sorted(_REGISTRY))}"
        )
    return _REGISTRY[name]


def available() -> list[str]:
    return sorted(_REGISTRY)
