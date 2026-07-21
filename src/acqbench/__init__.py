"""acqbench — a benchmark suite for Acquirium.

Provisions acquirium at arbitrary refs (PyPI releases, GitHub branches, local
working trees), runs each one across a matrix of server configs and component
topologies, and reports per-component performance and regressions.

The harness never imports acquirium: it drives a server running in a separate
venv over raw HTTP, so one copy of this code can benchmark every version.
"""

__version__ = "0.1.0"
