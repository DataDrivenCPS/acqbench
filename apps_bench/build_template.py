"""Build the pre-warmed data_dir template for one ref (run with its venv)."""

from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from serverctl import seed_template  # noqa: E402


def main() -> int:
    ref, root = sys.argv[1], Path(sys.argv[2])
    port_start = int(sys.argv[3]) if len(sys.argv) > 3 else 8811
    t0 = time.perf_counter()
    path = seed_template(
        Path(sys.executable), root / "templates" / ref, root / "work" / ref,
        port_start=port_start,
    )
    print(f"[{ref}] template at {path} in {time.perf_counter() - t0:.0f}s", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
