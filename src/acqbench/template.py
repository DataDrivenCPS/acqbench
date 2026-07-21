"""Pre-warmed data_dir templates.

A cold acquirium boot spends most of its time building embedding indexes —
measured at ~48s for the graph index and ~286s for the QUDT index, out of ~380s
to /health. That work is cached under `data_dir/embedding_cache`, but
`recreate = true` deletes data_dir wholesale and so pays it again every time.

Rather than eat that per cell, we boot once per (ref, embedding config), let it
build the caches, snapshot the resulting data_dir, and then hand every cell a
copy to boot against with `recreate = false`. A cell still pays the ontology
parse (~27s, not cached by anything), but not the index build.

The snapshot is taken before any benchmark data is written, so a copy is a
clean slate for timeseries while still being warm for embeddings.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import dataclass
from pathlib import Path

from rich.console import Console

from .provision import Install
from .render import render_config, render_env
from .server import free_port, running_server
from .spec import Backend, Cell
from dataclasses import replace as _replace


@dataclass(frozen=True)
class TemplateKey:
    """What a template's contents actually depend on.

    Not the backend: data_dir holds the graph store and embedding cache, and a
    duckdb file that is empty at snapshot time. Not the topology or
    read_batch_size either — neither changes what startup builds.
    """

    ref_slug: str
    embedding_model: str
    ontology_sources: tuple[str, ...]

    @classmethod
    def for_cell(cls, cell: Cell) -> "TemplateKey":
        cfg = cell.config
        return cls(
            ref_slug=cell.ref.slug,
            embedding_model=cfg.embedding_model or "default",
            ontology_sources=cfg.ontology_sources,
        )

    @property
    def slug(self) -> str:
        h = hashlib.sha256(
            json.dumps(
                [self.ref_slug, self.embedding_model, list(self.ontology_sources)]
            ).encode()
        ).hexdigest()[:10]
        return f"{self.ref_slug}-{h}"


class TemplateCache:
    """Builds pre-warmed data_dirs on demand and reuses them."""

    def __init__(self, root: Path, fastembed_cache: Path, console: Console | None = None):
        self.root = root
        self.fastembed_cache = fastembed_cache
        self.console = console or Console()
        self._built: dict[TemplateKey, Path] = {}

    def get(self, cell: Cell, install: Install, *, startup_timeout: float) -> Path:
        key = TemplateKey.for_cell(cell)
        if key in self._built:
            return self._built[key]

        dest = self.root / key.slug
        stamp = dest / ".acqbench-template.json"
        if stamp.exists():
            self._built[key] = dest
            return dest

        path = self._build(cell, install, key, dest, startup_timeout=startup_timeout)
        self._built[key] = path
        return path

    def _build(
        self,
        cell: Cell,
        install: Install,
        key: TemplateKey,
        dest: Path,
        *,
        startup_timeout: float,
    ) -> Path:
        self.console.print(
            f"  [yellow]pre-warming template[/] {key.slug} "
            "[dim](one cold boot; builds embedding caches — several minutes)[/]"
        )
        if dest.exists():
            shutil.rmtree(dest)
        build_dir = self.root / f".building-{key.slug}"
        if build_dir.exists():
            shutil.rmtree(build_dir)
        build_dir.mkdir(parents=True)

        # Always pre-warm on duckdb: the template only carries data_dir, and the
        # duckdb backend needs no external service to reach a healthy state.
        warm_cell = Cell(
            ref=cell.ref,
            config=_replace(cell.config, backend=Backend.DUCKDB, driver_count=0, app_count=0),
            topology=cell.topology,
        )
        data_dir = build_dir / "data"
        config_path = render_config(
            warm_cell, build_dir, port=(port := free_port()), recreate=True, data_dir=data_dir
        )
        env = render_env(warm_cell, build_dir, fastembed_cache=self.fastembed_cache)

        with running_server(
            install, config_path, build_dir, port=port, env_extra=env,
            startup_timeout=startup_timeout,
        ) as srv:
            self.console.print(
                f"  [dim]template warm in {srv.startup_seconds:.0f}s; snapshotting[/]"
            )

        # Snapshot only after the server is down, so nothing is mid-write.
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(data_dir, dest)
        (dest / ".acqbench-template.json").write_text(
            json.dumps(
                {
                    "ref": install.ref_spec,
                    "resolved": install.resolved,
                    "embedding_model": key.embedding_model,
                    "ontology_sources": list(key.ontology_sources),
                },
                indent=2,
            )
        )
        shutil.rmtree(build_dir, ignore_errors=True)
        return dest


def seed_from_template(template: Path, data_dir: Path) -> None:
    """Give a cell its own warm copy of the template."""
    if data_dir.exists():
        shutil.rmtree(data_dir)
    data_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(template, data_dir)
    # The stamp is harness metadata; acquirium ignores it, but leaving it in a
    # live data_dir invites confusion when someone goes looking.
    stamp = data_dir / ".acqbench-template.json"
    if stamp.exists():
        stamp.unlink()
