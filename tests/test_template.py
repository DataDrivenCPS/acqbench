from __future__ import annotations

from acqbench.spec import Backend, Cell, Ref, ServerConfig, Topology
from acqbench.template import TemplateKey, seed_from_template


def _cell(**cfg_kw):
    return Cell(Ref.parse("pypi:0.3.1"), ServerConfig(**cfg_kw), Topology.SERVER)


def test_template_is_shared_across_axes_that_dont_change_startup():
    # data_dir holds the graph store and embedding cache. Neither the backend,
    # the read batch size, nor the topology changes what a boot builds, so
    # these must share one (expensive) template.
    a = TemplateKey.for_cell(_cell(backend=Backend.DUCKDB, read_batch_size=1000))
    b = TemplateKey.for_cell(_cell(backend=Backend.TIMESCALE, read_batch_size=50_000))
    assert a == b


def test_template_differs_when_embedding_model_differs():
    a = TemplateKey.for_cell(_cell())
    b = TemplateKey.for_cell(_cell(embedding_model="BAAI/bge-base-en-v1.5"))
    assert a != b and a.slug != b.slug


def test_template_differs_when_ontologies_differ():
    a = TemplateKey.for_cell(_cell())
    b = TemplateKey.for_cell(_cell(ontology_sources=("./extra.ttl",)))
    assert a != b


def test_template_differs_per_ref():
    a = TemplateKey.for_cell(Cell(Ref.parse("pypi:0.3.1"), ServerConfig(), Topology.SERVER))
    b = TemplateKey.for_cell(Cell(Ref.parse("git:main"), ServerConfig(), Topology.SERVER))
    assert a != b


def test_template_slug_is_filesystem_safe():
    slug = TemplateKey.for_cell(
        Cell(Ref.parse("git:feature/x"), ServerConfig(), Topology.SERVER)
    ).slug
    assert "/" not in slug


def test_seed_copies_and_strips_harness_metadata(tmp_path):
    template = tmp_path / "tpl"
    (template / "embedding_cache").mkdir(parents=True)
    (template / "embedding_cache" / "graph.npz").write_text("cached")
    (template / ".acqbench-template.json").write_text("{}")

    dest = tmp_path / "cell" / "data"
    seed_from_template(template, dest)

    assert (dest / "embedding_cache" / "graph.npz").read_text() == "cached"
    assert not (dest / ".acqbench-template.json").exists()


def test_seed_replaces_existing_data(tmp_path):
    template = tmp_path / "tpl"
    template.mkdir()
    (template / "fresh.txt").write_text("new")

    dest = tmp_path / "data"
    dest.mkdir()
    (dest / "stale.txt").write_text("old")

    seed_from_template(template, dest)
    assert (dest / "fresh.txt").exists()
    assert not (dest / "stale.txt").exists()  # a cell must not inherit prior state
