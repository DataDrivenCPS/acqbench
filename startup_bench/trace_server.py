"""Launch `acquirium server` with a span tracer attached.

Replicates `acquirium.cli.server_cmd` (config -> env, uvicorn.run) but first
wraps every function/method in acquirium's startup modules plus selected
third-party entry points (rdflib, ontoenv, fastembed, onnxruntime, duckdb,
ray, uvicorn) so each call becomes a span in $TRACE_DIR/spans.jsonl.

All timestamps are CLOCK_MONOTONIC ns, comparable with the runner process.
Functions called more than PER_NAME_LIMIT times stop emitting individual
spans; their calls are still summed into aggregates.json at exit.
"""
from __future__ import annotations

import time

T_INTERP = time.monotonic_ns()

import atexit
import builtins
import functools
import inspect
import json
import logging
import os
import sys
import threading
from pathlib import Path

TRACE_DIR = Path(os.environ["TRACE_DIR"])
TRACE_DIR.mkdir(parents=True, exist_ok=True)
PER_NAME_LIMIT = 300

_out = open(TRACE_DIR / "spans.jsonl", "w", buffering=1)
_wlock = threading.Lock()
_tls = threading.local()
_ids = iter(range(1, 10**12))
_counts: dict[str, int] = {}
_agg: dict[str, list] = {}  # name -> [calls, total_ns, self_ns]


def _emit(rec: dict) -> None:
    with _wlock:
        _out.write(json.dumps(rec) + "\n")


def event(name: str, **kw) -> None:
    _emit({"ev": name, "t": time.monotonic_ns(), "thread": threading.current_thread().name, **kw})


def _stack() -> list:
    s = getattr(_tls, "stack", None)
    if s is None:
        s = _tls.stack = []
    return s


class _Span:
    __slots__ = ("name", "id", "parent", "t0", "child_ns", "depth", "attrs")

    def __init__(self, name, attrs=None):
        self.name = name
        self.attrs = attrs

    def __enter__(self):
        st = _stack()
        self.parent = st[-1].id if st else 0
        self.depth = len(st)
        self.id = next(_ids)
        self.child_ns = 0
        st.append(self)
        self.t0 = time.monotonic_ns()
        return self

    def __exit__(self, et, ev, tb):
        t1 = time.monotonic_ns()
        st = _stack()
        st.pop()
        dur = t1 - self.t0
        if st:
            st[-1].child_ns += dur
        with _wlock:
            n = _counts[self.name] = _counts.get(self.name, 0) + 1
            a = _agg.setdefault(self.name, [0, 0, 0])
            a[0] += 1
            a[1] += dur
            a[2] += dur - self.child_ns
        if n <= PER_NAME_LIMIT:
            rec = {"id": self.id, "p": self.parent, "d": self.depth, "name": self.name,
                   "t0": self.t0, "t1": t1, "self": dur - self.child_ns,
                   "thread": threading.current_thread().name}
            if et is not None:
                rec["err"] = et.__name__
            if self.attrs:
                rec["a"] = self.attrs
            _emit(rec)
        return False


def span(name, **attrs):
    return _Span(name, attrs or None)


_KNOWN_IRIS = {
    "urn:nawi-water-ontology": "water",
    "http://data.ashrae.org/standard223/1.0/model/all": "s223",
    "https://qudt.org/vocab/unit": "qudt-unit",
    "https://qudt.org/vocab/quantitykind": "qudt-qk",
    "https://brickschema.org/schema/Brick/ref": "brick-ref",
}


def _short_iri(iri) -> str:
    s = str(iri).rstrip("/")
    return _KNOWN_IRIS.get(s) or s.rsplit("/", 1)[-1].rsplit("#", 1)[-1]


def _graph_iri(g) -> str:
    from rdflib import OWL, RDF
    return _short_iri(next(g.subjects(RDF.type, OWL.Ontology), "?"))


# name -> callable(*args) giving a per-call label, so repeated calls (one per
# ontology / matcher) stay separate steps in the report.
LABELS = {
    "_ontologies.load_bundled_graph": lambda filename, *a, **k: filename,
    "_ontologies.rename_ontology_iri": lambda g, frm, to, *a, **k: _short_iri(to),
    "graph_store._OntoenvOxigraphStore.add_graph": lambda self, iri, *a, **k: _short_iri(iri),
    "graph_store._OntoenvOxigraphStore.get_graph": lambda self, iri, *a, **k: _short_iri(iri),
    "graph_store._replace_named_graph": lambda ds, g, uri, *a, **k: _short_iri(uri),
    "graph_store.OxigraphGraphStore.named_graph": lambda self, iri, *a, **k: _short_iri(iri),
    "graph_store.OxigraphGraphStore._open_dataset": lambda path, *a, **k: Path(path).name,
    "graph_store.OxigraphGraphStore._store_has_graph": lambda self, iri, *a, **k: _short_iri(iri),
    "embedding_matcher.EmbeddingMatcher.__init__": lambda self, *a, **k: k.get("name", "?"),
    "embedding_matcher.EmbeddingMatcher.build_index": lambda self, *a, **k: self.name,
    "embedding_matcher.EmbeddingMatcher._embed": lambda self, texts, *a, **k: f"{self.name} n={len(texts)}",
    "embedding_matcher.EmbeddingMatcher._ensure_model": lambda self, *a, **k: self.name,
    "embedding_matcher.EmbeddingMatcher._try_load_cache": lambda self, *a, **k: self.name,
    "embedding_matcher.EmbeddingMatcher._save_cache": lambda self, *a, **k: self.name,
    "embedding_matcher.EmbeddingMatcher._concepts_hash": lambda concepts, *a, **k: f"n={len(concepts)}",
    "embedding_matcher.EmbeddingMatcher._build_surfaces_and_meta": lambda concepts, *a, **k: f"n={len(concepts)}",
    "embedding_matcher.EmbeddingMatcher._set_index": lambda self, *a, **k: self.name,
    "qudt_store.QUDTStore.extract_concepts": lambda g, rdf_type, *a, **k: _short_iri(rdf_type),
    "graph_concepts.extract_concepts": lambda *a, **k: (k.get("kind") or "graph"),
    "rdflib.Graph.parse": lambda self, source=None, *a, **k: Path(str(source)).name if source is not None else "data",
    "rdflib.Graph.serialize": lambda self, *a, **k: f"{len(self)} triples",
    "manager.Manager._mark_index_ready": lambda self, index, *a, **k: index,
    # local-runtime-init: native Oxigraph extraction
    "graph_store.select_values": lambda store, query, iris, *a, **k: _iris(iris),
    "graph_store.OxigraphGraphStore.select_in_graphs": lambda self, query, iris, *a, **k: _iris(iris),
    "graph_store.OxigraphGraphStore.copy_graphs": lambda self, iris, *a, **k: _iris(iris),
    "qudt_store.QUDTStore.concept_query": lambda rdf_type, *a, **k: _short_iri(rdf_type),
}


def _iris(iris) -> str:
    return "+".join(_short_iri(i) for i in iris) if isinstance(iris, (list, tuple)) else "?"


def _wrap(fn, name):
    if getattr(fn, "__traced__", False):
        return fn
    if inspect.iscoroutinefunction(fn):
        @functools.wraps(fn)
        async def aw(*a, **k):
            # async spans don't nest on the thread stack (the loop interleaves)
            t0 = time.monotonic_ns()
            try:
                return await fn(*a, **k)
            finally:
                _emit({"id": next(_ids), "p": 0, "d": 0, "name": name + " (async)",
                       "t0": t0, "t1": time.monotonic_ns(), "self": None,
                       "thread": threading.current_thread().name})
        aw.__traced__ = True
        return aw
    if inspect.isgeneratorfunction(fn) or inspect.isasyncgenfunction(fn):
        return fn

    labeler = LABELS.get(name)
    if labeler is None:
        @functools.wraps(fn)
        def w(*a, **k):
            with _Span(name):
                return fn(*a, **k)
    else:
        @functools.wraps(fn)
        def w(*a, **k):
            try:
                label = f"{name} [{labeler(*a, **k)}]"
            except Exception:
                label = name
            with _Span(label):
                return fn(*a, **k)
    w.__traced__ = True
    return w


def _wrap_class(cls, prefix, only=None, skip=()):
    for attr, raw in list(vars(cls).items()):
        if only is not None and attr not in only:
            continue
        if attr in skip or (attr.startswith("__") and attr != "__init__"):
            continue
        name = f"{prefix}.{attr}"
        try:
            if isinstance(raw, staticmethod):
                setattr(cls, attr, staticmethod(_wrap(raw.__func__, name)))
            elif isinstance(raw, classmethod):
                setattr(cls, attr, classmethod(_wrap(raw.__func__, name)))
            elif inspect.isfunction(raw):
                setattr(cls, attr, _wrap(raw, name))
        except (TypeError, AttributeError):
            pass


def _wrap_module(mod, short):
    """Wrap every function and class defined in *mod*; rebind in every module
    that imported the original object by name."""
    replaced = {}
    for attr, obj in list(vars(mod).items()):
        if getattr(obj, "__module__", None) != mod.__name__:
            continue
        if inspect.isfunction(obj):
            new = _wrap(obj, f"{short}.{attr}")
            if new is not obj:
                replaced[id(obj)] = new
                setattr(mod, attr, new)
        elif inspect.isclass(obj) and not hasattr(obj, "remote"):  # never touch Ray actor classes
            _wrap_class(obj, f"{short}.{attr}")
    if replaced:
        for m in list(sys.modules.values()):
            d = getattr(m, "__dict__", None)
            if not d or not str(getattr(m, "__name__", "")).startswith("acquirium"):
                continue
            for k, v in list(d.items()):
                r = replaced.get(id(v))
                if r is not None and inspect.isfunction(v):
                    d[k] = r


class _Proxy:
    """Times every method call on a Rust-backed object whose type can't be patched."""

    def __init__(self, target, prefix):
        object.__setattr__(self, "_t", target)
        object.__setattr__(self, "_p", prefix)

    def __getattr__(self, item):
        v = getattr(self._t, item)
        if callable(v) and not item.startswith("_"):
            name = f"{self._p}.{item}"

            def call(*a, **k):
                label = name
                try:
                    if item == "add" and a and hasattr(a[0], "subjects"):
                        label = f"{name} [{_graph_iri(a[0])}]"
                    elif item == "copy_graph" and a:
                        label = f"{name} [{_short_iri(a[0])}]"
                except Exception:
                    pass
                with _Span(label):
                    return v(*a, **k)
            return call
        return v


class _OntoEnvShim:
    def __init__(self, real):
        self._real = real

    def __getattr__(self, item):
        v = getattr(self._real, item)
        if item in ("connect", "create", "open", "adopt"):
            def ctor(*a, **k):
                with _Span(f"ontoenv.OntoEnv.{item}"):
                    return _Proxy(v(*a, **k), "ontoenv.env")
            return ctor
        return v


# ----------------------------------------------------------------- lazy imports
_PATCH_ON_IMPORT = {}
_real_import = builtins.__import__
_startup_imports_done = False


def _traced_import(name, globals=None, locals=None, fromlist=(), level=0):
    top = name.partition(".")[0]
    if level == 0 and _startup_imports_done and top not in sys.modules and not _stack_busy():
        _tls.importing = True
        try:
            with _Span(f"lazy import {top}"):
                mod = _real_import(name, globals, locals, fromlist, level)
        finally:
            _tls.importing = False
        for key, fn in list(_PATCH_ON_IMPORT.items()):
            if key in sys.modules:
                _PATCH_ON_IMPORT.pop(key)
                try:
                    fn()
                except Exception as exc:  # tracer must never break the server
                    event("patch_failed", target=key, error=repr(exc))
        return mod
    return _real_import(name, globals, locals, fromlist, level)


def _stack_busy():
    return getattr(_tls, "importing", False)


builtins.__import__ = _traced_import


def _patch_fastembed():
    from fastembed import TextEmbedding
    from fastembed.common import model_management, onnx_model
    _wrap_class(TextEmbedding, "fastembed.TextEmbedding", only={"__init__", "embed"})
    _wrap_class(model_management.ModelManagement, "fastembed.ModelManagement")
    for cls in vars(onnx_model).values():
        if inspect.isclass(cls) and cls.__module__ == onnx_model.__name__:
            _wrap_class(cls, f"fastembed.{cls.__name__}")
    import fastembed.text.onnx_embedding as oe
    _wrap_class(oe.OnnxTextEmbedding, "fastembed.OnnxTextEmbedding")
    try:
        import huggingface_hub
        huggingface_hub.snapshot_download = _wrap(huggingface_hub.snapshot_download,
                                                  "huggingface_hub.snapshot_download")
        model_management.snapshot_download = huggingface_hub.snapshot_download
    except Exception:
        pass
    import onnxruntime as ort
    orig = ort.InferenceSession.__init__
    ort.InferenceSession.__init__ = _wrap(orig, "onnxruntime.InferenceSession.__init__")
    event("patched", target="fastembed")


def _patch_duckdb():
    import duckdb
    duckdb.connect = _wrap(duckdb.connect, "duckdb.connect")
    event("patched", target="duckdb")


_PATCH_ON_IMPORT["fastembed"] = _patch_fastembed
_PATCH_ON_IMPORT["duckdb"] = _patch_duckdb


# ---------------------------------------------------------------- logging
class _LogCapture(logging.Handler):
    def emit(self, record):
        try:
            _emit({"log": record.getMessage()[:400], "t": time.monotonic_ns(),
                   "lg": record.name, "lvl": record.levelname,
                   "thread": record.threadName})
        except Exception:
            pass


def main() -> None:
    global _startup_imports_done
    event("interp_start", t_launch=int(os.environ.get("T_LAUNCH_NS", "0")), t_interp=T_INTERP)

    with span("import acquirium.cli"):
        from acquirium import cli
    with span("import uvicorn"):
        import uvicorn
    with span("import acquirium.Server.app"):
        import acquirium.Server.app as appmod
    with span("import (remaining startup modules)"):
        import rdflib
        import ray
        import ray.actor
        import ray._private.worker as rw
        import acquirium.Storage.graph_store as gs
        import acquirium.Storage as st
        import acquirium.Storage.duckdb_store as ds
        import acquirium.Server.manager as mg
        import acquirium.Server.config as cfgmod
        import acquirium.TextMatch.embedding_matcher as em
        import acquirium.TextMatch.qudt_store as qs
        import acquirium.TextMatch.resolver as rs
        # modules that only exist on some refs
        extra_modules = []
        for mod_name, short in (("acquirium.TextMatch.graph_concepts", "graph_concepts"),
                                ("acquirium.TextMatch.unit_key", "unit_key")):
            try:
                extra_modules.append((__import__(mod_name, fromlist=["_"]), short))
            except ImportError:
                pass
        import acquirium._ontologies as onto
        import acquirium.Drivers.supervisor as sup
        import acquirium.Server.insert_stats as ins
        import acquirium.Materialization.runtime as mr
        import acquirium.Storage.publication.revision as rev
    event("imports_done")

    with span("tracer: patch"):
        for mod, short in [
            (gs, "graph_store"), (st, "Storage"), (ds, "duckdb_store"), (mg, "manager"),
            (cfgmod, "config"), (em, "embedding_matcher"), (qs, "qudt_store"),
            (rs, "resolver"), (onto, "_ontologies"), (sup, "supervisor"),
            (ins, "insert_stats"), (mr, "materializer"), (rev, "publication"),
        ] + extra_modules:
            _wrap_module(mod, short)
        for fname in ("_deploy_config_apps", "_self_connect_cfg", "_start_config_drivers",
                      "_wait_until_healthy"):
            setattr(appmod, fname, _wrap(getattr(appmod, fname), f"app.{fname}"))
        gs.OntoEnv = _OntoEnvShim(gs.OntoEnv)
        _wrap_class(rdflib.Graph, "rdflib.Graph",
                    only={"parse", "serialize", "query", "__iadd__", "__add__", "__sub__", "update"})
        # ray: explicit init plus auto-init path, actor creation, blocking gets
        ray_init = _wrap(rw.init, "ray.init")
        rw.init = ray_init
        ray.init = ray_init
        ray.get = _wrap(ray.get, "ray.get")
        _wrap_class(ray.actor.ActorClass, "ray.ActorClass", only={"_remote", "options"})
        _wrap_class(uvicorn.Config, "uvicorn.Config", only={"load", "configure_logging"})
        _wrap_class(uvicorn.Server, "uvicorn.Server", only={"startup", "shutdown", "main_loop"})

        # lifespan: time startup (enter) and shutdown (exit) separately
        router = appmod.app.router
        orig_ctx = router.lifespan_context

        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def timed_lifespan(app):
            t0 = time.monotonic_ns()
            cm = orig_ctx(app)
            state = await cm.__aenter__()
            _emit({"id": next(_ids), "p": 0, "d": 0, "name": "lifespan startup",
                   "t0": t0, "t1": time.monotonic_ns(), "self": None, "thread": "MainThread"})
            event("app_ready")
            try:
                yield state
            finally:
                t2 = time.monotonic_ns()
                await cm.__aexit__(None, None, None)
                _emit({"id": next(_ids), "p": 0, "d": 0, "name": "lifespan shutdown",
                       "t0": t2, "t1": time.monotonic_ns(), "self": None, "thread": "MainThread"})
        router.lifespan_context = timed_lifespan
    _startup_imports_done = True

    logging.getLogger("acquirium").addHandler(_LogCapture())
    logging.getLogger("fastembed").addHandler(_LogCapture())

    config_path = Path(os.environ["BENCH_CONFIG"])
    os.environ["ACQUIRIUM_VERBOSE"] = "1"
    with span("cli: configure_logging + config"):
        from acquirium.internals._log import configure_logging
        configure_logging(verbose=True)
        if hasattr(cli, "_load_config"):  # main
            cfg = cli._load_config(config_path)
            cli._apply_server_env(cfg)
            os.environ.setdefault("ACQUIRIUM_CONFIG", str(config_path.resolve()))
        else:  # local-runtime-init: Server.config.load_config
            loaded = cfgmod.load_config(config_path)
            cfg = loaded.data
            loaded.apply_server_env()
            loaded.activate()
        port = int(cfg["server"]["port"])
        os.environ["ACQUIRIUM_SELF_PORT"] = str(port)
    import signal
    signal.signal(signal.SIGTERM, cli._sigterm_as_keyboard_interrupt)
    event("uvicorn_run")
    try:
        uvicorn.run(appmod.app, host="127.0.0.1", port=port, workers=1)
    finally:
        event("uvicorn_exit")
        with open(TRACE_DIR / "aggregates.json", "w") as f:
            json.dump({k: {"calls": v[0], "total_ns": v[1], "self_ns": v[2]}
                       for k, v in _agg.items()}, f)
        _out.flush()


if __name__ == "__main__":
    main()
