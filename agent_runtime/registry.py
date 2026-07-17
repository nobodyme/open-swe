"""Graph registry: resolve assistant ids to compiled graphs.

Reads the ``AGENT_RUNTIME_CONFIG`` file (langgraph.json shape): the ``graphs``
mapping (``module:attr`` entrypoints — compiled Pregel objects or factories,
sync or async, exactly the shapes the app registers today) and ``http.app``
(the webapp the runtime mounts at ``/`` per D1).

Checkpointer injection happens per run in the executor via
``config["configurable"][CONFIG_KEY_CHECKPOINTER]`` — the same mechanism
langgraph-api uses, honored by MIT langgraph's ``Pregel._defaults``. The
registry's job for state is the STORE: every resolved graph gets
``app.state.store`` attached so in-graph ``get_store()`` resolves to the same
instance the Store router serves (MIGRATION §1).
"""

from __future__ import annotations

import importlib
import importlib.util
import inspect
import json
import sys
from pathlib import Path
from typing import Any

from langgraph.pregel import Pregel
from langgraph.store.base import BaseStore


class GraphRegistry:
    def __init__(
        self,
        config_path: Path,
        *,
        store: BaseStore | None = None,
        checkpointer: Any | None = None,
    ) -> None:
        raw = json.loads(config_path.read_text(encoding="utf-8"))
        graphs = raw.get("graphs")
        if not isinstance(graphs, dict) or not graphs:
            raise RuntimeError(f"{config_path} declares no graphs")
        self._entrypoints: dict[str, str] = {str(k): str(v) for k, v in graphs.items()}
        http = raw.get("http") or {}
        self.webapp_path: str | None = http.get("app") if isinstance(http, dict) else None
        self.env_file: str | None = raw.get("env") if isinstance(raw.get("env"), str) else None
        self.store = store
        self.checkpointer = checkpointer
        self._loaded: dict[str, Any] = {}

    @property
    def assistant_ids(self) -> list[str]:
        return list(self._entrypoints)

    def _load_entrypoint(self, assistant_id: str) -> Any:
        if assistant_id in self._loaded:
            return self._loaded[assistant_id]
        spec = self._entrypoints.get(assistant_id)
        if spec is None:
            raise KeyError(f"unknown assistant_id: {assistant_id!r}")
        module_path, _, attr = spec.partition(":")
        # Path-style entrypoints ("./pkg/mod.py:attr") load by file location —
        # the shape the contract config uses.
        if module_path.endswith(".py"):
            file_path = Path(module_path).resolve()
            # Sibling imports (e.g. the e2e entrypoint's `import patches`)
            # must resolve regardless of whether the webapp loaded first.
            parent = str(file_path.parent)
            if parent not in sys.path:
                sys.path.insert(0, parent)
            module_name = f"_agent_runtime_graph_{assistant_id}"
            module_spec = importlib.util.spec_from_file_location(module_name, file_path)
            if module_spec is None or module_spec.loader is None:
                raise RuntimeError(f"cannot load graph module from {module_path}")
            module = importlib.util.module_from_spec(module_spec)
            module_spec.loader.exec_module(module)
        else:
            module = importlib.import_module(module_path)
        entry = getattr(module, attr)
        self._loaded[assistant_id] = entry
        return entry

    async def resolve_managed(
        self, assistant_id: str, config: dict[str, Any] | None = None
    ) -> tuple[Pregel, Any | None]:
        """Resolve to (graph, context-manager-or-None).

        Entrypoints come in every shape the app registers today: compiled
        Pregel, sync/async factory, and @asynccontextmanager factory (the
        tracing wrappers in agent/graphs/*). For CM factories the caller owns
        the exit — the executor holds it open for the run's duration.
        """
        entry = self._load_entrypoint(assistant_id)
        graph: Any = entry
        cm: Any | None = None
        if callable(entry) and not isinstance(entry, Pregel):
            result = entry(config or {})
            if hasattr(result, "__aenter__"):
                cm = result
                graph = await cm.__aenter__()
            elif inspect.isawaitable(result):
                graph = await result
            else:
                graph = result
        if not isinstance(graph, Pregel):
            if cm is not None:
                await cm.__aexit__(None, None, None)
            raise TypeError(
                f"entrypoint for {assistant_id!r} resolved to {type(graph).__name__}, "
                "expected a compiled graph"
            )
        if self.store is not None and graph.store is None:
            graph.store = self.store
        # Attribute attachment, NOT config-key injection: MIT langgraph's
        # DeltaChannel write-replay in aget_state only works with a
        # compiled-in checkpointer (CONFIG_KEY_CHECKPOINTER injection leaves
        # delta channels — deepagents' messages — empty on state reads;
        # verified empirically against langgraph 1.x).
        if self.checkpointer is not None and graph.checkpointer is None:
            graph.checkpointer = self.checkpointer
        return graph, cm

    async def resolve(self, assistant_id: str, config: dict[str, Any] | None = None) -> Pregel:
        """Resolve to a compiled graph; context-managed factories are entered
        and intentionally left to the GC after use (state reads only — runs
        go through resolve_managed)."""
        graph, _cm = await self.resolve_managed(assistant_id, config)
        return graph


def load_webapp(webapp_path: str | None) -> Any | None:
    """Import the http.app target (default agent.webapp:app) for the D1 mount.

    Accepts both module form (``agent.webapp:app``) and file form
    (``./tests/e2e/harness.py:app`` — the e2e config's shape)."""
    if not webapp_path:
        return None
    module_path, _, attr = webapp_path.partition(":")
    if module_path.endswith(".py"):
        file_path = Path(module_path).resolve()
        # Match langgraph dev: the entry file's directory is importable so
        # sibling modules (e2e_env, patches, fakes) resolve.
        parent = str(file_path.parent)
        if parent not in sys.path:
            sys.path.insert(0, parent)
        module_spec = importlib.util.spec_from_file_location("_agent_runtime_webapp", file_path)
        if module_spec is None or module_spec.loader is None:
            raise RuntimeError(f"cannot load webapp from {module_path}")
        module = importlib.util.module_from_spec(module_spec)
        module_spec.loader.exec_module(module)
    else:
        module = importlib.import_module(module_path)
    return getattr(module, attr)
