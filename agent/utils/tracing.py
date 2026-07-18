"""Per-graph LangSmith tracing-project routing for langgraph.json entrypoints."""

import contextlib
import os
from collections.abc import AsyncIterator, Awaitable, Callable

import langsmith as ls
from langgraph.graph.state import RunnableConfig
from langgraph.pregel import Pregel

# LANGSMITH_PROJECT is the base: tracing_context(project_name=...) overrides
# the ambient env var, so without this the env setting silently loses to the
# hardcoded defaults. Review traces keep their own project — named by
# LANGSMITH_REVIEW_PROJECT, else derived from the base with a -review suffix.
_ENV_PROJECT = os.getenv("LANGSMITH_PROJECT")
AGENT_TRACING_PROJECT = _ENV_PROJECT or "open-swe-agent"
REVIEW_TRACING_PROJECT = os.getenv("LANGSMITH_REVIEW_PROJECT") or (
    f"{_ENV_PROJECT}-review" if _ENV_PROJECT else "open-swe-review"
)


def traced_graph_factory(
    factory: Callable[[RunnableConfig], Awaitable[Pregel]],
    project_name: str,
) -> Callable[[RunnableConfig], contextlib.AbstractAsyncContextManager[Pregel]]:
    @contextlib.asynccontextmanager
    async def entrypoint(config: RunnableConfig) -> AsyncIterator[Pregel]:
        graph = await factory(config)
        with ls.tracing_context(project_name=project_name):
            yield graph

    return entrypoint
