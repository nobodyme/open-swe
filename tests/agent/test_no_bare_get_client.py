"""Guard against ``langgraph_sdk.get_client`` calls that omit a real ``url=``.

``get_client(url=None)`` — and therefore any call without a non-None ``url``
keyword, since ``url`` defaults to None — mounts the Elastic-licensed
``langgraph_api`` package's in-process ASGI transport, silently reintroducing
the runtime dependency the FastAPI migration removes (docs/MIGRATION.md §1).
Every client must come from the URL-resolving helpers:
``agent.utils.thread_ops.langgraph_client`` or ``agent.dispatch.dispatch_client``.

The detector deliberately over-approximates: any call to a name bound to
``get_client`` (direct, ``from langgraph_sdk import get_client as gc``,
``langgraph_sdk.get_client``, module aliases, simple ``alias = get_client``
rebinding) is a violation unless it passes ``url=<non-None>``.
"""

from __future__ import annotations

import ast
from pathlib import Path

AGENT_ROOT = Path(__file__).resolve().parents[2] / "agent"

# The two helpers are the only modules allowed to call langgraph_sdk.get_client
# directly (and they must always pass url=<non-None> — see the second test).
ALLOWED_FILES = {
    AGENT_ROOT / "utils" / "thread_ops.py",
    AGENT_ROOT / "dispatch.py",
}


def _has_real_url_kwarg(call: ast.Call) -> bool:
    for keyword in call.keywords:
        if keyword.arg == "url":
            return not (isinstance(keyword.value, ast.Constant) and keyword.value.value is None)
        if keyword.arg is None:
            # **kwargs — can't prove url is absent; give it the benefit of
            # the doubt only in the allowlisted helpers, nowhere else.
            return False
    return False


def offending_get_client_calls(source: str, filename: str = "<string>") -> list[int]:
    """Line numbers of get_client-style calls lacking a real ``url=``."""
    tree = ast.parse(source, filename=filename)

    tracked_names = {"get_client"}
    module_aliases = {"langgraph_sdk"}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "langgraph_sdk":
            for alias in node.names:
                if alias.name == "get_client":
                    tracked_names.add(alias.asname or alias.name)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "langgraph_sdk":
                    module_aliases.add(alias.asname or alias.name)
    # Simple rebinding: ``mk = get_client`` (or of any tracked alias).
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Assign)
            and isinstance(node.value, ast.Name)
            and node.value.id in tracked_names
        ):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    tracked_names.add(target.id)

    lines: list[int] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        is_tracked = (isinstance(func, ast.Name) and func.id in tracked_names) or (
            isinstance(func, ast.Attribute)
            and func.attr == "get_client"
            and (not isinstance(func.value, ast.Name) or func.value.id in module_aliases)
        )
        if is_tracked and not _has_real_url_kwarg(node):
            lines.append(node.lineno)
    return lines


def _scan(path: Path) -> list[int]:
    return offending_get_client_calls(path.read_text(), filename=str(path))


def test_no_bare_get_client_calls_in_agent() -> None:
    offenders: dict[str, list[int]] = {}
    for path in sorted(AGENT_ROOT.rglob("*.py")):
        if path in ALLOWED_FILES:
            continue
        lines = _scan(path)
        if lines:
            offenders[str(path.relative_to(AGENT_ROOT.parent))] = lines
    assert not offenders, (
        "get_client calls without url=<non-None> found (use "
        f"langgraph_client()/dispatch_client() instead): {offenders}"
    )


def test_allowed_helpers_always_pass_url() -> None:
    for path in ALLOWED_FILES:
        assert not _scan(path), f"{path} must always call get_client(url=<non-None>)"


def test_detector_catches_known_bypass_forms() -> None:
    """The detector itself is under test: each historical bypass form must
    be caught, and the sanctioned form must pass."""
    bypasses = [
        "from langgraph_sdk import get_client\nget_client()",
        "from langgraph_sdk import get_client as gc\ngc()",
        "from langgraph_sdk import get_client\nget_client(api_key='x')",
        "from langgraph_sdk import get_client\nget_client(url=None)",
        "from langgraph_sdk import get_client\nmk = get_client\nmk()",
        "import langgraph_sdk\nlanggraph_sdk.get_client()",
        "import langgraph_sdk as lgs\nlgs.get_client()",
        "import langgraph_sdk\nlanggraph_sdk.get_client(timeout=5)",
    ]
    for source in bypasses:
        assert offending_get_client_calls(source), f"bypass not caught: {source!r}"

    sanctioned = [
        "from langgraph_sdk import get_client\nget_client(url='http://x')",
        "from langgraph_sdk import get_client\nget_client(url=resolve_url())",
        "client.threads.get_client_info()",  # unrelated attribute name
    ]
    for source in sanctioned:
        assert not offending_get_client_calls(source), f"false positive: {source!r}"
