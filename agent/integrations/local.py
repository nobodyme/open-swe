import os
import re

from deepagents.backends import LocalShellBackend
from deepagents.backends.protocol import ExecuteResponse

_GH_TOKEN_SENTINEL = re.compile(r"GH_TOKEN=dummy\b")


class GitHubTokenLocalShellBackend(LocalShellBackend):
    """`LocalShellBackend` that authenticates the ``GH_TOKEN=dummy`` sentinel.

    Prompts and tools hardcode ``GH_TOKEN=dummy gh ...``; on LangSmith
    sandboxes a GitHub proxy injects real credentials at the network layer, so
    the literal value never matters. Local sandboxes have no proxy, so the real
    token is substituted for the sentinel here — at the execution boundary —
    keeping it out of prompts, tool messages, logs, and checkpoints. Output is
    scrubbed back to ``dummy`` in case a command echoes its environment.
    Without a token, commands run unchanged (unauthenticated).
    """

    _github_token: str | None = None

    def set_github_token(self, token: str | None) -> None:
        self._github_token = token

    def execute(self, command: str, *, timeout: int | None = None) -> ExecuteResponse:
        token = self._github_token
        if token and isinstance(command, str):
            command = _GH_TOKEN_SENTINEL.sub(lambda _: f"GH_TOKEN='{token}'", command)
        response = super().execute(command, timeout=timeout)
        if token and token in response.output:
            response = ExecuteResponse(
                output=response.output.replace(token, "dummy"),
                exit_code=response.exit_code,
                truncated=response.truncated,
            )
        return response


def create_local_sandbox(sandbox_id: str | None = None):
    """Create a local shell sandbox with no isolation.

    WARNING: This runs commands directly on the host machine with no sandboxing.
    Only use for local development with human-in-the-loop enabled.

    The root directory defaults to ~/.open-swe/sandboxes and can be overridden
    via the LOCAL_SANDBOX_ROOT_DIR environment variable. It is created if it
    does not already exist. It must NOT live inside the server's own repo:
    under `make dev` the agent's clones/edits would land in uvicorn's --reload
    watch tree and every run would restart (and orphan) itself mid-flight.

    Args:
        sandbox_id: Ignored for local sandboxes; accepted for interface compatibility.

    Returns:
        GitHubTokenLocalShellBackend instance implementing SandboxBackendProtocol.
    """
    root_dir = os.getenv("LOCAL_SANDBOX_ROOT_DIR") or os.path.expanduser("~/.open-swe/sandboxes")
    os.makedirs(root_dir, exist_ok=True)

    return GitHubTokenLocalShellBackend(
        root_dir=root_dir,
        virtual_mode=True,
        inherit_env=True,
    )
