from __future__ import annotations

from collections.abc import Generator
from datetime import UTC, datetime, timedelta
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from deepagents.backends import LocalShellBackend
from deepagents.backends.protocol import ExecuteResponse

import agent.integrations.local as local_mod
from agent.integrations.local import GitHubTokenLocalShellBackend
from agent.utils import github_proxy


class _StubLocalShellBackend:
    def __init__(self, *, root_dir, virtual_mode, inherit_env):
        self.root_dir = root_dir
        self.virtual_mode = virtual_mode
        self.inherit_env = inherit_env


@pytest.fixture(autouse=True)
def _clear_proxy_token_state() -> Generator[None, None, None]:
    github_proxy._PROXY_TOKEN_EXPIRY.clear()
    yield
    github_proxy._PROXY_TOKEN_EXPIRY.clear()


def test_create_local_sandbox_creates_missing_root_dir(monkeypatch, tmp_path):
    root = tmp_path / "nested" / "openswe-sandbox"
    monkeypatch.setenv("LOCAL_SANDBOX_ROOT_DIR", str(root))
    monkeypatch.setattr(local_mod, "GitHubTokenLocalShellBackend", _StubLocalShellBackend)

    backend = local_mod.create_local_sandbox()

    assert root.is_dir()
    stub = cast(_StubLocalShellBackend, backend)
    assert stub.root_dir == str(root)
    assert stub.virtual_mode is True
    assert stub.inherit_env is True


def test_create_local_sandbox_defaults_outside_cwd(monkeypatch, tmp_path):
    monkeypatch.delenv("LOCAL_SANDBOX_ROOT_DIR", raising=False)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(local_mod, "GitHubTokenLocalShellBackend", _StubLocalShellBackend)
    monkeypatch.setattr(
        local_mod.os.path, "expanduser", lambda p: p.replace("~", str(tmp_path / "home"))
    )

    backend = local_mod.create_local_sandbox()

    stub = cast(_StubLocalShellBackend, backend)
    # Never the server's cwd — clones there land in uvicorn's --reload watch
    # tree and restart the runtime mid-run.
    assert stub.root_dir == str(tmp_path / "home" / ".open-swe" / "sandboxes")
    assert stub.virtual_mode is True


def test_create_local_sandbox_returns_github_token_backend(monkeypatch, tmp_path):
    monkeypatch.setenv("LOCAL_SANDBOX_ROOT_DIR", str(tmp_path / "root"))

    backend = local_mod.create_local_sandbox()

    assert isinstance(backend, GitHubTokenLocalShellBackend)


def _backend(tmp_path) -> GitHubTokenLocalShellBackend:
    return GitHubTokenLocalShellBackend(
        root_dir=str(tmp_path), virtual_mode=True, inherit_env=False
    )


def _capture_parent_execute(monkeypatch, output: str = "ok") -> dict[str, str]:
    captured: dict[str, str] = {}

    def fake_execute(self, command, *, timeout=None):
        captured["command"] = command
        return ExecuteResponse(output=output, exit_code=0, truncated=False)

    monkeypatch.setattr(LocalShellBackend, "execute", fake_execute)
    return captured


class TestGitHubTokenSubstitution:
    def test_substitutes_sentinel_with_real_token(self, monkeypatch, tmp_path):
        backend = _backend(tmp_path)
        captured = _capture_parent_execute(monkeypatch)
        backend.set_github_token("ghs_realtoken123")

        result = backend.execute("GH_TOKEN=dummy gh repo clone acme/widgets")

        assert captured["command"] == "GH_TOKEN='ghs_realtoken123' gh repo clone acme/widgets"
        assert result.exit_code == 0

    def test_substitutes_every_sentinel_occurrence(self, monkeypatch, tmp_path):
        backend = _backend(tmp_path)
        captured = _capture_parent_execute(monkeypatch)
        backend.set_github_token("ghs_tok")

        backend.execute("GH_TOKEN=dummy git fetch && GH_TOKEN=dummy gh pr view 1")

        assert captured["command"] == (
            "GH_TOKEN='ghs_tok' git fetch && GH_TOKEN='ghs_tok' gh pr view 1"
        )

    def test_no_token_leaves_command_untouched(self, monkeypatch, tmp_path):
        backend = _backend(tmp_path)
        captured = _capture_parent_execute(monkeypatch)

        backend.execute("GH_TOKEN=dummy gh repo clone acme/widgets")

        assert captured["command"] == "GH_TOKEN=dummy gh repo clone acme/widgets"

    def test_command_without_sentinel_untouched(self, monkeypatch, tmp_path):
        backend = _backend(tmp_path)
        captured = _capture_parent_execute(monkeypatch)
        backend.set_github_token("ghs_tok")

        backend.execute("echo GH_TOKEN=dummyish && ls -la")

        assert captured["command"] == "echo GH_TOKEN=dummyish && ls -la"

    def test_set_github_token_updates_existing_backend(self, monkeypatch, tmp_path):
        backend = _backend(tmp_path)
        captured = _capture_parent_execute(monkeypatch)

        backend.set_github_token("ghs_first")
        backend.execute("GH_TOKEN=dummy gh api /user")
        assert captured["command"] == "GH_TOKEN='ghs_first' gh api /user"

        backend.set_github_token("ghs_second")
        backend.execute("GH_TOKEN=dummy gh api /user")
        assert captured["command"] == "GH_TOKEN='ghs_second' gh api /user"

    def test_redacts_token_from_output(self, monkeypatch, tmp_path):
        backend = _backend(tmp_path)
        _capture_parent_execute(monkeypatch, output="GH_TOKEN='ghs_secret' leaked ghs_secret")
        backend.set_github_token("ghs_secret")

        result = backend.execute("GH_TOKEN=dummy env")

        assert "ghs_secret" not in result.output
        assert result.output == "GH_TOKEN='dummy' leaked dummy"

    def test_real_execution_never_sees_sentinel(self, tmp_path):
        backend = _backend(tmp_path)
        backend.set_github_token("ghs_realtoken123")

        result = backend.execute("GH_TOKEN=dummy /bin/sh -c 'echo token=$GH_TOKEN'")

        # The real token reached the subprocess, and the echoed value was
        # scrubbed back to the sentinel before returning.
        assert result.exit_code == 0
        assert "ghs_realtoken123" not in result.output
        assert "token=dummy" in result.output


class TestLocalGitHubTokenProvisioning:
    @pytest.mark.asyncio
    async def test_create_sandbox_sets_local_github_token(self, tmp_path):
        backend = _backend(tmp_path)
        with (
            patch("agent.server.create_sandbox", new=AsyncMock(return_value=backend)),
            patch(
                "agent.server.get_github_app_installation_token_with_expiry",
                new=AsyncMock(return_value=("ghs_local", "2025-01-01T13:00:00Z")),
            ),
            patch("agent.server._configure_github_proxy", new_callable=AsyncMock) as mock_proxy,
            patch.dict("os.environ", {"SANDBOX_TYPE": "local"}),
        ):
            from agent.server import _create_sandbox_with_proxy

            result = await _create_sandbox_with_proxy(thread_id="thread-local")

        assert result is backend
        assert backend._github_token == "ghs_local"
        mock_proxy.assert_not_called()
        assert "thread-local" in github_proxy._PROXY_TOKEN_EXPIRY

    @pytest.mark.asyncio
    async def test_refresh_path_sets_token_on_reused_local_backend(self, tmp_path):
        backend = _backend(tmp_path)
        with (
            patch(
                "agent.server.get_github_app_installation_token_with_expiry",
                new=AsyncMock(return_value=("ghs_refreshed", "2025-01-01T13:00:00Z")),
            ),
            patch("agent.server._configure_github_proxy", new_callable=AsyncMock) as mock_proxy,
            patch.dict("os.environ", {"SANDBOX_TYPE": "local"}),
        ):
            from agent.server import _refresh_github_proxy

            await _refresh_github_proxy(backend, thread_id="thread-local")

        assert backend._github_token == "ghs_refreshed"
        mock_proxy.assert_not_called()

    @pytest.mark.asyncio
    async def test_prefers_explicit_github_proxy_token(self, tmp_path):
        backend = _backend(tmp_path)
        token_mock = AsyncMock(return_value=("ghs_app", None))
        with (
            patch("agent.server.get_github_app_installation_token_with_expiry", new=token_mock),
            patch.dict("os.environ", {"SANDBOX_TYPE": "local"}),
        ):
            from agent.server import _refresh_github_proxy

            await _refresh_github_proxy(backend, "gho_user_oauth", thread_id="thread-local")

        assert backend._github_token == "gho_user_oauth"
        token_mock.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_token_available_leaves_backend_unset(self, tmp_path):
        backend = _backend(tmp_path)
        with (
            patch(
                "agent.server.get_github_app_installation_token_with_expiry",
                new=AsyncMock(return_value=(None, None)),
            ),
            patch.dict("os.environ", {"SANDBOX_TYPE": "local"}),
        ):
            from agent.server import _refresh_github_proxy

            await _refresh_github_proxy(backend, thread_id="thread-local")

        assert backend._github_token is None

    @pytest.mark.asyncio
    async def test_non_local_backend_skips_token_minting(self):
        token_mock = AsyncMock(return_value=("ghs_app", None))
        with (
            patch("agent.server.get_github_app_installation_token_with_expiry", new=token_mock),
            patch.dict("os.environ", {"SANDBOX_TYPE": "daytona"}),
        ):
            from agent.server import _refresh_github_proxy

            await _refresh_github_proxy(MagicMock(id="sb-1"), thread_id="thread-x")

        token_mock.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_mid_run_refresh_updates_local_backend_token(self, tmp_path):
        now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
        github_proxy.record_proxy_token_expiry("thread-l", now + timedelta(minutes=1))
        backend = _backend(tmp_path)
        with (
            patch.dict("os.environ", {"SANDBOX_TYPE": "local"}),
            patch.dict(github_proxy.SANDBOX_BACKENDS, {"thread-l": backend}, clear=True),
            patch(
                "agent.utils.github_proxy.get_github_app_installation_token_with_expiry",
                new=AsyncMock(return_value=("ghs_new", "2025-01-01T13:00:00Z")),
            ),
        ):
            assert await github_proxy.maybe_refresh_proxy_token("thread-l", now=now) is True

        assert backend._github_token == "ghs_new"
        expires_at, _recorded, _scope, _perms = github_proxy._PROXY_TOKEN_EXPIRY["thread-l"]
        assert expires_at == datetime(2025, 1, 1, 13, 0, 0, tzinfo=UTC)

    @pytest.mark.asyncio
    async def test_mid_run_refresh_skips_non_token_backend(self):
        now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
        github_proxy.record_proxy_token_expiry("thread-l", now + timedelta(minutes=1))
        with (
            patch.dict("os.environ", {"SANDBOX_TYPE": "daytona"}),
            patch.dict(
                github_proxy.SANDBOX_BACKENDS, {"thread-l": MagicMock(id="sb-1")}, clear=True
            ),
            patch(
                "agent.utils.github_proxy.get_github_app_installation_token_with_expiry",
                new=AsyncMock(return_value=("ghs_new", None)),
            ) as token_mock,
        ):
            assert await github_proxy.maybe_refresh_proxy_token("thread-l", now=now) is False

        token_mock.assert_not_awaited()
