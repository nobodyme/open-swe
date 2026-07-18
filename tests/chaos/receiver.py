"""Local completion-webhook capture receiver for chaos tests.

Pattern copied from the Phase 0/1 capture receivers (never imported — D5).
Auth is a bearer ``?token=`` (constant-time compared receiver-side by
agent/completion.py) — the payload itself is NOT signed.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse


class WebhookCapture:
    def __init__(self, token: str = "chaos-secret") -> None:
        self.token = token
        self.received: list[dict[str, Any]] = []
        capture = self

        class _Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802
                length = int(self.headers.get("Content-Length", "0"))
                payload = json.loads(self.rfile.read(length) or b"{}")
                query = parse_qs(urlparse(self.path).query)
                capture.received.append(
                    {
                        "payload": payload,
                        "token_valid": query.get("token") == [capture.token],
                    }
                )
                self.send_response(200)
                self.end_headers()

            def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
                pass

        self._server = HTTPServer(("127.0.0.1", 0), _Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def url(self) -> str:
        port = self._server.server_address[1]
        return f"http://127.0.0.1:{port}/webhooks/run-complete?token={self.token}"

    def __enter__(self) -> WebhookCapture:
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._server.shutdown()
        self._thread.join(timeout=5)
