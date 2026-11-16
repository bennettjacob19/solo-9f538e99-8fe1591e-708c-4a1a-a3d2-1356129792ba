"""Mock artifact directory service (pure Python stdlib).

Each instance serves artifact metadata from ``ARTIFACT_DIR`` and advertises a
configurable *directory identifier* inside every metadata response, so the
gateway can verify it before switching its upstream.

Runtime control plane (used by the web UI / tests):

* ``GET  /control/state``                 current id / mode / delay
* ``POST /control/mode``                  ``{"mode": "normal|delay|503",
                                            "delay": <seconds>}``
* ``POST /control/directory-id``          ``{"id": "..."}``
* ``GET  /control/stats``                 real per-artifact hit counters
* ``POST /control/stats/reset``           zero the counters
* ``GET  /healthz``
"""

from __future__ import annotations

import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

BASE_DIR = Path(__file__).resolve().parent
ARTIFACT_DIR = Path(os.environ.get("ARTIFACT_DIR", BASE_DIR / "artifacts"))

# In-process, per-instance runtime state. ThreadingHTTPServer spawns a thread
# per connection, so all mutable state is guarded by ``_lock``.
_lock = threading.RLock()
_state = {
    "directory_id": os.environ.get("DIRECTORY_ID", "directory-default"),
    "mode": "normal",          # normal | delay | 503
    "delay": float(os.environ.get("DEFAULT_DELAY", "2.0")),
}
_stats = {"hits": 0, "by_artifact": {}, "last_requests": []}

VALID_MODES = {"normal", "delay", "503"}


def _resolve(artifact: str) -> Path | None:
    """Map an artifact name to a metadata file inside ARTIFACT_DIR only."""
    candidate = (ARTIFACT_DIR / artifact).resolve()
    try:
        candidate.relative_to(ARTIFACT_DIR.resolve())
    except ValueError:
        return None
    if candidate.suffix != ".json":
        candidate = candidate.with_suffix(candidate.suffix + ".json")
    return candidate if candidate.is_file() else None


class DirectoryHandler(BaseHTTPRequestHandler):
    server_version = "ArtifactDirectory/1.0"

    # ---- helpers -------------------------------------------------------
    def _send_json(self, status: int, payload: dict) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            return {}
        return data if isinstance(data, dict) else {}

    def log_message(self, fmt: str, *args) -> None:  # quieter logs
        print(f"[directory {_state['directory_id']}] {self.address_string()} {fmt % args}")

    # ---- GET -----------------------------------------------------------
    def do_GET(self) -> None:  # noqa: N802 (stdlib naming)
        path = urlparse(self.path).path
        if path == "/healthz":
            self._send_json(200, {"status": "ok", "directory_id": _state["directory_id"]})
            return
        if path == "/control/state":
            with _lock:
                self._send_json(200, dict(_state))
            return
        if path == "/control/stats":
            with _lock:
                self._send_json(200, json.loads(json.dumps(_stats)))
            return
        if path.startswith("/artifacts/"):
            self._serve_artifact(path[len("/artifacts/"):])
            return
        self._send_json(404, {"error": "not_found", "path": path})

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        payload = self._read_json()
        if path == "/control/mode":
            mode = str(payload.get("mode") or "").strip()
            if mode not in VALID_MODES:
                self._send_json(400, {"error": f"mode must be one of {sorted(VALID_MODES)}"})
                return
            with _lock:
                if payload.get("delay") is not None:
                    try:
                        delay = float(payload["delay"])
                    except (TypeError, ValueError):
                        self._send_json(400, {"error": "delay must be a number"})
                        return
                    if not 0 <= delay <= 120:
                        self._send_json(400, {"error": "delay out of range (0..120s)"})
                        return
                    _state["delay"] = delay
                _state["mode"] = mode
                self._send_json(200, dict(_state))
            return
        if path == "/control/directory-id":
            new_id = str(payload.get("id") or "").strip()
            if not new_id:
                self._send_json(400, {"error": "id must be a non-empty string"})
                return
            with _lock:
                _state["directory_id"] = new_id
                self._send_json(200, dict(_state))
            return
        if path == "/control/stats/reset":
            with _lock:
                _stats["hits"] = 0
                _stats["by_artifact"] = {}
                _stats["last_requests"] = []
                self._send_json(200, {"ok": True})
            return
        self._send_json(404, {"error": "not_found", "path": path})

    # ---- metadata serving ---------------------------------------------
    def _serve_artifact(self, artifact: str) -> None:
        with _lock:
            mode = _state["mode"]
            delay = _state["delay"]
            directory_id = _state["directory_id"]
            _stats["hits"] += 1
            _stats["by_artifact"][artifact] = _stats["by_artifact"].get(artifact, 0) + 1
            recent = _stats["last_requests"]
            recent.insert(0, {"ts": round(time.time(), 3), "artifact": artifact, "mode": mode})
            del recent[30:]

        if mode == "delay":
            time.sleep(delay)
        if mode == "503":
            self._send_json(503, {"error": "directory_unavailable", "artifact": artifact,
                                  "directory": {"id": directory_id}})
            return

        path = _resolve(artifact)
        if path is None:
            # 4xx is a *healthy* response from the directory.
            self._send_json(404, {"error": "artifact_not_found", "artifact": artifact,
                                  "directory": {"id": directory_id}})
            return
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            self._send_json(500, {"error": f"unreadable metadata: {exc}",
                                  "directory": {"id": directory_id}})
            return
        self._send_json(200, {
            "artifact": artifact,
            "metadata": data,
            "directory": {"id": directory_id},
        })


def main() -> None:
    port = int(os.environ.get("PORT", "9001"))
    ThreadingHTTPServer.daemon_threads = True
    server = ThreadingHTTPServer(("0.0.0.0", port), DirectoryHandler)
    print(f"directory service '{_state['directory_id']}' listening on :{port} "
          f"(artifacts: {ARTIFACT_DIR})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
