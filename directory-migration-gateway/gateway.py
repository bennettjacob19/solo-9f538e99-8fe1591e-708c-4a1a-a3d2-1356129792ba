"""Artifact-directory migration gateway (pure Python stdlib).

Responsibilities
----------------
* Two worker threads pull query jobs from a bounded queue.  Each worker reads
  the *current* upstream URL **at dequeue time**, so an online switch never
  waits for in-flight slow queries and newly admitted queries immediately use
  the new directory.
* Per-URL circuit breakers, persisted to ``DATA_DIR``.  Every query is bound to
  the URL it was admitted against; a late result can only mutate *that* URL's
  breaker, and era/generation checks discard stale outcomes so they can never
  change the current configuration's breaker conclusion.
* Safe online switching: a preflight query to the target URL must succeed and
  return the expected directory identifier.  Preflight never touches breaker
  counters.  The switch itself is optimistic on a configuration version and is
  serialized by a lock, so same-version concurrent switches have exactly one
  winner; any failure leaves the configuration untouched.
* JSON state is persisted atomically (temp file + fsync + rename), so restarts
  keep the configuration and every URL's breaker state.
"""

from __future__ import annotations

import json
import os
import queue
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
DATA_DIR = Path(os.environ.get("DATA_DIR", BASE_DIR / "data"))

CONFIG_PATH = DATA_DIR / "config.json"
BREAKERS_PATH = DATA_DIR / "breakers.json"

REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", "3.0"))
PREFLIGHT_TIMEOUT = float(os.environ.get("PREFLIGHT_TIMEOUT", "5.0"))
COOLDOWN = float(os.environ.get("COOLDOWN", "5.0"))
FAILURE_THRESHOLD = int(os.environ.get("FAILURE_THRESHOLD", "3"))
WORKER_COUNT = int(os.environ.get("WORKER_COUNT", "2"))
QUEUE_MAX = int(os.environ.get("QUEUE_MAX", "128"))
LISTEN_PORT = int(os.environ.get("PORT", "8080"))

# Directory instances exposed in the web UI control panels.
DIRECTORY_TARGETS = {
    "a": os.environ.get("DIRECTORY_A_URL", "http://directory-a:9000"),
    "b": os.environ.get("DIRECTORY_B_URL", "http://directory-b:9000"),
}

DEFAULT_CONFIG = {
    "version": 1,
    "upstream_url": os.environ.get("UPSTREAM_URL", "http://directory-a:9000"),
    "artifact": os.environ.get("DEFAULT_ARTIFACT", "example-app.json"),
    "updated_at": None,
}


# --------------------------------------------------------------------------
# Persistence helpers
# --------------------------------------------------------------------------
def _atomic_write_json(path: Path, payload: dict) -> None:
    """Write JSON durably: temp file -> fsync -> atomic rename -> dir fsync."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    data = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(data.decode("utf-8"))
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)
    try:
        dir_fd = os.open(str(path.parent), os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except OSError:
        # Some filesystems do not support fsync on directories.
        pass


def _load_json(path: Path, default: dict) -> dict:
    if not path.is_file():
        return json.loads(json.dumps(default))
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        return data if isinstance(data, dict) else json.loads(json.dumps(default))
    except (OSError, ValueError):
        return json.loads(json.dumps(default))


# --------------------------------------------------------------------------
# Circuit breakers (one entry per upstream URL)
# --------------------------------------------------------------------------
class BreakerRegistry:
    """In-memory breaker state with durable JSON snapshots."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = threading.RLock()
        stored = _load_json(path, {})
        # Persisted snapshots are wrapped as {"breakers": {url: entry}}; accept
        # both the wrapped format and a bare {url: entry} mapping.
        if isinstance(stored, dict) and "breakers" in stored \
                and isinstance(stored["breakers"], dict):
            stored = stored["breakers"]
        # Normalize / fill missing fields so old snapshots stay loadable.
        self._breakers: dict[str, dict] = {}
        for url, entry in stored.items():
            if not isinstance(entry, dict):
                continue
            self._breakers[url] = self._fresh_entry(url, entry)

    @staticmethod
    def _fresh_entry(url: str, base: dict | None = None) -> dict:
        base = base or {}
        return {
            "url": url,
            "state": str(base.get("state", "CLOSED")),
            "consecutive_failures": int(base.get("consecutive_failures", 0)),
            "opened_at": base.get("opened_at"),
            "last_event": base.get("last_event", "initialized"),
            "last_event_at": base.get("last_event_at"),
            # ``generation`` bumps on every state transition; ``era`` bumps
            # whenever the URL is (re)selected as the active upstream.
            "generation": int(base.get("generation", 0)),
            "era": int(base.get("era", 0)),
            "probe_in_flight": False,
            "requests": int(base.get("requests", 0)),
            "failures": int(base.get("failures", 0)),
            "rejected": int(base.get("rejected", 0)),
        }

    def lock(self) -> threading.RLock:
        return self._lock

    def entry(self, url: str) -> dict:
        with self._lock:
            if url not in self._breakers:
                self._breakers[url] = self._fresh_entry(url)
                self._persist_locked()
            return self._breakers[url]

    def snapshot(self) -> list[dict]:
        with self._lock:
            return [dict(e) for e in self._breakers.values()]

    def reset(self) -> None:
        with self._lock:
            urls = list(self._breakers)
            self._breakers = {u: self._fresh_entry(u) for u in urls}
            self._persist_locked()

    def bump_era(self, url: str) -> None:
        """Mark a (re)selection of ``url``; invalidates all older in-flight
        results so late replies from a previous era cannot land here.

        A half-open probe that is still in flight from the previous era is
        orphaned by definition: its result will be discarded as stale, so the
        probe slot is released and the breaker reopens with a fresh cooldown
        instead of being stuck in HALF_OPEN forever.
        """
        with self._lock:
            entry = self.entry(url)
            entry["era"] += 1
            if entry["state"] == "HALF_OPEN":
                entry["state"] = "OPEN"
                entry["generation"] += 1
                entry["probe_in_flight"] = False
                entry["opened_at"] = time.time()
                entry["last_event"] = "orphan_probe_reopened_on_reselect"
            else:
                entry["last_event"] = "selected_as_upstream"
            entry["last_event_at"] = round(time.time(), 3)
            self._persist_locked()

    def admit(self, url: str, now: float) -> tuple[bool, dict, int, int]:
        """Decide whether a request may leave for ``url``.

        Returns ``(allowed, entry_snapshot, era, generation)``.
        """
        with self._lock:
            entry = self.entry(url)
            entry["requests"] += 1
            if entry["state"] == "CLOSED":
                self._persist_locked()
                return True, dict(entry), entry["era"], entry["generation"]
            if entry["state"] == "OPEN":
                if entry["opened_at"] is not None and now - entry["opened_at"] >= COOLDOWN:
                    entry["state"] = "HALF_OPEN"
                    entry["generation"] += 1
                    entry["probe_in_flight"] = True
                    entry["last_event"] = "half_open_probe"
                    entry["last_event_at"] = round(now, 3)
                    self._persist_locked()
                    return True, dict(entry), entry["era"], entry["generation"]
                entry["rejected"] += 1
                self._persist_locked()
                return False, dict(entry), entry["era"], entry["generation"]
            # HALF_OPEN: the single probe is still in flight.
            entry["rejected"] += 1
            self._persist_locked()
            return False, dict(entry), entry["era"], entry["generation"]

    def record(self, url: str, success: bool, era: int, generation: int,
               reason: str, now: float) -> dict:
        """Record a finished call. Stale eras/generations are ignored."""
        with self._lock:
            entry = self.entry(url)
            stale = entry["era"] != era or entry["generation"] != generation
            result = {"stale": stale, "state": entry["state"]}
            if stale:
                entry["last_event"] = (
                    "stale_success_ignored" if success else "stale_failure_ignored"
                )
                entry["last_event_at"] = round(now, 3)
                self._persist_locked()
                result.update(dict(entry))
                return result
            if success:
                entry["failures"] = entry.get("failures", 0)
                if entry["state"] == "HALF_OPEN":
                    entry["state"] = "CLOSED"
                    entry["generation"] += 1
                    entry["probe_in_flight"] = False
                entry["consecutive_failures"] = 0
                entry["last_event"] = f"success:{reason}"
            else:
                entry["failures"] += 1
                if entry["state"] == "HALF_OPEN":
                    entry["state"] = "OPEN"
                    entry["generation"] += 1
                    entry["probe_in_flight"] = False
                    entry["opened_at"] = now
                elif entry["state"] == "CLOSED":
                    entry["consecutive_failures"] += 1
                    if entry["consecutive_failures"] >= FAILURE_THRESHOLD:
                        entry["state"] = "OPEN"
                        entry["generation"] += 1
                        entry["opened_at"] = now
                entry["last_event"] = f"failure:{reason}"
            entry["last_event_at"] = round(now, 3)
            self._persist_locked()
            result.update(dict(entry))
            return result

    def _persist_locked(self) -> None:
        _atomic_write_json(self._path, {"breakers": self._breakers})


# --------------------------------------------------------------------------
# Gateway core: config + worker pool
# --------------------------------------------------------------------------
class Gateway:
    def __init__(self) -> None:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._config = _load_json(CONFIG_PATH, DEFAULT_CONFIG)
        # Backfill any missing default keys.
        for key, value in DEFAULT_CONFIG.items():
            self._config.setdefault(key, value)
        self.breakers = BreakerRegistry(BREAKERS_PATH)
        self._queue: queue.Queue = queue.Queue(maxsize=QUEUE_MAX)
        self._inflight_lock = threading.Lock()
        self._inflight: dict[int, dict] = {}
        self._job_seq = 0
        self._workers = [
            threading.Thread(target=self._worker_loop, name=f"worker-{i + 1}",
                             daemon=True)
            for i in range(WORKER_COUNT)
        ]

    def start(self) -> None:
        for worker in self._workers:
            worker.start()

    # ---- config --------------------------------------------------------
    def config_snapshot(self) -> dict:
        with self._lock:
            return dict(self._config)

    def inflight_snapshot(self) -> list[dict]:
        now = time.time()
        with self._inflight_lock:
            out = []
            for job in self._inflight.values():
                item = dict(job)
                item["age_seconds"] = round(now - item["dequeued_at"], 3)
                out.append(item)
            return out

    # ---- preflight (never touches breakers) ----------------------------
    def preflight(self, url: str, expected_id: str, artifact: str) -> dict:
        target = url.rstrip("/") + "/artifacts/" + urllib.parse.quote(artifact, safe="")
        started = time.time()
        try:
            with urllib.request.urlopen(target, timeout=PREFLIGHT_TIMEOUT) as resp:
                status = resp.status
                body = resp.read()
        except urllib.error.HTTPError as exc:
            status = exc.code
            body = exc.read()
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            return {"ok": False, "stage": "request", "url": url, "artifact": artifact,
                    "error": f"connection error: {exc}", "duration": round(time.time() - started, 3)}
        try:
            payload = json.loads(body.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            payload = {}
        actual_id = ((payload.get("directory") or {}).get("id")) if isinstance(payload, dict) else None
        ok = status == 200 and actual_id == expected_id
        reason = None
        if status != 200:
            reason = f"unexpected HTTP status {status}"
        elif actual_id != expected_id:
            reason = f"directory id mismatch: expected {expected_id!r}, got {actual_id!r}"
        return {"ok": ok, "stage": "verify", "url": url, "artifact": artifact,
                "http_status": status, "expected_id": expected_id,
                "actual_id": actual_id, "reason": reason,
                "duration": round(time.time() - started, 3)}

    # ---- optimistic switch ---------------------------------------------
    def switch(self, url: str, expected_id: str, expected_version: int,
               artifact: str | None = None) -> tuple[int, dict]:
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            return 400, {"ok": False, "error": "url must be an absolute http(s) URL"}
        if not expected_id or not isinstance(expected_id, str):
            return 400, {"ok": False, "error": "expected_id is required"}
        if not isinstance(expected_version, int):
            return 400, {"ok": False, "error": "version must be an integer"}

        # 1) preflight outside the lock: ordinary queries keep flowing.
        check = self.preflight(url, expected_id, artifact or self._config["artifact"])
        if not check["ok"]:
            return 409, {"ok": False, "error": "preflight failed", "preflight": check,
                         "config_unchanged": True}

        # 2) serialized optimistic commit.
        with self._lock:
            current = self._config
            if current["version"] != expected_version:
                return 409, {
                    "ok": False, "error": "version conflict: another switch committed first",
                    "expected_version": expected_version,
                    "current_version": current["version"],
                    "config_unchanged": True,
                }
            new_config = dict(current)
            new_config["version"] = current["version"] + 1
            new_config["upstream_url"] = url
            if artifact:
                new_config["artifact"] = artifact
            new_config["updated_at"] = round(time.time(), 3)
            try:
                _atomic_write_json(CONFIG_PATH, new_config)
            except OSError as exc:
                # Persistence failed: in-memory config is deliberately left
                # untouched, so the running configuration does not change.
                return 500, {"ok": False, "error": f"config write failed: {exc}",
                              "config_unchanged": True}
            self._config = new_config
        # Era bump outside the config lock (breaker has its own lock); its
        # persistence failure must not undo a durably committed switch.
        try:
            self.breakers.bump_era(url)
        except OSError as exc:
            print(f"[gateway] WARNING: era bump persist failed: {exc}")
        return 200, {"ok": True, "config": dict(new_config), "preflight": check}

    # ---- query worker pool ---------------------------------------------
    def submit_query(self, artifact: str) -> dict:
        job_id = self._next_job_id()
        result_q: queue.Queue = queue.Queue(maxsize=1)
        try:
            self._queue.put((job_id, artifact, result_q), block=True, timeout=30)
        except queue.Full:
            return {"gateway_status": 503, "error": "gateway queue full",
                    "artifact": artifact}
        try:
            return result_q.get(block=True, timeout=max(REQUEST_TIMEOUT + 30, 35))
        except queue.Empty:
            return {"gateway_status": 504, "error": "worker timeout", "artifact": artifact}

    def _next_job_id(self) -> int:
        with self._inflight_lock:
            self._job_seq += 1
            return self._job_seq

    def _worker_loop(self) -> None:
        worker = threading.current_thread().name
        while True:
            job_id, artifact, result_q = self._queue.get()
            try:
                result = self._handle_job(job_id, artifact, worker)
            except Exception as exc:  # pragma: no cover - defensive
                result = {"gateway_status": 500, "error": f"worker error: {exc}",
                          "artifact": artifact, "worker": worker}
            finally:
                with self._inflight_lock:
                    self._inflight.pop(job_id, None)
            result_q.put(result)

    def _handle_job(self, job_id: int, artifact: str, worker: str) -> dict:
        now = time.time()
        # Bind this job to the *current* config at dequeue time. A switch
        # afterwards neither cancels nor redirects it.
        config = self.config_snapshot()
        url = config["upstream_url"]
        version = config["version"]
        with self._inflight_lock:
            self._inflight[job_id] = {
                "job_id": job_id, "artifact": artifact, "worker": worker,
                "bound_url": url, "config_version": version,
                "dequeued_at": round(now, 3),
            }

        allowed, breaker_before, era, generation = self.breakers.admit(url, now)
        base = {
            "artifact": artifact, "worker": worker, "upstream_url": url,
            "config_version": version, "era": era, "generation": generation,
        }
        if not allowed:
            retry_after = COOLDOWN
            if breaker_before.get("opened_at") is not None:
                retry_after = max(0, round(
                    COOLDOWN - (now - breaker_before["opened_at"]), 3))
            return {**base, "gateway_status": 503,
                    "error": f"circuit breaker OPEN for {url}",
                    "retry_after_seconds": retry_after,
                    "breaker": self.breakers.entry(url)}

        started = time.time()
        target = url.rstrip("/") + "/artifacts/" + urllib.parse.quote(artifact, safe="")
        status: int
        body: bytes
        try:
            with urllib.request.urlopen(target, timeout=REQUEST_TIMEOUT) as resp:
                status = resp.status
                body = resp.read()
            reason = "http_2xx" if status < 400 else f"http_{status}"
            success = status < 500  # 4xx means the directory itself is healthy
        except urllib.error.HTTPError as exc:
            status = exc.code
            body = exc.read()
            reason = f"http_{status}"
            success = status < 500
        except TimeoutError:
            status, body, reason, success = 504, b"", "timeout", False
        except (urllib.error.URLError, OSError) as exc:
            status, body, reason, success = 502, str(exc).encode(), "connection_error", False
        duration = round(time.time() - started, 3)

        breaker_after = self.breakers.record(url, success, era, generation, reason, time.time())
        directory_id = None
        try:
            parsed_body = json.loads(body.decode("utf-8"))
            if isinstance(parsed_body, dict):
                directory_id = (parsed_body.get("directory") or {}).get("id")
        except (ValueError, UnicodeDecodeError):
            parsed_body = None

        gateway_status = 200 if success else 502
        return {
            **base,
            "gateway_status": gateway_status,
            "upstream_http_status": status,
            "directory_id": directory_id,
            "duration_seconds": duration,
            "stale_result_ignored": bool(breaker_after.get("stale")),
            "breaker_event": breaker_after.get("last_event"),
            "breaker": breaker_after,
            "body": parsed_body if parsed_body is not None else body.decode("utf-8", "replace"),
        }


GATEWAY = Gateway()


# --------------------------------------------------------------------------
# HTTP layer
# --------------------------------------------------------------------------
def _http_json(url: str, method: str = "GET", payload: dict | None = None,
               timeout: float = 5.0) -> tuple[int, bytes]:
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read()


class Handler(BaseHTTPRequestHandler):
    server_version = "MigrationGateway/1.0"

    def _send_json(self, status: int, payload: dict | list) -> None:
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
        try:
            data = json.loads(self.rfile.read(length).decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            return {}
        return data if isinstance(data, dict) else {}

    def log_message(self, fmt: str, *args) -> None:
        print(f"[gateway] {self.address_string()} {fmt % args}")

    # ---- GET -----------------------------------------------------------
    def do_GET(self) -> None:  # noqa: N802
        parsed = urllib.parse.urlparse(self.path)
        path, query = parsed.path, urllib.parse.parse_qs(parsed.query)
        if path in ("/", "/index.html"):
            self._serve_static("index.html", "text/html; charset=utf-8")
            return
        if path.startswith("/static/"):
            name = path[len("/static/"):]
            ctype = "application/javascript; charset=utf-8" if name.endswith(".js") else "text/plain"
            self._serve_static(name, ctype)
            return
        if path == "/healthz":
            self._send_json(200, {"status": "ok", "config": GATEWAY.config_snapshot()})
            return
        if path == "/api/config":
            self._send_json(200, GATEWAY.config_snapshot())
            return
        if path == "/api/breakers":
            self._send_json(200, {"breakers": GATEWAY.breakers.snapshot(),
                                  "threshold": FAILURE_THRESHOLD, "cooldown": COOLDOWN})
            return
        if path == "/api/inflight":
            self._send_json(200, {"workers": WORKER_COUNT,
                                  "inflight": GATEWAY.inflight_snapshot()})
            return
        if path == "/api/directories":
            self._send_json(200, self._directory_overview())
            return
        if path == "/api/query":
            artifact = (query.get("artifact") or [GATEWAY.config_snapshot()["artifact"]])[0]
            result = GATEWAY.submit_query(artifact)
            self._send_json(int(result.get("gateway_status", 200)), result)
            return
        if path == "/api/preflight":
            url = (query.get("url") or [""])[0]
            expected_id = (query.get("expected_id") or [""])[0]
            artifact = (query.get("artifact") or [GATEWAY.config_snapshot()["artifact"]])[0]
            if not url or not expected_id:
                self._send_json(400, {"ok": False, "error": "url and expected_id are required"})
                return
            result = GATEWAY.preflight(url, expected_id, artifact)
            self._send_json(200 if result["ok"] else 409, result)
            return
        self._send_json(404, {"error": "not_found", "path": path})

    # ---- POST ----------------------------------------------------------
    def do_POST(self) -> None:  # noqa: N802
        path = urllib.parse.urlparse(self.path).path
        payload = self._read_json()
        if path == "/api/switch":
            url = str(payload.get("url") or "").strip()
            expected_id = str(payload.get("expected_id") or "").strip()
            version = payload.get("version")
            if not isinstance(version, int):
                self._send_json(400, {"ok": False,
                                      "error": "version (integer) is required"})
                return
            artifact = payload.get("artifact") or None
            status, body = GATEWAY.switch(url, expected_id, version, artifact)
            self._send_json(status, body)
            return
        if path == "/api/preflight":
            url = str(payload.get("url") or "").strip()
            expected_id = str(payload.get("expected_id") or "").strip()
            artifact = payload.get("artifact") or GATEWAY.config_snapshot()["artifact"]
            if not url or not expected_id:
                self._send_json(400, {"ok": False, "error": "url and expected_id are required"})
                return
            result = GATEWAY.preflight(url, expected_id, artifact)
            self._send_json(200 if result["ok"] else 409, result)
            return
        if path == "/api/breakers/reset":
            GATEWAY.breakers.reset()
            self._send_json(200, {"ok": True})
            return
        if path == "/api/directories/control":
            self._directory_control(payload)
            return
        self._send_json(404, {"error": "not_found", "path": path})

    # ---- static files --------------------------------------------------
    def _serve_static(self, name: str, content_type: str) -> None:
        target = (STATIC_DIR / name).resolve()
        try:
            target.relative_to(STATIC_DIR.resolve())
        except ValueError:
            self._send_json(404, {"error": "not_found"})
            return
        if not target.is_file():
            self._send_json(404, {"error": "not_found"})
            return
        body = target.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # ---- directory control proxy --------------------------------------
    def _resolve_directory_url(self, target: str) -> str | None:
        if target in DIRECTORY_TARGETS:
            return DIRECTORY_TARGETS[target]
        parsed = urllib.parse.urlparse(target)
        if parsed.scheme in ("http", "https") and parsed.netloc:
            return target
        return None

    def _directory_overview(self) -> dict:
        out = {}
        for name, base_url in DIRECTORY_TARGETS.items():
            item = {"url": base_url}
            try:
                status, body = _http_json(base_url + "/control/state")
                item["state"] = json.loads(body)
                item["state_status"] = status
            except (OSError, ValueError) as exc:
                item["state_error"] = str(exc)
            try:
                status, body = _http_json(base_url + "/control/stats")
                item["stats"] = json.loads(body)
            except (OSError, ValueError) as exc:
                item["stats_error"] = str(exc)
            out[name] = item
        return out

    def _directory_control(self, payload: dict) -> None:
        base_url = self._resolve_directory_url(str(payload.get("target") or ""))
        if base_url is None:
            self._send_json(400, {"error": "target must be 'a', 'b' or an http(s) URL"})
            return
        action = payload.get("action")
        try:
            if action == "mode":
                status, body = _http_json(base_url + "/control/mode", "POST", {
                    "mode": payload.get("mode"),
                    "delay": payload.get("delay"),
                })
            elif action == "directory-id":
                status, body = _http_json(base_url + "/control/directory-id", "POST",
                                          {"id": payload.get("id")})
            elif action == "stats-reset":
                status, body = _http_json(base_url + "/control/stats/reset", "POST", {})
            else:
                self._send_json(400, {"error": "action must be mode, directory-id or stats-reset"})
                return
        except OSError as exc:
            self._send_json(502, {"error": f"directory control failed: {exc}"})
            return
        try:
            self._send_json(status, json.loads(body))
        except ValueError:
            self._send_json(status, {"raw": body.decode("utf-8", "replace")})


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    GATEWAY.start()
    ThreadingHTTPServer.daemon_threads = True
    server = ThreadingHTTPServer(("0.0.0.0", LISTEN_PORT), Handler)
    print(f"gateway listening on :{LISTEN_PORT}, workers={WORKER_COUNT}, "
          f"data dir={DATA_DIR}, config={GATEWAY.config_snapshot()}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
