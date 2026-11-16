"""Unit tests for breaker state machine, persistence and switch semantics.

Pure stdlib; no network or running services required::

    python -I tests/test_unit.py
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import threading
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import gateway as gw  # noqa: E402

FAILURES: list[str] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    if condition:
        print(f"  PASS  {name}")
    else:
        FAILURES.append(f"{name} :: {detail}")
        print(f"  FAIL  {name}  {detail}")


def fresh_registry(tmp: Path) -> gw.BreakerRegistry:
    return gw.BreakerRegistry(tmp / "breakers.json")


def main() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="gw-unit-"))

    # 1) threshold opens; success resets; 4xx-style successes count as healthy
    print("\n[1] breaker counting and OPEN transition")
    reg = fresh_registry(tmp)
    url = "http://dir-x:9000"
    now = time.time()
    allowed, _, era, gen = reg.admit(url, now)
    check("first call admitted", allowed)
    for i in range(2):
        reg.record(url, False, era, gen, "http_503", now + i)
    check("still CLOSED after 2 failures", reg.entry(url)["state"] == "CLOSED")
    reg.record(url, False, era, gen, "http_503", now + 2)
    check("OPEN after the 3rd consecutive failure", reg.entry(url)["state"] == "OPEN")
    allowed2, entry2, _, _ = reg.admit(url, now + 3)
    check("OPEN rejects before cooldown", not allowed2)
    check("rejection counted", entry2["rejected"] >= 1)

    # 2) exactly one half-open probe after cooldown
    print("\n[2] single half-open probe election under concurrency")
    results: list = []

    def admit() -> None:
        results.append(reg.admit(url, now + 100))  # well past cooldown

    threads = [threading.Thread(target=admit) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    admitted = [r for r in results if r[0]]
    check("exactly one probe admitted", len(admitted) == 1, str(len(admitted)))
    check("state is HALF_OPEN with probe in flight",
          reg.entry(url)["state"] == "HALF_OPEN" and reg.entry(url)["probe_in_flight"])

    # 3) stale era/generation outcomes are discarded
    print("\n[3] stale era and generation outcomes cannot change conclusions")
    _, _, probe_era, probe_gen = admitted[0]
    outcome = reg.record(url, True, probe_era - 1, probe_gen, "http_2xx", now + 101)
    check("old-era success marked stale", outcome["stale"] is True)
    check("stale success does not close HALF_OPEN",
          reg.entry(url)["state"] == "HALF_OPEN")
    outcome = reg.record(url, False, probe_era, probe_gen - 1, "timeout", now + 102)
    check("old-generation failure marked stale", outcome["stale"] is True)
    check("stale failure does not reopen either",
          reg.entry(url)["state"] == "HALF_OPEN")
    # The genuine probe failing reopens with a new generation + cooldown.
    reg.record(url, False, probe_era, probe_gen, "http_503", now + 103)
    check("real probe failure reopens", reg.entry(url)["state"] == "OPEN")

    # 4) re-selection while HALF_OPEN orphans the probe slot
    print("\n[4] re-selection orphans an in-flight half-open probe")
    reg.admit(url, now + 200)  # cooldown elapsed -> HALF_OPEN again
    check("back in HALF_OPEN", reg.entry(url)["state"] == "HALF_OPEN")
    reg.bump_era(url)
    entry = reg.entry(url)
    check("orphaned probe reopens the breaker", entry["state"] == "OPEN")
    check("probe slot released", entry["probe_in_flight"] is False)
    era_now = entry["era"]
    later = reg.admit(url, time.time() + 1000)
    check("breaker can probe again after cooldown", later[0] is True
          and later[2] == era_now)

    # 5) persistence round trip, including the wrapped snapshot format
    print("\n[5] breaker persistence survives reconstruction")
    path = tmp / "breakers.json"
    on_disk = json.loads(path.read_text())
    check("snapshot is wrapped under 'breakers'", "breakers" in on_disk)
    reg2 = gw.BreakerRegistry(path)
    check("reloaded registry keeps every URL",
          reg2.entry(url)["state"] in ("OPEN", "HALF_OPEN"))
    check("reloaded counters match",
          reg2.entry(url)["failures"] == reg.entry(url)["failures"]
          and reg2.entry(url)["rejected"] == reg.entry(url)["rejected"])
    check("reloaded era matches", reg2.entry(url)["era"] == reg.entry(url)["era"])
    # Bare-mapping backward compatibility.
    bare = tmp / "bare.json"
    bare.write_text(json.dumps({"http://u:1": {"state": "OPEN", "failures": 9}}))
    reg3 = gw.BreakerRegistry(bare)
    check("bare mapping format also loads",
          reg3.entry("http://u:1")["state"] == "OPEN"
          and reg3.entry("http://u:1")["failures"] == 9)

    # 6) per-URL isolation: A open never affects B
    print("\n[6] per-URL breaker isolation")
    a, b = "http://a:9000", "http://b:9000"
    _, _, ea, ga = reg2.admit(a, time.time())
    for i in range(3):
        reg2.record(a, False, ea, ga, "http_503", time.time() + i)
    check("A opens", reg2.entry(a)["state"] == "OPEN")
    allowed_b, _, _, _ = reg2.admit(b, time.time())
    check("B remains independently CLOSED and admittable", allowed_b)

    # 7) config write failure must leave the in-memory configuration untouched
    print("\n[7] failed config write leaves configuration unchanged")
    data_dir = Path(tempfile.mkdtemp(prefix="gw-cfg-"))
    gw.CONFIG_PATH = data_dir / "config.json"
    gw.DATA_DIR = data_dir
    g = gw.Gateway()
    original = g.config_snapshot()

    real_write = gw._atomic_write_json

    def boom(path: Path, payload: dict) -> None:
        if path == gw.CONFIG_PATH:
            raise OSError("simulated disk failure")
        real_write(path, payload)

    gw._atomic_write_json = boom
    # Preflight is forced to succeed by stubbing it.
    g.preflight = lambda *a, **k: {"ok": True}  # type: ignore[method-assign]
    status, body = g.switch("http://new:9000", "directory-new",
                            original["version"])
    gw._atomic_write_json = real_write
    check("write failure reported as 500", status == 500, str(body))
    check("in-memory config unchanged",
          g.config_snapshot()["upstream_url"] == original["upstream_url"]
          and g.config_snapshot()["version"] == original["version"],
          str(g.config_snapshot()))
    check("response explicitly says config unchanged",
          body.get("config_unchanged") is True)

    # 8) optimistic version conflict: same version, one winner, one loser
    print("\n[8] optimistic switch conflict handling")
    g.preflight = lambda *a, **k: {"ok": True}  # type: ignore[method-assign]
    v = g.config_snapshot()["version"]
    outcomes: list = []

    def do_switch(u: str) -> None:
        outcomes.append(g.switch(u, "dir", v))

    t1 = threading.Thread(target=do_switch, args=("http://one:9000",))
    t2 = threading.Thread(target=do_switch, args=("http://two:9000",))
    t1.start(); t2.start(); t1.join(); t2.join()
    check("exactly one winner", sum(1 for s, _ in outcomes if s == 200) == 1)
    check("one version conflict",
          sum(1 for s, b in outcomes if s == 409 and "version" in b["error"]) == 1)
    check("version advanced once", g.config_snapshot()["version"] == v + 1)

    # 9) bad input validation
    print("\n[9] switch input validation")
    status, body = g.switch("not-a-url", "x", g.config_snapshot()["version"])
    check("non-http URL rejected", status == 400)
    status, body = g.switch("http://ok:1", "", g.config_snapshot()["version"])
    check("empty expected id rejected", status == 400)
    status, body = g.switch("http://ok:1", "x", "not-an-int")  # type: ignore[arg-type]
    check("non-integer version rejected", status == 400)

    # 10) preflight honestly verifies the directory id against a live stub
    print("\n[10] preflight id verification against a local stub server")
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    # Fresh gateway so the preflight stub installed above cannot shadow the
    # real implementation.
    g2 = gw.Gateway()

    class Stub(BaseHTTPRequestHandler):
        def log_message(self, *a):  # silence
            pass

        def do_GET(self):  # noqa: N802
            import json as _json
            payload = _json.dumps({"directory": {"id": "actual-id"}}).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Stub)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        ok = g2.preflight(f"http://127.0.0.1:{port}", "actual-id", "example-app.json")
        check("matching id passes preflight", ok["ok"] is True, str(ok))
        bad = g2.preflight(f"http://127.0.0.1:{port}", "expected-other",
                           "example-app.json")
        check("mismatched id fails preflight with reason",
              bad["ok"] is False and "mismatch" in bad["reason"], str(bad))
        down = g2.preflight("http://127.0.0.1:1", "x", "example-app.json")
        check("unreachable preflight reports request-stage error",
              down["ok"] is False and down["stage"] == "request", str(down))
    finally:
        server.shutdown()

    print(f"\n==== unit checks done, {len(FAILURES)} failures ====")
    for item in FAILURES:
        print(" -", item)
    return 1 if FAILURES else 0


if __name__ == "__main__":
    sys.exit(main())
