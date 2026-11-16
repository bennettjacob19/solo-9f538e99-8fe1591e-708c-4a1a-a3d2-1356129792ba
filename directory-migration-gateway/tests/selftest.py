"""End-to-end self-test for the directory-migration gateway.

Pure stdlib; run it against a live stack (docker compose or the local
``run_local.sh`` orchestrator)::

    GATEWAY=http://127.0.0.1:8080 \
    DIR_A=http://127.0.0.1:9001 DIR_B=http://127.0.0.1:9002 \
    python -I tests/selftest.py

Checks implemented:

1. baseline query + URL-bound directory id on the response
2. 503 failures open only that URL's breaker; the other URL stays CLOSED
3. preflight never changes breaker counters; works even while breaker OPEN
4. OPEN breaker rejects locally (upstream hit counter does not grow)
5. in-flight slow queries stay pinned to the old URL after an online switch;
   their late success/failure cannot change the new URL's breaker conclusion
6. same-version concurrent switch: exactly one winner, loser gets 409,
   failed/wrong-id switches leave the configuration untouched
7. rollback (switch back) restores the old URL's retained breaker state
8. restart persistence (when RESTART_CMD is provided)
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

GATEWAY = os.environ.get("GATEWAY", "http://127.0.0.1:8080")
DIR_A = os.environ.get("DIR_A", "http://127.0.0.1:9001")
DIR_B = os.environ.get("DIR_B", "http://127.0.0.1:9002")
RESTART_CMD = os.environ.get("RESTART_CMD", "")
ARTIFACT = "example-app.json"
FAILURE_THRESHOLD = int(os.environ.get("FAILURE_THRESHOLD", "3"))

PASS = 0
FAILURES: list[str] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    global PASS
    if condition:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAILURES.append(f"{name} :: {detail}")
        print(f"  FAIL  {name}  {detail}")


def http(url: str, method: str = "GET", payload: dict | None = None,
         timeout: float = 15.0) -> tuple[int, dict | list]:
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        try:
            return exc.code, json.loads(exc.read().decode("utf-8"))
        except ValueError:
            return exc.code, {}


def wait_healthy(url: str, attempts: int = 40) -> None:
    for _ in range(attempts):
        try:
            status, _ = http(url + "/healthz", timeout=2)
            if status == 200:
                return
        except OSError:
            pass
        time.sleep(0.5)
    raise RuntimeError(f"service never became healthy: {url}")


def set_mode(base: str, mode: str, delay: float | None = None) -> None:
    body = {"mode": mode}
    if delay is not None:
        body["delay"] = delay
    status, _ = http(base + "/control/mode", "POST", body)
    assert status == 200, (base, mode, status)


def reset_stats(base: str) -> None:
    http(base + "/control/stats/reset", "POST", {})


def breaker_of(url: str) -> dict:
    status, body = http(GATEWAY + "/api/breakers")
    assert status == 200
    for entry in body["breakers"]:
        if entry["url"] == url:
            return entry
    return {}


def config() -> dict:
    status, body = http(GATEWAY + "/api/config")
    assert status == 200
    return body


def switch(url: str, expected_id: str, version: int) -> tuple[int, dict]:
    return http(GATEWAY + "/api/switch", "POST",
                {"url": url, "expected_id": expected_id, "version": version})


def query_async(artifact: str = ARTIFACT) -> dict:
    """Fire one gateway query on a background thread."""
    box: dict = {}

    def run() -> None:
        box["result"] = http(
            GATEWAY + "/api/query?" + urllib.parse.urlencode({"artifact": artifact}),
            timeout=30)

    threading.Thread(target=run, daemon=True).start()
    return box


def main() -> int:
    wait_healthy(GATEWAY)
    wait_healthy(DIR_A)
    wait_healthy(DIR_B)

    # ---- deterministic baseline ---------------------------------------
    print("\n[setup] reset modes, stats and breakers")
    set_mode(DIR_A, "normal")
    set_mode(DIR_B, "normal")
    reset_stats(DIR_A)
    reset_stats(DIR_B)
    http(GATEWAY + "/api/breakers/reset", "POST", {})

    # 1) baseline query through directory A
    print("\n[1] baseline query routes to directory A")
    status, body = http(GATEWAY + "/api/query?artifact=" + ARTIFACT)
    check("baseline query 200", status == 200, str(body)[:300])
    check("response carries directory id directory-a",
          body.get("directory_id") == "directory-a", str(body.get("directory_id")))
    check("response states bound upstream URL",
          body.get("upstream_url") == DIR_A, str(body.get("upstream_url")))
    _, stats_a = http(DIR_A + "/control/stats")
    check("directory A really received the query", stats_a.get("hits", 0) >= 1,
          str(stats_a))

    # 2) failures on B open only B's breaker
    print("\n[2] 503 failures open only directory B's breaker")
    # Both directories are healthy now: switch to B, then make B fail.
    cfg = config()
    status, body = switch(DIR_B, "directory-b", cfg["version"])
    check("switch to B succeeds while B is healthy",
          status == 200 and body.get("ok") is True, f"{status} {body}")
    set_mode(DIR_B, "503")
    reset_stats(DIR_B)
    results = [http(GATEWAY + "/api/query?artifact=" + ARTIFACT) for _ in range(3)]
    check("three failures reported as 502",
          all(st == 502 for st, _ in results), str(results))
    br_b = breaker_of(DIR_B)
    check("B breaker is OPEN after 3 failures",
          br_b.get("state") == "OPEN", str(br_b))
    br_a = breaker_of(DIR_A)
    check("A breaker stays CLOSED while B fails",
          br_a.get("state") in ("CLOSED", ""), str(br_a))

    # 3) preflight does not touch breaker counters, even on an OPEN breaker
    print("\n[3] preflight bypasses the breaker and never changes counters")
    before = breaker_of(DIR_B)
    set_mode(DIR_B, "normal")
    st, body = http(GATEWAY + "/api/preflight", "POST",
                    {"url": DIR_B, "expected_id": "directory-b", "artifact": ARTIFACT})
    check("preflight against B succeeds", st == 200 and body.get("ok") is True,
          f"{st} {body}")
    set_mode(DIR_B, "503")
    st, body = http(GATEWAY + "/api/preflight", "POST",
                    {"url": DIR_B, "expected_id": "directory-b", "artifact": ARTIFACT})
    check("preflight reports failure honestly", body.get("ok") is False, str(body))
    after = breaker_of(DIR_B)
    check("preflight changed no breaker counters",
          before.get("requests") == after.get("requests")
          and before.get("failures") == after.get("failures")
          and before.get("state") == after.get("state"),
          f"before={before} after={after}")

    # 4) OPEN breaker rejects locally without touching the upstream
    print("\n[4] OPEN breaker rejects without calling the upstream")
    reset_stats(DIR_B)  # preflight above hit the directory; take a clean baseline
    st, body = http(GATEWAY + "/api/query?artifact=" + ARTIFACT)
    check("rejected with 503", st == 503 and "breaker" in str(body).lower(),
          f"{st} {body}")
    _, stats_b = http(DIR_B + "/control/stats")
    check("upstream B hit counter unchanged after rejection",
          stats_b.get("hits", 0) == 0, str(stats_b))
    rejected = breaker_of(DIR_B).get("rejected", 0)
    check("rejection counter incremented", rejected >= 1, str(rejected))

    # 5) in-flight slow queries stay pinned; late results can't change new config
    print("\n[5] slow in-flight queries are pinned and late results are quarantined")
    set_mode(DIR_A, "delay", 4.0)   # 4s latency, gateway timeout is 8s -> late success
    set_mode(DIR_B, "normal")
    # rollback to A (B's OPEN state must be retained for later checks)
    cfg = config()
    st, body = switch(DIR_A, "directory-a", cfg["version"])
    check("rollback switch to A succeeds", st == 200 and body.get("ok") is True,
          f"{st} {body}")
    slow1, slow2 = query_async(), query_async()
    time.sleep(1.0)  # let both workers dequeue and bind against A
    inflight = http(GATEWAY + "/api/inflight")[1]["inflight"]
    check("two queries are in flight on the two workers",
          len(inflight) >= 2, str(inflight))
    check("in-flight jobs are bound to A",
          all(j["bound_url"] == DIR_A for j in inflight), str(inflight))
    cfg = config()
    st, body = switch(DIR_B, "directory-b", cfg["version"])
    check("switch to B does not wait for slow A queries",
          st == 200 and body.get("ok") is True
          and (time.time() - inflight[0]["dequeued_at"]) < 3.5,
          f"{st} {body}")
    # Newly admitted query must immediately use B.
    st, body = http(GATEWAY + "/api/query?artifact=" + ARTIFACT)
    check("new query after switch uses B", st == 200
          and body.get("upstream_url") == DIR_B
          and body.get("directory_id") == "directory-b", f"{st} {body}")
    # This B query may legitimately be the cooldown recovery probe, so take
    # B's state *after* it as the baseline the late A replies must not touch.
    b_baseline = breaker_of(DIR_B)
    # Wait for the slow A queries; their late success must not mutate B.
    deadline = time.time() + 15
    while time.time() < deadline and ("result" not in slow1 or "result" not in slow2):
        time.sleep(0.25)
    check("both slow queries finished", "result" in slow1 and "result" in slow2)
    for label, box in (("slow1", slow1), ("slow2", slow2)):
        st, body = box.get("result", (0, {}))
        check(f"{label} still reports the original directory A",
              st == 200 and body.get("upstream_url") == DIR_A
              and body.get("directory_id") == "directory-a", str(body)[:200])
    b_after_wait = breaker_of(DIR_B)
    check("B breaker conclusion untouched by late A successes",
          b_after_wait.get("state") == b_baseline.get("state")
          and b_after_wait.get("consecutive_failures") == b_baseline.get("consecutive_failures")
          and b_after_wait.get("failures") == b_baseline.get("failures"),
          f"baseline={b_baseline} after={b_after_wait}")
    br_a = breaker_of(DIR_A)
    check("A breaker recorded its own late successes (still CLOSED)",
          br_a.get("state") == "CLOSED", str(br_a))

    # 5b) a late *failure* from an old era is quarantined and cannot trip the
    #      breaker (same-URL re-selection bumps the era while a query is in flight)
    print("\n[5b] late failure from an old era is quarantined by era")
    set_mode(DIR_A, "normal")  # preflight must pass before the slow query starts
    cfg = config()
    st, body = switch(DIR_A, "directory-a", cfg["version"])
    check("re-select A while healthy; active upstream is now A",
          st == 200 and body.get("ok") is True, f"{st} {body}")
    set_mode(DIR_A, "delay", 10.0)  # exceeds the 8s gateway timeout -> failure
    slow = query_async()
    time.sleep(1.0)
    bound = http(GATEWAY + "/api/inflight")[1]["inflight"]
    check("slow query is in flight against A",
          any(j["bound_url"] == DIR_A for j in bound), str(bound))
    era_before = breaker_of(DIR_A).get("era")
    set_mode(DIR_A, "normal")  # so the re-selection preflight passes
    cfg = config()
    st, body = switch(DIR_A, "directory-a", cfg["version"])  # same URL, new era
    check("re-selecting A succeeds and bumps its era again",
          st == 200 and body.get("ok") is True, f"{st} {body}")
    check("era actually advanced", breaker_of(DIR_A).get("era") == era_before + 1,
          str(breaker_of(DIR_A)))
    deadline = time.time() + 15
    while time.time() < deadline and "result" not in slow:
        time.sleep(0.25)
    st, old_body = slow.get("result", (0, {}))
    check("old slow query failed as 502/timeout", st == 502, str(old_body)[:200])
    check("stale failure flagged and ignored",
          old_body.get("stale_result_ignored") is True, str(old_body)[:300])
    check("stale failure did not trip A's breaker in the new era",
          breaker_of(DIR_A).get("state") == "CLOSED", str(breaker_of(DIR_A)))
    set_mode(DIR_A, "normal")

    # 6) optimistic concurrency: same version, exactly one winner
    print("\n[6] same-version concurrent switches have exactly one winner")
    cfg = config()
    version = cfg["version"]
    outcomes: list = []

    def do_switch(target: str, dir_id: str) -> None:
        outcomes.append(switch(target, dir_id, version))

    t1 = threading.Thread(target=do_switch, args=(DIR_A, "directory-a"))
    t2 = threading.Thread(target=do_switch, args=(DIR_B, "directory-b"))
    t1.start(); t2.start(); t1.join(); t2.join()
    winners = [o for o in outcomes if o[0] == 200]
    losers = [o for o in outcomes if o[0] == 409 and "version" in o[1].get("error", "")]
    check("exactly one concurrent switch wins", len(winners) == 1, str(outcomes))
    check("the loser gets a version conflict", len(losers) == 1, str(outcomes))
    check("config advanced by exactly one version",
          config()["version"] == version + 1, str(config()))
    # Wrong expected id: config must not change.
    cfg = config()
    st, body = switch(DIR_A, "wrong-id", cfg["version"])
    check("wrong directory id rejected", st == 409 and body.get("config_unchanged") is True,
          f"{st} {body}")
    check("version unchanged after failed switch", config()["version"] == cfg["version"],
          str(config()))
    # Unreachable URL: config must not change.
    st, body = switch("http://127.0.0.1:1", "directory-x", cfg["version"])
    check("unreachable URL rejected by preflight", st in (409, 500)
          and body.get("config_unchanged") is True, f"{st} {body}")
    check("version still unchanged", config()["version"] == cfg["version"], str(config()))

    # 7) rollback restores retained breaker state
    print("\n[7] rollback keeps per-URL breaker state")
    set_mode(DIR_A, "normal")
    set_mode(DIR_B, "normal")
    current = config()
    if current["upstream_url"] != DIR_B:
        st, body = switch(DIR_B, "directory-b", current["version"])
        check("move active upstream to B", st == 200 and body.get("ok") is True,
              f"{st} {body}")
    # Re-open B deterministically (independent of cooldown timing).
    set_mode(DIR_B, "503")
    for _ in range(FAILURE_THRESHOLD):
        st, _ = http(GATEWAY + "/api/query?artifact=" + ARTIFACT)
        if st == 503:
            break  # already open
    check("B breaker re-opened before rollback",
          breaker_of(DIR_B).get("state") == "OPEN", str(breaker_of(DIR_B)))
    set_mode(DIR_B, "normal")  # recovery would require a probe that never runs
    failures_before = breaker_of(DIR_B).get("failures")
    cfg = config()
    st, body = switch(DIR_A, "directory-a", cfg["version"])
    check("rollback to A succeeds", st == 200 and body.get("ok") is True, f"{st} {body}")
    br_a = breaker_of(DIR_A)
    check("A breaker record still exists after switching back", bool(br_a), str(br_a))
    br_b = breaker_of(DIR_B)
    check("B breaker remains OPEN across rollback (state not cleared)",
          br_b.get("state") == "OPEN", str(br_b))
    check("B failure counters survive rollback",
          br_b.get("failures") == failures_before, str(br_b))

    # 8) restart persistence
    if RESTART_CMD:
        print("\n[8] restart persistence")
        before_cfg = config()
        before_b = breaker_of(DIR_B)
        before_a = breaker_of(DIR_A)
        subprocess.run(RESTART_CMD, shell=True, check=True)
        wait_healthy(GATEWAY)
        after_cfg = config()
        check("config survives restart",
              after_cfg["version"] == before_cfg["version"]
              and after_cfg["upstream_url"] == before_cfg["upstream_url"],
              f"{before_cfg} -> {after_cfg}")
        check("B OPEN breaker survives restart",
              breaker_of(DIR_B).get("state") == "OPEN", str(breaker_of(DIR_B)))
        check("breaker counters survive restart",
              breaker_of(DIR_B).get("failures") == before_b.get("failures")
              and breaker_of(DIR_A).get("requests") == before_a.get("requests"),
              f"{before_a} {before_b}")
    else:
        print("\n[8] restart persistence skipped (set RESTART_CMD to enable)")

    print(f"\n==== {PASS} checks passed, {len(FAILURES)} failed ====")
    if FAILURES:
        for item in FAILURES:
            print(" -", item)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
