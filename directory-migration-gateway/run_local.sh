#!/usr/bin/env bash
# Run the full stack locally with the stdlib only (no Docker required):
#   directory-a on :9001, directory-b on :9002, gateway on :8080.
# State is written under ./data so restarts keep config + breaker state.
set -euo pipefail

cd "$(dirname "$0")"
PYTHON="${PYTHON:-python3}"
DATA_DIR="$(pwd)/data"
LOG_DIR="$(pwd)/logs"
mkdir -p "$DATA_DIR" "$LOG_DIR"

PORT_A=9001
PORT_B=9002
PORT_GW=8080
PIDS=()

start_directory() {
  local name="$1" port="$2"
  PORT="$port" DIRECTORY_ID="$name" ARTIFACT_DIR="$(pwd)/artifacts" \
    DEFAULT_DELAY="2.0" "$PYTHON" -I directory.py >"$LOG_DIR/$name.log" 2>&1 &
  PIDS+=("$!")
  echo "started $name on :$port (pid $!)"
}

start_gateway() {
  PORT="$PORT_GW" DATA_DIR="$DATA_DIR" \
    UPSTREAM_URL="http://127.0.0.1:$PORT_A" \
    DIRECTORY_A_URL="http://127.0.0.1:$PORT_A" \
    DIRECTORY_B_URL="http://127.0.0.1:$PORT_B" \
    WORKER_COUNT=2 REQUEST_TIMEOUT=8.0 PREFLIGHT_TIMEOUT=5.0 \
    COOLDOWN=5.0 FAILURE_THRESHOLD=3 \
    "$PYTHON" -I gateway.py >"$LOG_DIR/gateway.log" 2>&1 &
  GATEWAY_PID=$!
  PIDS+=("$GATEWAY_PID")
  echo "started gateway on :$PORT_GW (pid $GATEWAY_PID)"
}

wait_http() {
  local url="$1" tries=40
  while (( tries-- > 0 )); do
    if curl -fsS -o /dev/null "$url" 2>/dev/null; then return 0; fi
    sleep 0.5
  done
  echo "timeout waiting for $url" >&2
  return 1
}

cleanup() {
  echo
  echo "stopping local stack..."
  # The restart helper re-records the live gateway pid in gateway.pid, so
  # include it (it may differ from the original GATEWAY_PID).
  local restarted_pid
  restarted_pid="$(cat "$LOG_DIR/gateway.pid" 2>/dev/null || true)"
  for pid in "${PIDS[@]:-}" "$restarted_pid"; do
    [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
}
trap cleanup EXIT

start_directory directory-a "$PORT_A"
start_directory directory-b "$PORT_B"
start_gateway
wait_http "http://127.0.0.1:$PORT_A/healthz"
wait_http "http://127.0.0.1:$PORT_B/healthz"
wait_http "http://127.0.0.1:$PORT_GW/healthz"

restart_gateway_cmd() {
  kill "$GATEWAY_PID" 2>/dev/null || true
  wait "$GATEWAY_PID" 2>/dev/null || true
  start_gateway
  wait_http "http://127.0.0.1:$PORT_GW/healthz"
}

# The self-test restarts the gateway itself when RESTART_CMD points at this
# helper. It kills the recorded pid, waits for the port to be free, then starts
# a fresh gateway against the same DATA_DIR.
cat >"$LOG_DIR/restart.sh" <<EOF
#!/usr/bin/env bash
set -u
cd "$(pwd)"
OLDPID=\$(cat "$LOG_DIR/gateway.pid" 2>/dev/null || true)
if [ -n "\$OLDPID" ]; then
  kill "\$OLDPID" 2>/dev/null || true
  for _ in \$(seq 1 40); do
    if ! kill -0 "\$OLDPID" 2>/dev/null; then break; fi
    sleep 0.25
  done
fi
for _ in \$(seq 1 20); do
  if ! curl -fsS -o /dev/null "http://127.0.0.1:$PORT_GW/healthz" 2>/dev/null; then break; fi
  sleep 0.25
done
PORT="$PORT_GW" DATA_DIR="$DATA_DIR" \\
  UPSTREAM_URL="http://127.0.0.1:$PORT_A" \\
  DIRECTORY_A_URL="http://127.0.0.1:$PORT_A" \\
  DIRECTORY_B_URL="http://127.0.0.1:$PORT_B" \\
  WORKER_COUNT=2 REQUEST_TIMEOUT=8.0 PREFLIGHT_TIMEOUT=5.0 \\
  COOLDOWN=5.0 FAILURE_THRESHOLD=3 \\
  "$PYTHON" -I gateway.py >>"$LOG_DIR/gateway.log" 2>&1 &
echo \$! > "$LOG_DIR/gateway.pid"
for _ in \$(seq 1 40); do
  curl -fsS -o /dev/null "http://127.0.0.1:$PORT_GW/healthz" 2>/dev/null && exit 0
  sleep 0.25
done
exit 1
EOF
echo "$GATEWAY_PID" > "$LOG_DIR/gateway.pid"
chmod +x "$LOG_DIR/restart.sh"

echo
echo "stack is up: browser UI at http://127.0.0.1:$PORT_GW/"
echo "running self-test (including a gateway restart)..."
GATEWAY="http://127.0.0.1:$PORT_GW" \
  DIR_A="http://127.0.0.1:$PORT_A" \
  DIR_B="http://127.0.0.1:$PORT_B" \
  FAILURE_THRESHOLD=3 \
  RESTART_CMD="bash $LOG_DIR/restart.sh" \
  "$PYTHON" -I tests/selftest.py
TEST_RC=$?

if [[ "${1:-}" == "--keep" ]]; then
  echo
  echo "--keep given; stack stays up at http://127.0.0.1:$PORT_GW/ (Ctrl-C to stop)"
  wait
fi
exit "$TEST_RC"
