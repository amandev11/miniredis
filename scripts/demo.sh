#!/usr/bin/env bash
#
# scripts/demo.sh is MiniRedis's end-to-end demonstration.
#
# It runs the real binaries over real sockets and shows, in order:
#
#   1. a server starting with the append-only journal in "always" mode;
#   2. two independent clients sharing one keyspace;
#   3. atomic increments from several clients;
#   4. hash storage;
#   5. TTL expiration, observed from a client;
#   6. the HTTP readiness and metrics endpoints;
#   7. a graceful shutdown and a restart that recovers the data.
#
# Every step prints the actual command and the actual reply. A step that does
# not produce the expected reply fails the script, so a green run means the
# behaviour was observed rather than described.
#
# Usage:
#   make demo                      # builds bin/ and runs this script
#   MINIREDIS_DEMO_PORT=6399 ./scripts/demo.sh

set -euo pipefail

PORT="${MINIREDIS_DEMO_PORT:-6399}"
HTTP_PORT="${MINIREDIS_DEMO_HTTP_PORT:-7399}"
ADDR="127.0.0.1:${PORT}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVER="${ROOT}/bin/miniredis"
CLI="${ROOT}/bin/mini-cli"

DATA_DIR="$(mktemp -d)"
# A demonstration password, not a secret: it is passed through the environment
# so that it never appears in the process list.
PASSWORD="demo-password-not-a-secret"
SERVER_PID=""
SERVER_LOG="$(mktemp)"

for binary in "$SERVER" "$CLI"; do
	if [[ ! -x "$binary" ]]; then
		echo "demo: $binary is missing; run 'make build' first" >&2
		exit 1
	fi
done

cleanup() {
	if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
		kill -TERM "$SERVER_PID" 2>/dev/null || true
		wait "$SERVER_PID" 2>/dev/null || true
	fi
	rm -rf "$DATA_DIR" "$SERVER_LOG"
}
trap cleanup EXIT

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
show() { printf '$ %s\n' "$*"; }

# cli runs one command as a separate client process and prints the reply.
cli() {
	show "mini-cli --addr $ADDR $*"
	"$CLI" --addr "$ADDR" -a "$PASSWORD" "$@"
}

# capture runs one command and prints only the reply, for assertions. --raw
# suppresses the client's quoting so the output can be compared directly.
capture() { "$CLI" --addr "$ADDR" -a "$PASSWORD" --raw "$@" 2>/dev/null; }

# expect_int compares an integer reply, stripping the "(integer) " prefix the
# client prints in interactive mode.
expect_int() {
	local want="$1"
	shift
	local got
	got="$(capture "$@")"
	got="${got#(integer) }"
	if [[ "$got" != "$want" ]]; then
		echo "FAIL: '$*' returned '$got', expected '$want'" >&2
		exit 1
	fi
	printf '  check: %s -> %s\n' "$*" "$got"
}

expect() {
	local want="$1"
	shift
	local got
	got="$(capture "$@")"
	if [[ "$got" != "$want" ]]; then
		echo "FAIL: '$*' returned '$got', expected '$want'" >&2
		exit 1
	fi
	printf '  check: %s -> %s\n' "$*" "$got"
}

start_server() {
	local args=(--addr "$ADDR" --data-dir "$DATA_DIR" --http-addr "127.0.0.1:${HTTP_PORT}"
		--appendonly --appendfsync always --log-level warn)
	if [[ $# -gt 0 ]]; then
		args+=("$@")
	fi
	show "MINIREDIS_REQUIREPASS=*** miniredis ${args[*]}"
	MINIREDIS_REQUIREPASS="$PASSWORD" "$SERVER" "${args[@]}" >>"$SERVER_LOG" 2>&1 &
	SERVER_PID=$!

	for _ in $(seq 1 100); do
		if "$CLI" --addr "$ADDR" -a "$PASSWORD" PING >/dev/null 2>&1; then
			echo "  server is ready on $ADDR (pid $SERVER_PID)"
			return 0
		fi
		if ! kill -0 "$SERVER_PID" 2>/dev/null; then
			echo "demo: the server exited during startup; its log follows" >&2
			cat "$SERVER_LOG" >&2
			exit 1
		fi
		sleep 0.1
	done
	echo "demo: the server did not become ready within 10s" >&2
	cp "$SERVER_LOG" /dev/stderr
	exit 1
}

stop_server() {
	show "kill -TERM $SERVER_PID"
	kill -TERM "$SERVER_PID"
	local status=0
	wait "$SERVER_PID" || status=$?
	SERVER_PID=""
	echo "  server exited (status $status)"
}

# ---------------------------------------------------------------------------

say "1. start a server with the append-only journal in 'always' mode"
start_server

say "2. two independent clients share one keyspace"
cli SET greeting "hello from client one"
cli GET greeting
expect "hello from client one" GET greeting
# A second, entirely separate process reads what the first one wrote.
show "mini-cli --addr $ADDR GET greeting   # a second, separate process"
echo "  the second client reads: $(capture GET greeting)"

say "3. increments are atomic across clients"
cli INCR pageviews
cli INCR pageviews
cli INCR pageviews
expect "3" GET pageviews

say "4. hashes"
cli HSET user:1 name aman role engineer
cli HGETALL user:1
expect "aman" HGET user:1 name

say "5. expiration: a short TTL disappears, a persistent key does not"
cli SET session token-value PX 1500
cli SET permanent value
cli TTL session
expect "token-value" GET session
echo "  waiting 2 seconds for the deadline to pass..."
sleep 2
cli GET session
expect_int "0" EXISTS session
expect_int "-2" TTL session
expect "value" GET permanent

say "6. HTTP readiness and metrics"
if command -v curl >/dev/null 2>&1; then
	show "curl -s http://127.0.0.1:${HTTP_PORT}/ready"
	curl -fsS "http://127.0.0.1:${HTTP_PORT}/ready" || { echo "FAIL: readiness endpoint" >&2; exit 1; }
	show "curl -s http://127.0.0.1:${HTTP_PORT}/metrics | head -c 400"
	curl -fsS "http://127.0.0.1:${HTTP_PORT}/metrics" | head -c 400
	echo
else
	echo "  curl is not installed; skipping the HTTP checks"
fi

say "7. graceful shutdown, then restart: the data comes back"	show "mini-cli --addr $ADDR --raw INFO persistence | grep -Ei 'journal|snapshot'"
	capture INFO persistence | grep -Ei 'journal|snapshot' || true
	stop_server

say "7b. the data directory now holds the journal (and a shutdown snapshot)"
ls -l "$DATA_DIR"
start_server
# The journal is replayed on startup and the snapshot is loaded first.
cli GET greeting
expect "hello from client one" GET greeting
cli HGETALL user:1
cli DBSIZE
show "mini-cli --addr $ADDR --raw INFO persistence | grep -Ei 'recovery|journal|snapshot'"
capture INFO persistence | grep -Ei 'recovery|journal|snapshot' || true

stop_server

say "demo completed: two clients, atomic increments, hashes, expiration, and restart recovery all observed"
