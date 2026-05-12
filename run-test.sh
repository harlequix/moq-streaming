#!/usr/bin/env bash
set -euo pipefail

TESTBED="$(cd "$(dirname "$0")" && pwd)"
RESULTS="$TESTBED/results/$(date +%Y%m%d-%H%M%S)"

VIDEO="${1:-$TESTBED/testdata/bbb_30s.mp4}"
DURATION="${2:-15}" # seconds to run before stopping

if [ ! -f "$VIDEO" ]; then
    echo "Video not found: $VIDEO"
    echo "Usage: $0 [video-file] [duration-seconds]"
    echo ""
    echo "Default: testdata/bbb_30s.mp4 (download with: ./download-bbb.sh)"
    echo "Quick test: $0 testdata/test_10s.mp4 12"
    exit 1
fi

echo "=== MoQ Test: 1 publisher, 1 relay, 2 subscribers ==="
echo "Video:    $VIDEO"
echo "Duration: ${DURATION}s"
echo "Results:  $RESULTS"
echo ""

# Build binaries
echo "[build] Compiling..."
cd "$TESTBED"
go build -o moq-publisher ./cmd/publisher/
go build -o moq-relay ./cmd/relay/
go build -o moq-subscriber ./cmd/subscriber/
echo "[build] Done"

mkdir -p "$RESULTS/sub1" "$RESULTS/sub2"

PIDS=()
cleanup() {
    echo ""
    echo "[cleanup] Stopping processes..."
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# Start publisher on :4443
echo "[pub]   Starting publisher on :4443..."
"$TESTBED/moq-publisher" \
    --video "$VIDEO" \
    --namespace live \
    --track video \
    --listen :4443 \
    --manifest "$RESULTS/manifest.csv" \
    2>"$RESULTS/publisher.log" &
PIDS+=($!)
sleep 1

# Start relay: upstream=publisher:4443, downstream=:4444
echo "[relay] Starting relay on :4444..."
"$TESTBED/moq-relay" \
    --listen :4444 \
    --upstream 127.0.0.1:4443 \
    --namespace live \
    --track video \
    2>"$RESULTS/relay.log" &
PIDS+=($!)
sleep 1

# Start subscriber 1
echo "[sub1]  Starting subscriber 1..."
"$TESTBED/moq-subscriber" \
    --relay 127.0.0.1:4444 \
    --namespace live \
    --track video \
    --recv-log "$RESULTS/sub1/recv_log.csv" \
    --display-log "$RESULTS/sub1/display_log.csv" \
    --lua "$TESTBED/subscriber/framelog.lua" \
    2>"$RESULTS/sub1/subscriber.log" &
PIDS+=($!)

# Start subscriber 2
echo "[sub2]  Starting subscriber 2..."
"$TESTBED/moq-subscriber" \
    --relay 127.0.0.1:4444 \
    --namespace live \
    --track video \
    --recv-log "$RESULTS/sub2/recv_log.csv" \
    --display-log "$RESULTS/sub2/display_log.csv" \
    --lua "$TESTBED/subscriber/framelog.lua" \
    2>"$RESULTS/sub2/subscriber.log" &
PIDS+=($!)

echo ""
echo "[run]   All processes started. Running for ${DURATION}s..."
sleep "$DURATION"

echo "[run]   Duration reached, stopping..."
cleanup
trap - EXIT INT TERM

echo ""
echo "=== Results ==="
echo ""

for name in manifest sub1/recv_log sub2/recv_log sub1/display_log sub2/display_log; do
    file="$RESULTS/${name}.csv"
    if [ -f "$file" ]; then
        lines=$(($(wc -l < "$file") - 1)) # subtract header
        echo "  $name.csv: $lines rows"
    else
        echo "  $name.csv: MISSING"
    fi
done

echo ""
echo "--- Publisher manifest (first 5 data rows) ---"
head -6 "$RESULTS/manifest.csv" 2>/dev/null | column -t -s, || echo "(empty)"

echo ""
echo "--- Sub 1 recv_log (first 5 data rows) ---"
head -6 "$RESULTS/sub1/recv_log.csv" 2>/dev/null | column -t -s, || echo "(empty)"

echo ""
echo "--- Sub 2 recv_log (first 5 data rows) ---"
head -6 "$RESULTS/sub2/recv_log.csv" 2>/dev/null | column -t -s, || echo "(empty)"

echo ""
echo "--- Sub 1 display_log (first 5 data rows) ---"
head -6 "$RESULTS/sub1/display_log.csv" 2>/dev/null | column -t -s, || echo "(empty)"

echo ""
echo "--- Sub 2 display_log (first 5 data rows) ---"
head -6 "$RESULTS/sub2/display_log.csv" 2>/dev/null | column -t -s, || echo "(empty)"

echo ""
echo "Full results in: $RESULTS"
echo "Logs: publisher.log, relay.log, sub{1,2}/subscriber.log"
