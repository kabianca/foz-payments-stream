#!/usr/bin/env bash
# The whole argument, end to end, on a running stack:
#   1. produce a misbehaving stream (late, out of order, duplicated)
#   2. kill -9 the Spark JVM while it is still ingesting
#   3. let Docker restart it from the checkpoint
#   4. produce more
#   5. check every invariant over the Delta tables
set -euo pipefail
cd "$(dirname "$0")/.."

PYTHON=${PYTHON:-.venv/bin/python}
export FOZ_DELTA_ROOT=data/delta
check() { "$PYTHON" -m foz.check "$@" 2>/dev/null; }

step() { printf '\n\033[1;36m== %s\033[0m\n' "$*"; }

BASE=$(check --ingested)
step "0/5 $BASE rows already ingested; the proof counts from there"

step "1/5 producing 400 events: 10% late, 5% duplicated, shuffled (seed 1)"
docker compose run --rm producer --count 400 --rate 40 --late 0.10 --dup 0.05 --seed 1

step "2/5 waiting until the job has ingested them"
check --wait-for-rows $((BASE + 400)) --timeout 240 --status

step "3/5 producing 400 more (seed 2) and killing the JVM while they land"
docker compose run --rm -d producer --count 400 --rate 40 --late 0.10 --dup 0.05 --seed 2 > /dev/null
sleep 6
docker compose exec stream pkill -9 java || true
echo "killed the driver; Docker is restarting it from the checkpoint"

step "4/5 waiting until all $((BASE + 800)) rows are accounted for"
check --wait-for-rows $((BASE + 800)) --timeout 300 --status

step "5/5 invariants"
check
