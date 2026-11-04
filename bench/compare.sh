#!/usr/bin/env bash
# Measure two checkouts of this repository against each other on this machine:
#
#   bench/compare.sh <base checkout> <head checkout> [out dir] [series] [base filters] [head filters]
#
# runs FilterBench for the base then for the head, then `series` (3 by default) series of the load test
# (`load-test.sh 16 3 6 10 1`) alternating between the two, so that both see the same machine. The logs go to
# `out dir` (bench/results/<date> by default): base-filterbench.log, head-filterbench.log, base-load.log and
# head-load.log; `load-summary.sh` prints the medians of a load log. Compare each brotli row with the gzip row of its
# own log rather than across the two, the machine is rarely idle.
#
# Each checkout must be a separate clone or worktree: sbt 2 restores a compile that it has in its disk cache as links
# into the cache, which the JMH source generator cannot compile into, so a checkout whose compile is a cache hit of
# the other's must be cleaned first (`sbt clean`) or compiled with a cold cache.
set -euo pipefail
base=$(cd "${1:?base checkout}" && pwd)
head=$(cd "${2:?head checkout}" && pwd)
out=${3:-$(pwd)/bench/results/$(date +%Y-%m-%d)}
series=${4:-3}
base_filters=${5:-gzip,br}
head_filters=${6:-gzip,brq4,br}
bodies=strict4k,strict140k,streamed140k,chunked140k,chunkedTiny140k
mkdir -p "$out"
strip() { sed 's/\x1b\[[0-9;]*[A-Za-z]//g'; }
echo "== $(date) FilterBench base ($base)"
(cd "$base" && sbt -batch "bench/Jmh/run -p codec=none,gzip,br4,br5 -p body=$bodies bench.FilterBench") 2>&1 | strip > "$out/base-filterbench.log"
echo "== $(date) FilterBench head ($head)"
(cd "$head" && sbt -batch "bench/Jmh/run -p codec=none,gzip,br4,br5,br -p body=$bodies bench.FilterBench") 2>&1 | strip > "$out/head-filterbench.log"
for serie in $(seq 1 "$series"); do
  echo "== $(date) load test base, series $serie"
  (cd "$base" && bench/load-test.sh 16 3 6 10 1 "$base_filters") >> "$out/base-load.log" 2>&1
  echo "== $(date) load test head, series $serie"
  (cd "$head" && bench/load-test.sh 16 3 6 10 1 "$head_filters") >> "$out/head-load.log" 2>&1
done
echo "== $(date) done: $out"
grep -E '^FilterBench|SIZE' "$out/base-filterbench.log" "$out/head-filterbench.log" | sed "s#$out/##"
for side in base head; do echo "-- $side load test"; "$(dirname "$0")/load-summary.sh" "$out/$side-load.log"; done
