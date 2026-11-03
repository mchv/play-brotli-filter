#!/usr/bin/env bash
# Runs bench.LoadTest for every filter and endpoint, in a JVM of its own each, and repeats the whole series so that the
# noise of the machine can be averaged out: the order of the filters alternates from one series to the next.
# Usage: bench/load-test.sh [connections] [warm-up seconds] [seconds] [verify every n responses] [series]
set -euo pipefail

connections="${1:-16}"
warm_up="${2:-3}"
seconds="${3:-6}"
verify="${4:-10}"
series="${5:-3}"

cd "$(dirname "$0")/.."

commands=()
for ((serie = 1; serie <= series; serie++)); do
	filters=(gzip br)
	((serie % 2 == 0)) && filters=(br gzip)
	for endpoint in small large streamed chunked tiny; do
		for filter in "${filters[@]}"; do
			commands+=("bench/runMain bench.LoadTest $filter $endpoint $connections $warm_up $seconds $verify")
		done
	done
done

sbt --server --no-server -batch "${commands[@]}" 2>&1 |
	sed 's/\x1b\[[0-9;]*[A-Za-z]//g' | { grep -E '^RESULT|\[error\]' || [ $? -eq 1 ]; }
