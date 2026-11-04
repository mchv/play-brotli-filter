#!/usr/bin/env bash
# Runs bench.LoadTest for every filter and endpoint, in a JVM of its own each, and repeats the whole series so that the
# noise of the machine can be averaged out: the order of the filters alternates from one series to the next.
# Usage: bench/load-test.sh [connections] [warm-up seconds] [seconds] [verify every n responses] [series] [filters] [endpoints]
# where filters and endpoints are lists separated by commas, for example gzip,br,br0,brq5 (see bench.LoadTest).
set -euo pipefail

connections="${1:-16}"
warm_up="${2:-3}"
seconds="${3:-6}"
verify="${4:-10}"
series="${5:-3}"
IFS=, read -r -a filter_list <<<"${6:-gzip,br}"
IFS=, read -r -a endpoint_list <<<"${7:-small,large,streamed,chunked,tiny}"

cd "$(dirname "$0")/.."

commands=()
for ((serie = 1; serie <= series; serie++)); do
	filters=("${filter_list[@]}")
	if ((serie % 2 == 0)); then
		filters=()
		for ((i = ${#filter_list[@]} - 1; i >= 0; i--)); do filters+=("${filter_list[i]}"); done
	fi
	for endpoint in "${endpoint_list[@]}"; do
		for filter in "${filters[@]}"; do
			commands+=("bench/runMain bench.LoadTest $filter $endpoint $connections $warm_up $seconds $verify")
		done
	done
done

sbt --server --no-server -batch "${commands[@]}" 2>&1 |
	sed 's/\x1b\[[0-9;]*[A-Za-z]//g' | { grep -E '^RESULT|^FAILURES|\[error\]' || [ $? -eq 1 ]; }
