#!/usr/bin/env bash
# Measure two commits of this repository against each other on this machine:
#
#   bench/compare.sh <base ref> <head ref> [out dir] [series]
#
# checks out each ref in turn in this clone and runs FilterBench for it, then `series` (3 by default) series of the
# load test (`load-test.sh 16 3 6 0 1 gzip,br`, which does not decode the responses), alternating between the two refs
# so that both see the same machine. The working tree must be clean; the branch or commit it was on is checked out
# again at the end. The logs go to `out dir` (bench-results/<date> by default, which git ignores): base-filterbench.log,
# head-filterbench.log, base-load.log and head-load.log. Compare each brotli row with the gzip row of its own log, the
# machine is rarely idle.
#
# Each ref runs its own load-test.sh, which must take the filters argument and a verify argument of 0 (any commit since
# the load test learned to skip the verification).
#
# One clone rather than two: sbt 2 shares its disk cache between clones and restores a compile that the other clone
# made as links into the cache, which the JMH source generator cannot compile into. The bench module is cleaned before
# each FilterBench run, because the root `clean` does not reach it and the record of its last compile can name JMH
# classes that the other ref does not have.
set -euo pipefail

main() {
  local base_ref="${1:?usage: bench/compare.sh <base ref> <head ref> [out dir] [series]}"
  local head_ref="${2:?usage: bench/compare.sh <base ref> <head ref> [out dir] [series]}"
  local root
  root=$(git rev-parse --show-toplevel)
  local out="${3:-$root/bench-results/$(date +%Y-%m-%d)}"
  local series="${4:-3}"
  local bodies=strict4k,strict140k,streamed140k,chunked140k,chunkedTiny140k
  cd "$root"

  local base head
  base=$(git rev-parse --verify --quiet "$base_ref^{commit}") || {
    echo "Unknown ref: $base_ref" >&2
    exit 2
  }
  head=$(git rev-parse --verify --quiet "$head_ref^{commit}") || {
    echo "Unknown ref: $head_ref" >&2
    exit 2
  }
  if [[ -n $(git status --porcelain --untracked-files=no) ]]; then
    echo "The working tree has changes: commit or stash them, this script checks out other commits" >&2
    exit 2
  fi

  # The summary script of the starting ref, outside the tree that the checkouts change
  local tools start
  tools=$(mktemp -d)
  cp bench/load-summary.sh "$tools/"
  start=$(git symbolic-ref --quiet --short HEAD || git rev-parse HEAD)
  # shellcheck disable=SC2064 # expanded now: the trap runs after main has returned
  trap "git -C $(printf %q "$root") checkout -q $(printf %q "$start"); rm -rf -- $(printf %q "$tools")" EXIT

  mkdir -p -- "$out"
  local side ref serie
  for side in base head; do
    ref=$base
    [[ $side == head ]] && ref=$head
    checkout "$ref" "FilterBench"
    sbt --server --no-server -batch "bench/clean; bench/Jmh/run -p codec=none,gzip,br4,br5 -p body=$bodies bench.FilterBench" 2>&1 |
      strip >"$out/$side-filterbench.log"
  done
  for serie in $(seq 1 "$series"); do
    for side in base head; do
      ref=$base
      [[ $side == head ]] && ref=$head
      checkout "$ref" "load test, series $serie"
      bench/load-test.sh 16 3 6 0 1 gzip,br >>"$out/$side-load.log" 2>&1
    done
  done

  echo "== $(date) done: $out"
  { grep -hE '^FilterBench' "$out/base-filterbench.log" "$out/head-filterbench.log" || [ $? -eq 1 ]; }
  for side in base head; do
    echo "-- $side load test"
    "$tools/load-summary.sh" "$out/$side-load.log"
  done
}

checkout() {
  git checkout -q --detach "$1"
  echo "== $(date) $(git rev-parse --short HEAD): $2"
}

strip() { sed 's/\x1b\[[0-9;]*[A-Za-z]//g'; }

# The checkouts change this file: bash reads it as it runs, so the call and the exit are on the line it has read
main "$@"
exit
