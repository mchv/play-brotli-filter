#!/usr/bin/env bash
# Summarizes the RESULT lines of bench/load-test.sh: the median of each figure over the series, by endpoint and filter.
# Usage: bench/load-summary.sh <log>
set -euo pipefail

log="${1:?usage: load-summary.sh <log>}"
if grep -q '^FAILURES' "$log"; then
	echo "THE LOAD TEST REPORTED FAILURES:" >&2
	grep '^FAILURES' "$log" >&2
fi

grep -E '^RESULT' "$log" | sed -E 's/ +/ /g; s/= /=/g; s/us//g; s/KB//g; s/ms\/s//g' | awk '
	function median(name, key,   n, i, j, t, a) {
		n = count[key]
		for (i = 1; i <= n; i++) a[i] = values[name, key, i]
		for (i = 1; i <= n; i++) for (j = i + 1; j <= n; j++) if (a[j] < a[i]) { t = a[i]; a[i] = a[j]; a[j] = t }
		return (n % 2) ? a[(n + 1) / 2] : (a[n / 2] + a[n / 2 + 1]) / 2
	}
	{
		delete f
		for (i = 2; i <= NF; i++) { split($i, kv, "="); f[kv[1]] = kv[2] }
		key = f["endpoint"] " " f["filter"]
		if (!(key in count)) { order[++keys] = key }
		count[key]++
		n = count[key]
		split("rps p50 p95 p99 bytes/resp cpu/req alloc/req gc failures", names, " ")
		for (m in names) values[names[m], key, n] = f[names[m]]
		failures[key] += f["failures"]
	}
	END {
		printf "%-9s %-5s %6s %9s %9s %9s %7s %8s %8s %5s %4s\n", "endpoint", "filter", "runs", "req/s", "p50 us", "p99 us", "bytes", "cpu us", "alloc KB", "gc", "fail"
		for (k = 1; k <= keys; k++) {
			key = order[k]; split(key, e, " ")
			printf "%-9s %-5s %6d %9.0f %9.0f %9.0f %7.0f %8.0f %8.0f %5.0f %4d\n", e[1], e[2], count[key],
				median("rps", key), median("p50", key), median("p99", key), median("bytes/resp", key), median("cpu/req", key), median("alloc/req", key), median("gc", key), failures[key]
		}
	}'
