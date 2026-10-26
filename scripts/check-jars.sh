#!/usr/bin/env bash
# Fail if an assembled brotli4s jar contains Scala library classes.
# Usage: scripts/check-jars.sh [repo-root]   (run `sbt +common/packageBin` first)
set -euo pipefail

root="${1:-.}"
[[ "$root" == -* ]] && root="./$root"

if [[ ! -d "$root/common/target" ]]; then
	echo "check-jars: $root/common/target not found (run sbt +common/packageBin first)" >&2
	exit 2
fi

jars=()
while IFS= read -r -d '' jar; do
	jars+=("$jar")
done < <(find "$root/common/target" -path '*/scala-*/brotli4s_*.jar' ! -name '*-sources.jar' ! -name '*-javadoc.jar' -print0)

if [[ ${#jars[@]} -eq 0 ]]; then
	echo "check-jars: no brotli4s jars found under $root/common/target" >&2
	exit 2
fi

status=0
for jar in "${jars[@]}"; do
	# grep exits 1 when nothing matches; only that exit is swallowed.
	count=$({ jar tf "$jar" | grep -E '^scala/' || [ $? -eq 1 ]; } | wc -l)
	if [[ "$count" -gt 0 ]]; then
		echo "FAIL $jar contains $count scala/ entries" >&2
		status=1
	else
		echo "OK   $jar"
	fi
done
exit "$status"
