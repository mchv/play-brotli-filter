#!/usr/bin/env bash
set -euo pipefail

sbt "clean; +compile; +testFull; +common/packageBin"
"$(dirname "$0")/check-jars.sh" "$(dirname "$0")/.."
