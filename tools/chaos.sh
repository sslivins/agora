#!/usr/bin/env bash
# Thin wrapper around `python -m chaos` so the script lives at a stable path
# referenced from plan.md (Phase 1 deliverables) and the Phase 5 runbooks.
#
# Usage:
#   tools/chaos.sh list
#   tools/chaos.sh run <scenario-name>
#   tools/chaos.sh run-all [--json] [--verbose]
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."
exec python -m chaos "$@"
