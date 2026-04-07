#!/bin/bash
set -euo pipefail

# Download ONLY Practitioner resources to NDJSON.
#
# Resume behavior:
# - Downloads are resumable via state under: <output_dir>/download_state/Practitioner/
# - Re-running this script will resume if state.json says status=in_progress.
#
# Usage:
#   ./runner_practitioner.sh [output_dir] [extra download_cms_ndjson.py args...]
#
# Examples:
#   ./runner_practitioner.sh
#   ./runner_practitioner.sh /tmp/cms_ndjson --count 500 --limit 1000

DEFAULT_OUT_DIR="/Users/tgda/2026_03_31_cms_first_pass_ndjson/"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  python download_cms_ndjson.py --help
  exit 0
fi

OUT_DIR="$DEFAULT_OUT_DIR"
if [[ $# -ge 1 ]]; then
  OUT_DIR="$1"
  shift
fi

python download_cms_ndjson.py "$OUT_DIR" --resource-types Practitioner "$@"
