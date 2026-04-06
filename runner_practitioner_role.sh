#!/bin/bash
set -euo pipefail

# Download ONLY PractitionerRole resources to NDJSON.

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

python download_cms_ndjson.py "$OUT_DIR" --resource-types PractitionerRole "$@"
