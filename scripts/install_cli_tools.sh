#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   source scripts/install_cli_tools.sh
#
# What it does:
# - Ensures you have an activated Python environment (virtualenv/venv/conda)
# - Installs this repo in editable mode, which registers console scripts:
#     - fhir-tablesaw
#     - fhir-tablesaw-3tier
#
# Notes:
# - This script is meant to be *sourced* so the caller's environment PATH
#   reflects the venv's bin directory.

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  echo "ERROR: Please source this script (do not execute it):" >&2
  echo "  source scripts/install_cli_tools.sh" >&2
  exit 1
fi

if [[ -z "${VIRTUAL_ENV:-}" ]]; then
  echo "ERROR: No Python virtual environment detected (VIRTUAL_ENV is empty)." >&2
  echo "Activate a venv first, then re-run:" >&2
  echo "  python -m venv .venv && source .venv/bin/activate" >&2
  return 2
fi

echo "Using python: $(python -c 'import sys; print(sys.executable)')"
echo "Using pip:    $(python -m pip --version)"

echo "Installing repo in editable mode..."
python -m pip install -U pip
python -m pip install -e .

echo "\nInstalled console scripts (sanity check):"
command -v fhir-tablesaw >/dev/null && echo "  - fhir-tablesaw:      $(command -v fhir-tablesaw)" || true
command -v fhir-tablesaw-3tier >/dev/null && echo "  - fhir-tablesaw-3tier: $(command -v fhir-tablesaw-3tier)" || true

echo "\nTry:" 
echo "  fhir-tablesaw-3tier db-info"
echo "  fhir-tablesaw-3tier reset-db"
echo "  fhir-tablesaw-3tier slurp-ndh --fhir-server-url 'https://ndh-server.fast.hl7.org/fhir' --count 200"
