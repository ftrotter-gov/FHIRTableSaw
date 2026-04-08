"""Pytest configuration.

These tests exercise repo-root scripts (e.g. `verify_fhir_download.py`) and
repo-root utility packages (e.g. `util/`).

Depending on how pytest determines `rootdir`, the repository root may not be on
`sys.path` by default. Ensure it is present so imports like `import util...` and
`import verify_fhir_download` work reliably.
"""

from __future__ import annotations

import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
