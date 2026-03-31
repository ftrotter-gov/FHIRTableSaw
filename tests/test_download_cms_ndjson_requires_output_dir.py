import subprocess
import sys
from pathlib import Path


def test_download_cms_ndjson_requires_output_dir():
    script_path = Path(__file__).resolve().parents[1] / "download_cms_ndjson.py"

    # Run without output_dir positional arg. Argparse should fail with exit code 2.
    result = subprocess.run(
        [sys.executable, str(script_path)],
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "output_dir" in (result.stderr + result.stdout)
