"""CLI wrapper: python scripts/live_probe.py [--with-proxy]  (logic lives in transcript_probe.py)."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # repo root, wherever it is run from

from transcript_probe import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
