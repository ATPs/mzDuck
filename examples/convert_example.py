"""Convert bundled example data with the Python API.

Run from the repository root:

    /data/p/anaconda3/bin/python examples/convert_example.py --outdir /tmp/mzduck-example
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
INPUT_MZML = ROOT / "mzduck" / "example_data" / "tiny.mzML"
sys.path.insert(0, str(ROOT))

from mzduck import MzDuckFile


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--outdir", default="/tmp/mzduck-example")
    args = parser.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    mzduck_path = outdir / "tiny.mzduck"
    mgf_path = outdir / "tiny.mgf"
    mzml_path = outdir / "tiny.roundtrip.mzML"

    db = MzDuckFile.from_mzml(
        INPUT_MZML,
        mzduck_path,
        overwrite=True,
        batch_size=1,
        compute_sha256=False,
    )
    try:
        print(db.inspect())
        db.to_mgf(mgf_path, overwrite=True)
        db.to_mzml(mzml_path, overwrite=True)
    finally:
        db.close()

    print(f"mzDuck: {mzduck_path}")
    print(f"MGF: {mgf_path}")
    print(f"mzML: {mzml_path}")


if __name__ == "__main__":
    main()
