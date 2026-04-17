"""Query the bundled mzDuck example file."""

from __future__ import annotations

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "mzduck" / "example_data" / "tiny.mzduck"
sys.path.insert(0, str(ROOT))

from mzduck import MzDuckFile


def main() -> None:
    with MzDuckFile.open(DB_PATH) as db:
        print("Summary")
        print(db.inspect())

        print("\nSpectrum 1")
        spectrum = db.get_spectrum(1)
        print("native_id:", spectrum["native_id"])
        print("precursor_mz:", spectrum["precursor_mz"])
        print("mz:", spectrum["mz"].tolist())
        print("intensity:", spectrum["intensity"].tolist())

        print("\nPrecursor query")
        rows = db.query(
            """
            SELECT scan_number, rt, precursor_mz, precursor_charge
            FROM spectra
            WHERE precursor_mz BETWEEN ? AND ?
            ORDER BY rt
            """,
            [440.0, 450.0],
        ).fetchall()
        for row in rows:
            print(row)

        print("\nProduct-ion XIC around m/z 150")
        rows = db.query(
            """
            SELECT s.rt, SUM(p.intensity) AS xic
            FROM spectra s
            JOIN peaks p ON p.scan_number = s.scan_number
            WHERE p.mz BETWEEN ? AND ?
            GROUP BY s.rt
            ORDER BY s.rt
            """,
            [149.0, 151.0],
        ).fetchall()
        for row in rows:
            print(row)


if __name__ == "__main__":
    main()
