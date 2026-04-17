"""Regenerate the bundled tiny mzDuck example data.

Run from the repository root:

    /data/p/anaconda3/bin/python examples/make_example_data.py
"""

from __future__ import annotations

from pathlib import Path
import sys

import numpy as np
from psims.mzml import MzMLWriter
from psims.transform.mzml import MzMLToMzMLb


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "mzduck" / "example_data"
sys.path.insert(0, str(ROOT))

from mzduck import MzDuckFile


def write_tiny_mzml(path: Path) -> None:
    with path.open("wb") as stream:
        with MzMLWriter(stream, close=False, missing_reference_is_error=False) as out:
            out.register("Software", "mzduck-example")
            out.controlled_vocabularies()
            out.file_description(["MSn spectrum"])
            out.software_list(
                [
                    out.Software(
                        version="0.1.0",
                        id="mzduck-example",
                        params=["custom unreleased software tool", "python-psims"],
                    )
                ]
            )
            out.instrument_configuration_list(
                [
                    out.InstrumentConfiguration(
                        id="IC1",
                        component_list=out.ComponentList(
                            [
                                out.Source(params=["electrospray ionization"], order=1),
                                out.Analyzer(params=["quadrupole"], order=2),
                                out.Detector(params=["inductive detector"], order=3),
                            ]
                        ),
                    )
                ]
            )
            out.data_processing_list(
                [
                    out.DataProcessing(
                        processing_methods=[
                            {
                                "order": 0,
                                "software_reference": "mzduck-example",
                                "params": ["Conversion to mzML"],
                            }
                        ],
                        id="mzduck-example-processing",
                    )
                ]
            )
            with out.run(id="tiny_run"):
                with out.spectrum_list(count=2):
                    out.write_spectrum(
                        mz_array=np.asarray([100.0, 150.5, 200.25], dtype=np.float32),
                        intensity_array=np.asarray([10.0, 50.0, 25.0], dtype=np.float32),
                        id="controllerType=0 controllerNumber=1 scan=1",
                        polarity="positive scan",
                        centroided=True,
                        scan_start_time=1.5,
                        params=[
                            {"ms level": 2},
                            {"total ion current": 85.0},
                            {"base peak m/z": 150.5},
                            {
                                "name": "base peak intensity",
                                "value": 50.0,
                                "unit_accession": "MS:1000131",
                            },
                            {"lowest observed m/z": 100.0},
                            {"highest observed m/z": 200.25},
                        ],
                        scan_window_list=[(90.0, 1000.0)],
                        precursor_information={
                            "mz": 445.34,
                            "intensity": 1200.0,
                            "charge": 2,
                            "activation": [
                                "beam-type collision-induced dissociation",
                                {"collision energy": 27.5},
                            ],
                            "isolation_window": {
                                "target": 445.34,
                                "lower": 0.5,
                                "upper": 0.5,
                            },
                        },
                    )
                    out.write_spectrum(
                        mz_array=np.asarray([300.0, 250.0], dtype=np.float32),
                        intensity_array=np.asarray([5.0, 15.0], dtype=np.float32),
                        id="controllerType=0 controllerNumber=1 scan=2",
                        polarity="positive scan",
                        centroided=True,
                        scan_start_time=2.0,
                        params=[
                            {"ms level": 2},
                            {"total ion current": 20.0},
                            {"base peak m/z": 250.0},
                            {
                                "name": "base peak intensity",
                                "value": 15.0,
                                "unit_accession": "MS:1000131",
                            },
                        ],
                        scan_window_list=[(100.0, 900.0)],
                        precursor_information={
                            "mz": 550.2,
                            "intensity": 900.0,
                            "charge": 3,
                            "activation": [
                                "collision-induced dissociation",
                                {"collision energy": 35.0},
                            ],
                            "isolation_window": {
                                "target": 550.2,
                                "lower": 0.7,
                                "upper": 0.8,
                            },
                        },
                    )


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    mzml_path = DATA_DIR / "tiny.mzML"
    mzmlb_path = DATA_DIR / "tiny.mzMLb"
    mzduck_path = DATA_DIR / "tiny.mzduck"
    mgf_path = DATA_DIR / "tiny.mgf"

    write_tiny_mzml(mzml_path)
    MzMLToMzMLb(str(mzml_path), str(mzmlb_path)).write()
    db = MzDuckFile.from_mzml(
        mzml_path,
        mzduck_path,
        overwrite=True,
        batch_size=1,
        compute_sha256=False,
    )
    try:
        db.to_mgf(mgf_path, overwrite=True)
    finally:
        db.close()

    print(f"Wrote {mzml_path}")
    print(f"Wrote {mzmlb_path}")
    print(f"Wrote {mzduck_path}")
    print(f"Wrote {mgf_path}")


if __name__ == "__main__":
    main()
