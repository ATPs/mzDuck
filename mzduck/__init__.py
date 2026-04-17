"""mzDuck: DuckDB-backed storage for centroid MS2 mzML data."""

__version__ = "0.1.0"

from .file import MzDuckFile
from .examples import example_data_path


def from_mzml(
    mzml_path,
    output_path,
    *,
    overwrite=False,
    batch_size=5000,
    compression="zstd",
    compression_level=6,
    index_scan_number=False,
    compute_sha256=True,
):
    """Convert one mzML file into one mzDuck file."""
    return MzDuckFile.from_mzml(
        mzml_path,
        output_path,
        overwrite=overwrite,
        batch_size=batch_size,
        compression=compression,
        compression_level=compression_level,
        index_scan_number=index_scan_number,
        compute_sha256=compute_sha256,
    )


def open(path, read_only=True):
    """Open an existing mzDuck file."""
    return MzDuckFile.open(path, read_only=read_only)


def to_mgf(mzduck_path, output_path):
    """Export an mzDuck file to MGF."""
    handle = MzDuckFile.open(mzduck_path, read_only=True)
    try:
        return handle.to_mgf(output_path)
    finally:
        handle.close()


def to_mzml(mzduck_path, output_path, *, mz_precision=None, intensity_precision=None):
    """Export an mzDuck file to mzML."""
    handle = MzDuckFile.open(mzduck_path, read_only=True)
    try:
        return handle.to_mzml(
            output_path,
            mz_precision=mz_precision,
            intensity_precision=intensity_precision,
        )
    finally:
        handle.close()


__all__ = [
    "MzDuckFile",
    "__version__",
    "example_data_path",
    "from_mzml",
    "open",
    "to_mgf",
    "to_mzml",
]
