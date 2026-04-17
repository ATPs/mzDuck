"""mzDuck: DuckDB-backed storage for centroid MS2 mzML and mzMLb data."""

__version__ = "0.1.0"

from .file import MzDuckFile
from .examples import example_data_path


def from_mzml(mzml_path, output_path, *, overwrite=False, batch_size=5000):
    """Convert one mzML or mzMLb file into one mzDuck file."""
    return MzDuckFile.from_mzml(
        mzml_path,
        output_path,
        overwrite=overwrite,
        batch_size=batch_size,
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


def to_mzml(mzduck_path, output_path):
    """Export an mzDuck file to mzML."""
    handle = MzDuckFile.open(mzduck_path, read_only=True)
    try:
        return handle.to_mzml(output_path)
    finally:
        handle.close()


def to_mzmlb(mzduck_path, output_path):
    """Export an mzDuck file to mzMLb."""
    handle = MzDuckFile.open(mzduck_path, read_only=True)
    try:
        return handle.to_mzmlb(output_path)
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
    "to_mzmlb",
]
