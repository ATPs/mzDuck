"""MGF export for mzDuck."""

from __future__ import annotations

from pathlib import Path

import duckdb

from .reconstruction import mgf_title_for_scan
from .schema import table_exists

SELF_DESCRIBING_MGF_PARQUET_COLUMNS = {
    "scan_number",
    "title",
    "rt_seconds",
    "precursor_mz",
    "precursor_intensity",
    "precursor_charge",
    "mz_array",
    "intensity_array",
}

SELF_DESCRIBING_MGF_PARQUET_ERROR = (
    "Parquet input for export-mgf is supported only for self-describing "
    "mzduck mzml-mgf parquet output"
)


def rt_to_seconds(value, unit):
    if value is None:
        return None
    unit = (unit or "").lower()
    if unit in {"minute", "minutes", "min", "mins"}:
        return float(value) * 60.0
    if unit in {"second", "seconds", "sec", "secs", "s"}:
        return float(value)
    raise ValueError(f"Cannot convert retention time unit to seconds: {unit!r}")


def format_float(value):
    return format(float(value), ".15g")


def ensure_output_path(output_path, *, overwrite=False):
    path = Path(output_path)
    if path.exists():
        if not overwrite:
            raise FileExistsError(f"Output already exists: {path}")
        path.unlink()
    if path.parent and not path.parent.exists():
        raise FileNotFoundError(f"Output directory does not exist: {path.parent}")
    return path


def ensure_input_file(input_path):
    path = Path(input_path)
    if not path.exists():
        raise FileNotFoundError(f"Input file does not exist: {path}")
    if not path.is_file():
        raise ValueError(f"Input path is not a file: {path}")
    return path


def write_mgf_record(
    handle,
    *,
    title,
    rt_seconds,
    precursor_mz,
    precursor_intensity,
    precursor_charge,
    mz_array,
    intensity_array,
):
    handle.write("BEGIN IONS\n")
    handle.write(f"TITLE={title}\n")
    if precursor_mz is not None:
        pepmass = format_float(precursor_mz)
        if precursor_intensity is not None:
            pepmass += " " + format_float(precursor_intensity)
        handle.write(f"PEPMASS={pepmass}\n")
    if precursor_charge is not None:
        charge = int(precursor_charge)
        sign = "+" if charge >= 0 else "-"
        handle.write(f"CHARGE={abs(charge)}{sign}\n")
    if rt_seconds is not None:
        handle.write(f"RTINSECONDS={format_float(rt_seconds)}\n")
    for mz, intensity in zip(mz_array, intensity_array):
        handle.write(f"{format_float(mz)} {format_float(intensity)}\n")
    handle.write("END IONS\n\n")


def title_from_parquet(title_source, scan_number, precursor_charge):
    if title_source:
        charge = int(precursor_charge) if precursor_charge is not None else 0
        expected_suffix = f".{int(scan_number)}.{int(scan_number)}.{charge}"
        if str(title_source).endswith(expected_suffix):
            return str(title_source)
    return mgf_title_for_scan(
        {"mgf_title_source": title_source},
        scan_number,
        precursor_charge,
    )


def export_mgf(conn, output_path, *, overwrite=False):
    if not table_exists(conn, "mgf"):
        raise ValueError("This mzDuck file does not contain an mgf table")

    path = ensure_output_path(output_path, overwrite=overwrite)
    metadata = dict(conn.execute("SELECT key, value FROM run_metadata").fetchall())
    rt_unit = metadata.get("rt_unit")
    cursor = conn.execute(
        """
        SELECT
            scan_number,
            rt,
            precursor_mz,
            precursor_intensity,
            precursor_charge,
            mz_array,
            intensity_array
        FROM mgf
        ORDER BY scan_number
        """
    )

    with path.open("w", encoding="utf-8", newline="\n") as handle:
        while True:
            row = cursor.fetchone()
            if row is None:
                break
            (
                scan_number,
                rt,
                precursor_mz,
                precursor_intensity,
                precursor_charge,
                mz_array,
                intensity_array,
            ) = row
            title = mgf_title_for_scan(metadata, scan_number, precursor_charge)
            write_mgf_record(
                handle,
                title=title,
                rt_seconds=rt_to_seconds(rt, rt_unit),
                precursor_mz=precursor_mz,
                precursor_intensity=precursor_intensity,
                precursor_charge=precursor_charge,
                mz_array=mz_array,
                intensity_array=intensity_array,
            )
    return path


def export_mgf_parquet(parquet_path, output_path, *, overwrite=False):
    parquet = ensure_input_file(parquet_path)
    conn = duckdb.connect()
    try:
        validate_self_describing_mgf_parquet(conn, parquet)
        path = ensure_output_path(output_path, overwrite=overwrite)
        cursor = conn.execute(
            """
            SELECT
                scan_number,
                title,
                rt_seconds,
                precursor_mz,
                precursor_intensity,
                precursor_charge,
                mz_array,
                intensity_array
            FROM read_parquet(?)
            ORDER BY scan_number
            """,
            [str(parquet)],
        )
        with path.open("w", encoding="utf-8", newline="\n") as handle:
            while True:
                row = cursor.fetchone()
                if row is None:
                    break
                (
                    scan_number,
                    title_source,
                    rt_seconds,
                    precursor_mz,
                    precursor_intensity,
                    precursor_charge,
                    mz_array,
                    intensity_array,
                ) = row
                write_mgf_record(
                    handle,
                    title=title_from_parquet(title_source, scan_number, precursor_charge),
                    rt_seconds=rt_seconds,
                    precursor_mz=precursor_mz,
                    precursor_intensity=precursor_intensity,
                    precursor_charge=precursor_charge,
                    mz_array=mz_array,
                    intensity_array=intensity_array,
                )
    finally:
        conn.close()
    return path


def validate_self_describing_mgf_parquet(conn, parquet_path: Path):
    columns = {
        row[0]
        for row in conn.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)",
            [str(parquet_path)],
        ).fetchall()
    }
    missing = SELF_DESCRIBING_MGF_PARQUET_COLUMNS - columns
    if missing:
        raise ValueError(
            f"{SELF_DESCRIBING_MGF_PARQUET_ERROR}; missing column(s): "
            + ", ".join(sorted(missing))
        )
    mismatched = conn.execute(
        """
        SELECT COUNT(*)
        FROM read_parquet(?)
        WHERE len(mz_array) != len(intensity_array)
        """,
        [str(parquet_path)],
    ).fetchone()[0]
    if mismatched:
        raise ValueError(
            f"{SELF_DESCRIBING_MGF_PARQUET_ERROR}; "
            f"{mismatched} spectra have mismatched mz/intensity arrays"
        )
