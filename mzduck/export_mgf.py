"""MGF export for mzDuck."""

from __future__ import annotations

from pathlib import Path

from .schema import table_exists


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


def export_mgf(conn, output_path, *, overwrite=False):
    if not table_exists(conn, "mgf"):
        raise ValueError("This mzDuck file does not contain an mgf table")

    path = ensure_output_path(output_path, overwrite=overwrite)
    metadata = dict(conn.execute("SELECT key, value FROM run_metadata").fetchall())
    rt_unit = metadata.get("rt_unit")
    cursor = conn.execute(
        """
        SELECT
            title,
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
                title,
                rt,
                precursor_mz,
                precursor_intensity,
                precursor_charge,
                mz_array,
                intensity_array,
            ) = row
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
            seconds = rt_to_seconds(rt, rt_unit)
            if seconds is not None:
                handle.write(f"RTINSECONDS={format_float(seconds)}\n")
            for mz, intensity in zip(mz_array, intensity_array):
                handle.write(f"{format_float(mz)} {format_float(intensity)}\n")
            handle.write("END IONS\n\n")
    return path
