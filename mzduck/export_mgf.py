"""MGF export for mzDuck."""

from __future__ import annotations

from pathlib import Path


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
    path = ensure_output_path(output_path, overwrite=overwrite)
    metadata = dict(conn.execute("SELECT key, value FROM run_metadata").fetchall())
    rt_unit = metadata.get("rt_unit")
    native_id_template = metadata.get("native_id_template")
    cursor = conn.execute(
        """
        SELECT
            scan_number,
            native_id,
            rt,
            precursor_mz,
            precursor_charge,
            precursor_intensity,
            mz_array,
            intensity_array
        FROM spectra
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
                native_id,
                rt,
                precursor_mz,
                precursor_charge,
                precursor_intensity,
                mz_array,
                intensity_array,
            ) = row
            handle.write("BEGIN IONS\n")
            title = reconstruct_native_id(native_id, native_id_template, scan_number)
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


def reconstruct_native_id(native_id, native_id_template, scan_number):
    if native_id:
        return native_id
    if native_id_template:
        return native_id_template.format(scan_number=scan_number)
    return f"scan={scan_number}"
