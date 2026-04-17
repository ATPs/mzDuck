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
    spectra = conn.execute(
        """
        SELECT
            scan_id,
            native_id,
            rt,
            rt_unit,
            precursor_mz,
            precursor_charge,
            precursor_intensity
        FROM spectra
        ORDER BY scan_id
        """
    ).fetchall()

    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for (
            scan_id,
            native_id,
            rt,
            rt_unit,
            precursor_mz,
            precursor_charge,
            precursor_intensity,
        ) in spectra:
            handle.write("BEGIN IONS\n")
            handle.write(f"TITLE={native_id}\n")
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
            peaks = conn.execute(
                """
                SELECT mz, intensity
                FROM peaks
                WHERE scan_id = ?
                ORDER BY peak_index
                """,
                [scan_id],
            ).fetchall()
            for mz, intensity in peaks:
                handle.write(f"{format_float(mz)} {format_float(intensity)}\n")
            handle.write("END IONS\n\n")
    return path
