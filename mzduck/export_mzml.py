"""mzML export for mzDuck using psims."""

from __future__ import annotations

import json
import warnings

import numpy as np

from .export_mgf import ensure_output_path

SHORT_ACTIVATION_TO_TERM = {
    "HCD": "beam-type collision-induced dissociation",
    "CID": "collision-induced dissociation",
    "ETD": "electron transfer dissociation",
    "ECD": "electron capture dissociation",
    "IRMPD": "infrared multiphoton dissociation",
    "SID": "supplemental collision-induced dissociation",
}


def rt_to_minutes(value, unit):
    if value is None:
        return None
    unit = (unit or "").lower()
    if unit in {"minute", "minutes", "min", "mins"}:
        return float(value)
    if unit in {"second", "seconds", "sec", "secs", "s"}:
        return float(value) / 60.0
    raise ValueError(f"Cannot convert retention time unit to minutes: {unit!r}")


def export_mzml(conn, output_path, *, overwrite=False):
    try:
        from psims.mzml import MzMLWriter
        from psims.document import ReferentialIntegrityWarning
    except ImportError as exc:
        raise RuntimeError("psims is required for mzML export") from exc

    path = ensure_output_path(output_path, overwrite=overwrite)
    return export_with_writer(
        conn,
        path,
        writer_factory=lambda p: p.open("wb"),
        writer_class=MzMLWriter,
        close=False,
        referential_warning_class=ReferentialIntegrityWarning,
    )


def export_mzmlb(conn, output_path, *, overwrite=False):
    try:
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="hdf5plugin is missing.*",
                category=UserWarning,
            )
            from psims.document import ReferentialIntegrityWarning
            from psims.mzmlb import MzMLbWriter
    except ImportError as exc:
        raise RuntimeError("psims with mzMLb support is required for mzMLb export") from exc

    path = ensure_output_path(output_path, overwrite=overwrite)
    return export_with_writer(
        conn,
        path,
        writer_factory=lambda p: str(p),
        writer_class=MzMLbWriter,
        close=True,
        referential_warning_class=ReferentialIntegrityWarning,
    )


def export_with_writer(
    conn,
    path,
    *,
    writer_factory,
    writer_class,
    close,
    referential_warning_class,
):
    metadata = dict(conn.execute("SELECT key, value FROM run_metadata").fetchall())
    run_id = metadata.get("run_id") or "mzduck_run"
    count = conn.execute("SELECT COUNT(*) FROM spectra").fetchone()[0]

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=referential_warning_class)
        warnings.filterwarnings(
            "ignore",
            message="hdf5plugin is missing.*",
            category=UserWarning,
        )
        stream_or_path = writer_factory(path)
        try:
            with writer_class(
                stream_or_path,
                close=close,
                missing_reference_is_error=False,
            ) as writer:
                writer.register("Software", "mzduck")
                writer.controlled_vocabularies()
                writer.file_description(["MSn spectrum"])
                writer.software_list(
                    [
                        writer.Software(
                            version=metadata.get("mzduck_version") or "0.1.0",
                            id="mzduck",
                            params=["custom unreleased software tool", "python-psims"],
                        )
                    ]
                )
                writer.instrument_configuration_list(
                    [
                        writer.InstrumentConfiguration(
                            id="IC1",
                            component_list=writer.ComponentList(
                                [
                                    writer.Source(params=["electrospray ionization"], order=1),
                                    writer.Analyzer(params=["quadrupole"], order=2),
                                    writer.Detector(params=["inductive detector"], order=3),
                                ]
                            ),
                        )
                    ]
                )
                writer.data_processing_list(
                    [
                        writer.DataProcessing(
                            processing_methods=[
                                {
                                    "order": 0,
                                    "software_reference": "mzduck",
                                    "params": ["Conversion to mzML"],
                                }
                            ],
                            id="mzduck_processing",
                        )
                    ]
                )
                with writer.run(id=run_id):
                    with writer.spectrum_list(count=count):
                        spectra = conn.execute(
                            "SELECT * FROM spectra ORDER BY scan_id"
                        ).fetchall()
                        columns = [item[0] for item in conn.description]
                        for row in spectra:
                            spectrum = dict(zip(columns, row))
                            peaks = conn.execute(
                                """
                                SELECT mz, intensity
                                FROM peaks
                                WHERE scan_id = ?
                                ORDER BY peak_index
                                """,
                                [spectrum["scan_id"]],
                            ).fetchall()
                            mz_array = np.asarray(
                                [peak[0] for peak in peaks], dtype=np.float64
                            )
                            intensity_array = np.asarray(
                                [peak[1] for peak in peaks], dtype=np.float32
                            )
                            writer.write_spectrum(
                                mz_array=mz_array,
                                intensity_array=intensity_array,
                                id=spectrum["native_id"],
                                polarity=polarity_param(spectrum.get("polarity")),
                                centroided=bool_or_default(
                                    spectrum.get("centroided"), True
                                ),
                                precursor_information=precursor_information(spectrum),
                                scan_start_time=rt_to_minutes(
                                    spectrum.get("rt"), spectrum.get("rt_unit")
                                ),
                                params=spectrum_params(spectrum),
                                scan_params=scan_params(spectrum),
                                scan_window_list=scan_window_list(spectrum),
                            )
        finally:
            close_stream = getattr(stream_or_path, "close", None)
            if callable(close_stream) and not close:
                close_stream()
    return path


def bool_or_default(value, default):
    if value is None:
        return default
    return bool(value)


def polarity_param(value):
    if value == "positive":
        return "positive scan"
    if value == "negative":
        return "negative scan"
    return None


def spectrum_params(spectrum):
    params = [{"ms level": 2}]
    if spectrum.get("tic") is not None:
        params.append({"total ion current": spectrum["tic"]})
    if spectrum.get("base_peak_mz") is not None:
        params.append({"base peak m/z": spectrum["base_peak_mz"]})
    if spectrum.get("base_peak_intensity") is not None:
        params.append(
            {
                "name": "base peak intensity",
                "value": spectrum["base_peak_intensity"],
                "unit_accession": "MS:1000131",
            }
        )
    if spectrum.get("lowest_mz") is not None:
        params.append({"lowest observed m/z": spectrum["lowest_mz"]})
    if spectrum.get("highest_mz") is not None:
        params.append({"highest observed m/z": spectrum["highest_mz"]})
    return params


def scan_params(spectrum):
    params = []
    if spectrum.get("filter_string"):
        params.append({"filter string": spectrum["filter_string"]})
    if spectrum.get("ion_injection_time") is not None:
        params.append(
            {
                "name": "ion injection time",
                "value": spectrum["ion_injection_time"],
                "unitName": spectrum.get("ion_injection_time_unit") or "millisecond",
            }
        )
    if spectrum.get("monoisotopic_mz") is not None:
        params.append(
            {
                "name": "[Thermo Trailer Extra]Monoisotopic M/Z:",
                "value": spectrum["monoisotopic_mz"],
            }
        )
    return params


def scan_window_list(spectrum):
    lower = spectrum.get("scan_window_lower")
    upper = spectrum.get("scan_window_upper")
    if lower is None or upper is None:
        return []
    return [(lower, upper)]


def precursor_information(spectrum):
    precursor_mz = spectrum.get("precursor_mz")
    precursor_intensity = spectrum.get("precursor_intensity")
    precursor_charge = spectrum.get("precursor_charge")
    has_selected_ion = any(
        value is not None
        for value in (precursor_mz, precursor_intensity, precursor_charge)
    )
    isolation = isolation_window(spectrum)
    activation = activation_params(spectrum)
    if not has_selected_ion and isolation is None and activation is None:
        return None
    info = {}
    if precursor_mz is not None:
        info["mz"] = precursor_mz
    if precursor_intensity is not None:
        info["intensity"] = precursor_intensity
    if precursor_charge is not None:
        info["charge"] = int(precursor_charge)
    if spectrum.get("spectrum_ref"):
        info["spectrum_reference"] = spectrum["spectrum_ref"]
    if isolation is not None:
        info["isolation_window"] = isolation
    if activation is not None:
        info["activation"] = activation
    return info


def isolation_window(spectrum):
    target = spectrum.get("isolation_window_target")
    lower = spectrum.get("isolation_window_lower")
    upper = spectrum.get("isolation_window_upper")
    if target is None and lower is None and upper is None:
        return None
    return {"target": target, "lower": lower, "upper": upper}


def activation_params(spectrum):
    params = []
    activation_type = spectrum.get("activation_type")
    if activation_type:
        params.append(SHORT_ACTIVATION_TO_TERM.get(activation_type, activation_type))
    if spectrum.get("collision_energy") is not None:
        params.append({"collision energy": spectrum["collision_energy"]})
    if params:
        return params
    raw = spectrum.get("activation_cv")
    if raw:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return None
        for key in data:
            if key != "collision energy":
                return [key]
    return None
