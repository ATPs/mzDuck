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


def export_mzml(
    conn,
    output_path,
    *,
    overwrite=False,
    mz_precision=None,
    intensity_precision=None,
):
    try:
        from psims.mzml import MzMLWriter
        from psims.document import ReferentialIntegrityWarning
    except ImportError as exc:
        raise RuntimeError("psims is required for mzML export") from exc

    path = ensure_output_path(output_path, overwrite=overwrite)
    metadata = dict(conn.execute("SELECT key, value FROM run_metadata").fetchall())
    mz_dtype = dtype_for_precision(
        mz_precision, "m/z", metadata.get("mz_array_storage_dtype")
    )
    intensity_dtype = dtype_for_precision(
        intensity_precision,
        "intensity",
        metadata.get("intensity_array_storage_dtype"),
    )
    run_id = metadata.get("run_id") or "mzduck_run"
    count = conn.execute("SELECT COUNT(*) FROM spectra").fetchone()[0]

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=ReferentialIntegrityWarning)
        with path.open("wb") as stream:
            with MzMLWriter(
                stream,
                close=False,
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
                        cursor = conn.execute(
                            "SELECT * FROM spectra ORDER BY scan_number"
                        )
                        columns = [item[0] for item in conn.description]
                        while True:
                            row = cursor.fetchone()
                            if row is None:
                                break
                            spectrum = dict(zip(columns, row))
                            mz_array = np.asarray(
                                spectrum["mz_array"], dtype=mz_dtype
                            )
                            intensity_array = np.asarray(
                                spectrum["intensity_array"], dtype=intensity_dtype
                            )
                            writer.write_spectrum(
                                mz_array=mz_array,
                                intensity_array=intensity_array,
                                encoding={
                                    "m/z array": mz_dtype,
                                    "intensity array": intensity_dtype,
                                },
                                id=reconstruct_native_id(spectrum, metadata),
                                polarity=polarity_param(metadata.get("polarity")),
                                centroided=metadata_bool(
                                    metadata.get("centroided"), True
                                ),
                                precursor_information=precursor_information(spectrum),
                                scan_start_time=rt_to_minutes(
                                    spectrum.get("rt"), metadata.get("rt_unit")
                                ),
                                params=spectrum_params(spectrum),
                                scan_params=scan_params(spectrum, metadata),
                                scan_window_list=scan_window_list(spectrum),
                            )
    return path


def dtype_for_precision(precision, label, storage_dtype):
    if precision is None:
        if storage_dtype == "FLOAT":
            return np.float32
        if storage_dtype == "DOUBLE":
            return np.float64
        raise ValueError(f"Unknown stored {label} dtype: {storage_dtype!r}")
    value = int(precision)
    if value == 32:
        return np.float32
    if value == 64:
        return np.float64
    raise ValueError(f"{label} precision must be 32 or 64, got {precision!r}")


def metadata_bool(value, default):
    if value is None:
        return default
    return str(value).lower() == "true"


def reconstruct_native_id(spectrum, metadata):
    native_id = spectrum.get("native_id")
    if native_id:
        return native_id
    template = metadata.get("native_id_template")
    if template:
        return template.format(scan_number=spectrum["scan_number"])
    return f"scan={spectrum['scan_number']}"


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


def scan_params(spectrum, metadata):
    params = []
    if spectrum.get("filter_string"):
        params.append({"filter string": spectrum["filter_string"]})
    if spectrum.get("ion_injection_time") is not None:
        params.append(
            {
                "name": "ion injection time",
                "value": spectrum["ion_injection_time"],
                "unitName": metadata.get("ion_injection_time_unit") or "millisecond",
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
