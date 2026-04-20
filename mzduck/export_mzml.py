"""mzML export for mzDuck using psims."""

from __future__ import annotations

import warnings
from copy import deepcopy

import numpy as np

from .export_mgf import ensure_output_path
from .reconstruction import promote_structural_scan_fields, reconstruct_text_field
from .schema import (
    get_table_columns,
    msn_levels_present,
    msn_table_name,
    schema_version,
    table_exists,
)

SHORT_ACTIVATION_TO_TERM = {
    "HCD": "beam-type collision-induced dissociation",
    "CID": "collision-induced dissociation",
    "ETD": "electron transfer dissociation",
    "ECD": "electron capture dissociation",
    "IRMPD": "infrared multiphoton dissociation",
    "SID": "supplemental collision-induced dissociation",
}

EXPORT_COLUMNS = [
    "scan_number",
    "source_index",
    "instrument_configuration_ref",
    "native_id",
    "ms_level",
    "rt",
    "precursor_mz",
    "precursor_charge",
    "precursor_intensity",
    "collision_energy",
    "activation_type",
    "isolation_window_target",
    "isolation_window_lower",
    "isolation_window_upper",
    "spectrum_ref",
    "precursor_scan_number",
    "base_peak_mz",
    "base_peak_intensity",
    "tic",
    "lowest_mz",
    "highest_mz",
    "filter_string",
    "ion_injection_time",
    "monoisotopic_mz",
    "scan_window_lower",
    "scan_window_upper",
    "mz_array",
    "intensity_array",
]


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
        from psims.document import ReferentialIntegrityWarning
        from psims.mzml import MzMLWriter
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
    count = export_spectrum_count(conn)
    if count == 0:
        raise ValueError("This mzDuck file does not contain exportable spectra")
    text_overrides = load_text_overrides(conn)
    extra_params = load_extra_params(conn)
    instrument_configuration_refs = {}

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
                writer.file_description(file_description_params(conn))
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
                        for spectrum in iter_export_spectra(
                            conn,
                            metadata=metadata,
                            text_overrides=text_overrides,
                            extra_params=extra_params,
                        ):
                            if spectrum.get("instrument_configuration_ref"):
                                instrument_configuration_refs[spectrum["native_id"]] = (
                                    spectrum["instrument_configuration_ref"]
                                )
                            mz_array = np.asarray(spectrum["mz_array"], dtype=mz_dtype)
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
                                id=spectrum["native_id"],
                                polarity=polarity_param(metadata.get("polarity")),
                                centroided=metadata_bool(
                                    metadata.get("centroided"), True
                                ),
                                precursor_information=precursor_information(
                                    writer, spectrum
                                ),
                                scan_start_time=rt_to_minutes(
                                    spectrum.get("rt"), metadata.get("rt_unit")
                                ),
                                params=spectrum_params(spectrum),
                                scan_params=scan_params(spectrum, metadata),
                                scan_window_list=scan_window_list(spectrum),
                                instrument_configuration_id=spectrum.get(
                                    "instrument_configuration_ref"
                                ),
                            )
    restore_original_header_fragments(
        path,
        metadata,
        instrument_configuration_refs=instrument_configuration_refs,
    )
    return path


def export_spectrum_count(conn):
    count = 0
    if table_exists(conn, "ms1_spectra"):
        count += conn.execute("SELECT COUNT(*) FROM ms1_spectra").fetchone()[0]
    if table_exists(conn, "mgf"):
        count += conn.execute("SELECT COUNT(*) FROM mgf").fetchone()[0]
    elif table_exists(conn, "ms2_spectra"):
        count += conn.execute("SELECT COUNT(*) FROM ms2_spectra").fetchone()[0]
    for level in msn_levels_present(conn):
        count += conn.execute(
            f"SELECT COUNT(*) FROM {msn_table_name(level)}"
        ).fetchone()[0]
    return int(count)


def file_description_params(conn):
    params = []
    if table_exists(conn, "ms1_spectra") and conn.execute(
        "SELECT COUNT(*) FROM ms1_spectra"
    ).fetchone()[0]:
        params.append("MS1 spectrum")
    if table_exists(conn, "mgf") and conn.execute(
        "SELECT COUNT(*) FROM mgf"
    ).fetchone()[0]:
        params.append("MSn spectrum")
    elif table_exists(conn, "ms2_spectra") and conn.execute(
        "SELECT COUNT(*) FROM ms2_spectra"
    ).fetchone()[0]:
        params.append("MSn spectrum")
    for level in msn_levels_present(conn):
        if conn.execute(f"SELECT COUNT(*) FROM {msn_table_name(level)}").fetchone()[0]:
            params.append("MSn spectrum")
            break
    return params or ["MSn spectrum"]


def iter_export_spectra(conn, *, metadata, text_overrides, extra_params):
    selects = []
    if table_exists(conn, "ms1_spectra"):
        ms1_columns = set(get_table_columns(conn, "ms1_spectra"))
        instrument_config_expr = (
            "instrument_configuration_ref"
            if "instrument_configuration_ref" in ms1_columns
            else "CAST(NULL AS VARCHAR) AS instrument_configuration_ref"
        )
        selects.append(
            f"""
            SELECT
                scan_number,
                source_index,
                {instrument_config_expr},
                native_id,
                ms_level,
                rt,
                CAST(NULL AS DOUBLE) AS precursor_mz,
                CAST(NULL AS TINYINT) AS precursor_charge,
                CAST(NULL AS FLOAT) AS precursor_intensity,
                CAST(NULL AS FLOAT) AS collision_energy,
                CAST(NULL AS VARCHAR) AS activation_type,
                CAST(NULL AS DOUBLE) AS isolation_window_target,
                CAST(NULL AS FLOAT) AS isolation_window_lower,
                CAST(NULL AS FLOAT) AS isolation_window_upper,
                CAST(NULL AS VARCHAR) AS spectrum_ref,
                CAST(NULL AS INTEGER) AS precursor_scan_number,
                base_peak_mz,
                base_peak_intensity,
                tic,
                lowest_mz,
                highest_mz,
                filter_string,
                ion_injection_time,
                CAST(NULL AS DOUBLE) AS monoisotopic_mz,
                scan_window_lower,
                scan_window_upper,
                mz_array,
                intensity_array
            FROM ms1_spectra
            """
        )
    if v2_ms2_storage(conn):
        ms2_columns = set(get_table_columns(conn, "ms2_spectra"))
        instrument_config_expr = (
            "d.instrument_configuration_ref"
            if "instrument_configuration_ref" in ms2_columns
            else "CAST(NULL AS VARCHAR) AS instrument_configuration_ref"
        )
        selects.append(
            f"""
            SELECT
                m.scan_number,
                m.source_index,
                {instrument_config_expr},
                CAST(NULL AS VARCHAR) AS native_id,
                2 AS ms_level,
                m.rt,
                m.precursor_mz,
                m.precursor_charge,
                m.precursor_intensity,
                d.collision_energy,
                d.activation_type,
                d.isolation_window_target,
                d.isolation_window_lower,
                d.isolation_window_upper,
                CAST(NULL AS VARCHAR) AS spectrum_ref,
                d.precursor_scan_number,
                d.base_peak_mz,
                d.base_peak_intensity,
                d.tic,
                d.lowest_mz,
                d.highest_mz,
                CAST(NULL AS VARCHAR) AS filter_string,
                d.ion_injection_time,
                d.monoisotopic_mz,
                d.scan_window_lower,
                d.scan_window_upper,
                m.mz_array,
                m.intensity_array
            FROM mgf m
            LEFT JOIN ms2_spectra d USING (scan_number)
            """
        )
    elif table_exists(conn, "mgf"):
        if table_exists(conn, "ms2_spectra") and schema_version(conn) == "1":
            selects.append(
                """
                SELECT
                    m.scan_number,
                    CAST(NULL AS INTEGER) AS source_index,
                    CAST(NULL AS VARCHAR) AS instrument_configuration_ref,
                    d.native_id,
                    2 AS ms_level,
                    COALESCE(d.rt, m.rt) AS rt,
                    m.precursor_mz,
                    m.precursor_charge,
                    m.precursor_intensity,
                    d.collision_energy,
                    d.activation_type,
                    d.isolation_window_target,
                    d.isolation_window_lower,
                    d.isolation_window_upper,
                    d.spectrum_ref,
                    CAST(NULL AS INTEGER) AS precursor_scan_number,
                    d.base_peak_mz,
                    d.base_peak_intensity,
                    d.tic,
                    d.lowest_mz,
                    d.highest_mz,
                    d.filter_string,
                    d.ion_injection_time,
                    d.monoisotopic_mz,
                    d.scan_window_lower,
                    d.scan_window_upper,
                    m.mz_array,
                    m.intensity_array
                FROM mgf m
                LEFT JOIN ms2_spectra d USING (scan_number)
                """
            )
        else:
            selects.append(
                """
                SELECT
                    scan_number,
                    source_index,
                    CAST(NULL AS VARCHAR) AS instrument_configuration_ref,
                    CAST(NULL AS VARCHAR) AS native_id,
                    2 AS ms_level,
                    rt,
                    precursor_mz,
                    precursor_charge,
                    precursor_intensity,
                    CAST(NULL AS FLOAT) AS collision_energy,
                    CAST(NULL AS VARCHAR) AS activation_type,
                    precursor_mz AS isolation_window_target,
                    CAST(NULL AS FLOAT) AS isolation_window_lower,
                    CAST(NULL AS FLOAT) AS isolation_window_upper,
                    CAST(NULL AS VARCHAR) AS spectrum_ref,
                    CAST(NULL AS INTEGER) AS precursor_scan_number,
                    CAST(NULL AS FLOAT) AS base_peak_mz,
                    CAST(NULL AS FLOAT) AS base_peak_intensity,
                    CAST(NULL AS FLOAT) AS tic,
                    CAST(NULL AS FLOAT) AS lowest_mz,
                    CAST(NULL AS FLOAT) AS highest_mz,
                    CAST(NULL AS VARCHAR) AS filter_string,
                    CAST(NULL AS FLOAT) AS ion_injection_time,
                    CAST(NULL AS DOUBLE) AS monoisotopic_mz,
                    CAST(NULL AS FLOAT) AS scan_window_lower,
                    CAST(NULL AS FLOAT) AS scan_window_upper,
                    mz_array,
                    intensity_array
                FROM mgf
                """
            )
    for level in msn_levels_present(conn):
        table_name = msn_table_name(level)
        columns = set(get_table_columns(conn, table_name))
        instrument_config_expr = (
            "instrument_configuration_ref"
            if "instrument_configuration_ref" in columns
            else "CAST(NULL AS VARCHAR) AS instrument_configuration_ref"
        )
        if "precursor_scan_number" in columns:
            selects.append(
                f"""
                SELECT
                    scan_number,
                    source_index,
                    {instrument_config_expr},
                    CAST(NULL AS VARCHAR) AS native_id,
                    ms_level,
                    rt,
                    precursor_mz,
                    precursor_charge,
                    precursor_intensity,
                    collision_energy,
                    activation_type,
                    isolation_window_target,
                    isolation_window_lower,
                    isolation_window_upper,
                    CAST(NULL AS VARCHAR) AS spectrum_ref,
                    precursor_scan_number,
                    base_peak_mz,
                    base_peak_intensity,
                    tic,
                    lowest_mz,
                    highest_mz,
                    CAST(NULL AS VARCHAR) AS filter_string,
                    ion_injection_time,
                    monoisotopic_mz,
                    scan_window_lower,
                    scan_window_upper,
                    mz_array,
                    intensity_array
                FROM {table_name}
                """
            )
        else:
            selects.append(f"SELECT {', '.join(EXPORT_COLUMNS)} FROM {table_name}")

    if not selects:
        raise ValueError("This mzDuck file does not contain exportable spectra")
    sql = "\nUNION ALL\n".join(selects) + "\nORDER BY source_index NULLS LAST, scan_number"
    cursor = conn.execute(sql)
    columns = [item[0] for item in cursor.description]
    while True:
        row = cursor.fetchone()
        if row is None:
            break
        spectrum = dict(zip(columns, row))
        scan_number = spectrum["scan_number"]
        overrides = text_overrides.get(scan_number, {})
        spectrum["native_id"] = spectrum.get("native_id") or reconstruct_text_field(
            "native_id", spectrum, metadata, override=overrides.get("native_id")
        )
        if spectrum.get("spectrum_ref") is None:
            spectrum["spectrum_ref"] = reconstruct_text_field(
                "spectrum_ref",
                spectrum,
                metadata,
                override=overrides.get("spectrum_ref"),
            )
        if spectrum.get("filter_string") is None:
            spectrum["filter_string"] = reconstruct_text_field(
                "filter_string",
                spectrum,
                metadata,
                override=overrides.get("filter_string"),
            )
        spectrum["extra_params"] = deepcopy(extra_params.get(scan_number, {}))
        promote_structural_scan_fields(spectrum)
        yield spectrum


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


def polarity_param(value):
    if value == "positive":
        return "positive scan"
    if value == "negative":
        return "negative scan"
    return None


def spectrum_params(spectrum):
    params = [{"ms level": int(spectrum.get("ms_level") or 2)}]
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
    params.extend(params_for_scope(spectrum, "spectrum"))
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
    params.extend(params_for_scope(spectrum, "scan"))
    return params


def scan_window_list(spectrum):
    lower = spectrum.get("scan_window_lower")
    upper = spectrum.get("scan_window_upper")
    if lower is None or upper is None:
        return []
    return [(lower, upper)]


def precursor_information(writer, spectrum):
    try:
        from psims.mzml.writer import PrecursorBuilder
    except ImportError:
        return plain_precursor_information(spectrum)

    if int(spectrum.get("ms_level") or 1) < 2:
        return None
    extra = spectrum.get("extra_params") or {}
    has_selected_ion = any(
        spectrum.get(key) is not None
        for key in ("precursor_mz", "precursor_intensity", "precursor_charge")
    )
    has_isolation = any(
        spectrum.get(key) is not None
        for key in (
            "isolation_window_target",
            "isolation_window_lower",
            "isolation_window_upper",
        )
    )
    has_activation = bool(
        spectrum.get("activation_type") or spectrum.get("collision_energy") is not None
    )
    has_extra = any(extra.get(scope) for scope in extra)
    if not any((has_selected_ion, has_isolation, has_activation, has_extra)):
        return None

    builder = PrecursorBuilder(
        writer, spectrum_reference=spectrum.get("spectrum_ref")
    )
    for param in params_for_scope(spectrum, "precursor"):
        builder.add_param(param)
    if has_selected_ion or extra.get("selected_ion"):
        ion = builder.selected_ion(
            selected_ion_mz=spectrum.get("precursor_mz"),
            intensity=spectrum.get("precursor_intensity"),
            charge=spectrum.get("precursor_charge"),
        )
        for param in params_for_scope(spectrum, "selected_ion"):
            ion.add_param(param)
    if has_isolation or extra.get("isolation_window"):
        isolation = builder.isolation_window(
            target=spectrum.get("isolation_window_target"),
            lower=spectrum.get("isolation_window_lower"),
            upper=spectrum.get("isolation_window_upper"),
        )
        for param in params_for_scope(spectrum, "isolation_window"):
            isolation.add_param(param)
    if has_activation or extra.get("activation"):
        activation = builder.activation()
        activation_type = spectrum.get("activation_type")
        if activation_type:
            activation.add_param(
                SHORT_ACTIVATION_TO_TERM.get(activation_type, activation_type)
            )
        if spectrum.get("collision_energy") is not None:
            activation.add_param({"collision energy": spectrum["collision_energy"]})
        for param in params_for_scope(spectrum, "activation"):
            activation.add_param(param)
    return builder


def plain_precursor_information(spectrum):
    if int(spectrum.get("ms_level") or 1) < 2:
        return None
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
    return params or None


def load_text_overrides(conn):
    if not table_exists(conn, "spectrum_text_overrides"):
        return {}
    rows = conn.execute(
        """
        SELECT scan_number, field_name, value
        FROM spectrum_text_overrides
        ORDER BY scan_number, field_name
        """
    ).fetchall()
    result = {}
    for scan_number, field_name, value in rows:
        result.setdefault(int(scan_number), {})[field_name] = value
    return result


def load_extra_params(conn):
    if not table_exists(conn, "spectrum_extra_params"):
        return {}
    rows = conn.execute(
        """
        SELECT
            scan_number,
            scope,
            ordinal,
            accession,
            name,
            value,
            unit_accession,
            unit_name,
            cv_ref
        FROM spectrum_extra_params
        ORDER BY scan_number, scope, ordinal
        """
    ).fetchall()
    result = {}
    for row in rows:
        scan_number = int(row[0])
        entry = {
            "scan_number": scan_number,
            "scope": row[1],
            "ordinal": row[2],
            "accession": row[3],
            "name": row[4],
            "value": row[5],
            "unit_accession": row[6],
            "unit_name": row[7],
            "cv_ref": row[8],
        }
        result.setdefault(scan_number, {}).setdefault(entry["scope"], []).append(entry)
    return result


def params_for_scope(spectrum, scope):
    rows = (spectrum.get("extra_params") or {}).get(scope, [])
    return [param_row_to_psims(row) for row in rows]


def param_row_to_psims(row):
    param = {"name": row["name"]}
    if row.get("accession"):
        param["accession"] = row["accession"]
    if row.get("value") is not None:
        param["value"] = row["value"]
    if row.get("unit_accession"):
        param["unit_accession"] = row["unit_accession"]
    if row.get("unit_name"):
        param["unitName"] = row["unit_name"]
    return param


def v2_ms2_storage(conn):
    if not table_exists(conn, "ms2_spectra"):
        return False
    if schema_version(conn) != "2":
        return False
    return True


def restore_original_header_fragments(path, metadata, *, instrument_configuration_refs=None):
    try:
        from lxml import etree
    except ImportError:
        return

    parser = etree.XMLParser(remove_blank_text=False)
    tree = etree.parse(str(path), parser)
    root = tree.getroot()
    mzml_root = mzml_document_root(root)
    namespace = etree.QName(mzml_root.tag).namespace

    for tag_name, metadata_key in (
        ("fileDescription", "file_description_xml"),
        ("referenceableParamGroupList", "referenceable_param_groups_xml"),
        ("softwareList", "software_list_xml"),
        ("instrumentConfigurationList", "instrument_configuration_xml"),
        ("dataProcessingList", "data_processing_xml"),
    ):
        fragment = metadata.get(metadata_key)
        if fragment:
            replace_root_fragment(mzml_root, tag_name, fragment, namespace)

    run = find_child(mzml_root, "run", namespace)
    if run is not None:
        if metadata.get("run_id"):
            run.set("id", metadata["run_id"])
        if metadata.get("run_start_time"):
            run.set("startTimeStamp", metadata["run_start_time"])
        if metadata.get("instrument_config_ref"):
            run.set(
                "defaultInstrumentConfigurationRef",
                metadata["instrument_config_ref"],
            )
        spectrum_list = find_child(run, "spectrumList", namespace)
        if (
            spectrum_list is not None
            and metadata.get("spectrum_list_default_data_processing_ref")
        ):
            spectrum_list.set(
                "defaultDataProcessingRef",
                metadata["spectrum_list_default_data_processing_ref"],
            )
        if instrument_configuration_refs:
            restore_scan_instrument_configuration_refs(
                spectrum_list,
                namespace,
                instrument_configuration_refs,
            )

    tree.write(str(path), encoding="UTF-8", xml_declaration=True, pretty_print=True)


def replace_root_fragment(root, tag_name, fragment, namespace):
    new_element = parse_fragment(fragment, namespace)
    current = find_child(root, tag_name, namespace)
    if current is not None:
        root.replace(current, new_element)
        return
    run = find_child(root, "run", namespace)
    if run is not None:
        root.insert(root.index(run), new_element)
        return
    root.append(new_element)


def parse_fragment(fragment, namespace):
    if namespace:
        wrapped = f'<wrapper xmlns="{namespace}">{fragment}</wrapper>'
    else:
        wrapped = f"<wrapper>{fragment}</wrapper>"
    from lxml import etree

    wrapper = etree.fromstring(wrapped.encode("utf-8"))
    return wrapper[0]


def find_child(parent, tag_name, namespace):
    if namespace:
        return parent.find(f"{{{namespace}}}{tag_name}")
    return parent.find(tag_name)


def restore_scan_instrument_configuration_refs(
    spectrum_list,
    namespace,
    instrument_configuration_refs,
):
    if spectrum_list is None:
        return
    spectrum_tag = namespaced_tag("spectrum", namespace)
    scan_tag = namespaced_tag("scan", namespace)
    for spectrum in spectrum_list.iterfind(spectrum_tag):
        native_id = spectrum.get("id")
        if not native_id:
            continue
        instrument_config_ref = instrument_configuration_refs.get(native_id)
        if not instrument_config_ref:
            continue
        scan = spectrum.find(f".//{scan_tag}")
        if scan is not None:
            scan.set("instrumentConfigurationRef", instrument_config_ref)


def mzml_document_root(root):
    if root.tag.rsplit("}", 1)[-1] == "mzML":
        return root
    namespace = getattr(root, "nsmap", {}).get(None)
    child = find_child(root, "mzML", namespace)
    return child if child is not None else root


def namespaced_tag(tag_name, namespace):
    if namespace:
        return f"{{{namespace}}}{tag_name}"
    return tag_name
