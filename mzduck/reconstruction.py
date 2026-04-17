"""Helpers for reconstructable mzML text fields in mzDuck v2."""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP


DEFAULT_MGF_TITLE_TEMPLATE = (
    "{mgf_title_source}.{scan_number}.{scan_number}.{precursor_charge}"
)
FILTER_STRING_ENCODING_RAW = "raw"
FILTER_STRING_ENCODING_THERMO_MS2_V1 = "thermo_ms2_v1"
TEXT_OVERRIDE_FIELDS = {"native_id", "spectrum_ref", "filter_string"}
SCAN_STRUCTURAL_FIELD_NAMES = {
    "instrumentConfigurationRef": "instrument_configuration_ref",
}

THERMO_FILTER_PREFIXES = {
    "HCD": "FTMS + c NSI d Full ms2 ",
    "CID": "ITMS + c NSI r d Full ms2 ",
}


def infer_scan_template(pairs, *, placeholder_name):
    """Infer a scan-number template like ``scan={scan_number}``."""
    if not pairs or any(not text for text, _ in pairs):
        return None
    first_text, first_scan_number = pairs[0]
    token = f"scan={first_scan_number}"
    if token not in first_text:
        return None
    candidate = first_text.replace(token, f"scan={{{placeholder_name}}}", 1)
    for text, scan_number in pairs:
        if candidate.format(**{placeholder_name: scan_number}) != text:
            return None
    return candidate


def format_half_up(value, places):
    if value is None:
        return None
    quantum = "0." + ("0" * (places - 1)) + "1" if places else "1"
    return str(
        Decimal(str(value)).quantize(Decimal(quantum), rounding=ROUND_HALF_UP)
    )


def mgf_title_for_scan(metadata, scan_number, precursor_charge):
    source = metadata.get("mgf_title_source") or "mzduck"
    charge = int(precursor_charge) if precursor_charge is not None else 0
    return f"{source}.{int(scan_number)}.{int(scan_number)}.{charge}"


def render_template(template, **values):
    if not template:
        return None
    return template.format(**values)


def thermo_ms2_v1_filter_string(record):
    activation_type = record.get("activation_type")
    prefix = THERMO_FILTER_PREFIXES.get(activation_type)
    if prefix is None:
        return None
    collision_energy = record.get("collision_energy")
    target = record.get("isolation_window_target")
    lower = record.get("scan_window_lower")
    upper = record.get("scan_window_upper")
    if None in (collision_energy, target, lower, upper):
        return None
    target_text = format_half_up(target, 4)
    energy_text = format_half_up(collision_energy, 2)
    lower_text = format_half_up(lower, 4)
    upper_text = format_half_up(upper, 4)
    return (
        f"{prefix}{target_text}@{activation_type.lower()}{energy_text} "
        f"[{lower_text}-{upper_text}]"
    )


FILTER_STRING_ENCODERS = {
    FILTER_STRING_ENCODING_THERMO_MS2_V1: thermo_ms2_v1_filter_string,
}


class FilterStringDetector:
    """Detect a run-level filter-string encoder by exact full-run matching."""

    def __init__(self, encoder_names=None):
        if encoder_names is None:
            encoder_names = list(FILTER_STRING_ENCODERS)
        self.encoder_names = list(encoder_names)
        self._matches = {name: True for name in self.encoder_names}
        self._saw_non_null = False

    def observe(self, record):
        value = record.get("filter_string")
        if value is None:
            return
        self._saw_non_null = True
        for name in self.encoder_names:
            if not self._matches[name]:
                continue
            candidate = FILTER_STRING_ENCODERS[name](record)
            if candidate != value:
                self._matches[name] = False

    def encoding(self):
        if not self._saw_non_null:
            return FILTER_STRING_ENCODING_RAW
        for name in self.encoder_names:
            if self._matches[name]:
                return name
        return FILTER_STRING_ENCODING_RAW


def reconstruct_filter_string(record, metadata, override=None):
    if override:
        return override
    encoding = metadata.get("filter_string_encoding") or FILTER_STRING_ENCODING_RAW
    encoder = FILTER_STRING_ENCODERS.get(encoding)
    if encoder is None:
        return None
    return encoder(record)


def reconstruct_text_field(field_name, record, metadata, override=None):
    if override:
        return override
    if field_name == "native_id":
        template = metadata.get("native_id_template")
        if template:
            return render_template(template, scan_number=record["scan_number"])
        return f"scan={record['scan_number']}"
    if field_name == "spectrum_ref":
        template = metadata.get("spectrum_ref_template")
        precursor_scan_number = record.get("precursor_scan_number")
        if template and precursor_scan_number is not None:
            return render_template(
                template, precursor_scan_number=int(precursor_scan_number)
            )
        return None
    if field_name == "filter_string":
        return reconstruct_filter_string(record, metadata, override=override)
    raise KeyError(f"Unsupported text field override: {field_name}")


def promote_structural_scan_fields(spectrum):
    """Promote scan-level XML attributes out of generic extra-param rows."""
    extra_params = dict(spectrum.get("extra_params") or {})
    scan_rows = list(extra_params.get("scan") or [])
    if not scan_rows:
        return spectrum

    kept_rows = []
    for row in scan_rows:
        field_name = SCAN_STRUCTURAL_FIELD_NAMES.get(row.get("name"))
        if field_name and spectrum.get(field_name) is None:
            spectrum[field_name] = row.get("value")
            continue
        kept_rows.append(row)

    if kept_rows:
        extra_params["scan"] = kept_rows
    else:
        extra_params.pop("scan", None)
    spectrum["extra_params"] = extra_params
    return spectrum
