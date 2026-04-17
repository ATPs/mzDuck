"""DuckDB schema contract for mzDuck."""

from __future__ import annotations

import json
import re

from .reconstruction import DEFAULT_MGF_TITLE_TEMPLATE

SCHEMA_VERSION = "2"
SUPPORTED_SCHEMA_VERSIONS = {"1", "2"}

RUN_METADATA_DDL = """
CREATE TABLE run_metadata (
    key      VARCHAR PRIMARY KEY,
    value    VARCHAR
)
"""

MGF_COLUMNS = [
    "scan_number",
    "title",
    "rt",
    "precursor_mz",
    "precursor_intensity",
    "precursor_charge",
    "mz_array",
    "intensity_array",
]

MS1_COLUMNS = [
    "scan_number",
    "source_index",
    "instrument_configuration_ref",
    "native_id",
    "ms_level",
    "rt",
    "base_peak_mz",
    "base_peak_intensity",
    "tic",
    "lowest_mz",
    "highest_mz",
    "filter_string",
    "ion_injection_time",
    "scan_window_lower",
    "scan_window_upper",
    "mz_array",
    "intensity_array",
]

MS1_DDL_TEMPLATE = """
CREATE TABLE ms1_spectra (
    scan_number          INTEGER NOT NULL,
    source_index         INTEGER NOT NULL,
    instrument_configuration_ref VARCHAR,
    native_id            VARCHAR,
    ms_level             UTINYINT NOT NULL,
    rt                   FLOAT NOT NULL,
    base_peak_mz         FLOAT,
    base_peak_intensity  FLOAT,
    tic                  FLOAT,
    lowest_mz            FLOAT,
    highest_mz           FLOAT,
    filter_string        VARCHAR,
    ion_injection_time   FLOAT,
    scan_window_lower    FLOAT,
    scan_window_upper    FLOAT,
    mz_array             {mz_array_type}[] NOT NULL,
    intensity_array      {intensity_array_type}[] NOT NULL
)
"""

V1_MS2_COLUMNS = [
    "scan_number",
    "source_index",
    "native_id",
    "ms_level",
    "rt",
    "collision_energy",
    "activation_type",
    "isolation_window_target",
    "isolation_window_lower",
    "isolation_window_upper",
    "spectrum_ref",
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
]

MS2_COLUMNS = [
    "scan_number",
    "source_index",
    "instrument_configuration_ref",
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
    "precursor_scan_number",
    "base_peak_mz",
    "base_peak_intensity",
    "tic",
    "lowest_mz",
    "highest_mz",
    "ion_injection_time",
    "monoisotopic_mz",
    "scan_window_lower",
    "scan_window_upper",
    "mz_array",
    "intensity_array",
]

MS2_DDL_TEMPLATE = """
CREATE TABLE ms2_spectra (
    scan_number                INTEGER NOT NULL,
    source_index               INTEGER NOT NULL,
    instrument_configuration_ref VARCHAR,
    ms_level                   UTINYINT NOT NULL,
    rt                         FLOAT NOT NULL,
    precursor_mz               DOUBLE,
    precursor_charge           TINYINT,
    precursor_intensity        FLOAT,
    collision_energy           FLOAT,
    activation_type            VARCHAR,
    isolation_window_target    DOUBLE,
    isolation_window_lower     FLOAT,
    isolation_window_upper     FLOAT,
    precursor_scan_number      INTEGER,
    base_peak_mz               FLOAT,
    base_peak_intensity        FLOAT,
    tic                        FLOAT,
    lowest_mz                  FLOAT,
    highest_mz                 FLOAT,
    ion_injection_time         FLOAT,
    monoisotopic_mz            DOUBLE,
    scan_window_lower          FLOAT,
    scan_window_upper          FLOAT,
    mz_array                   {mz_array_type}[] NOT NULL,
    intensity_array            {intensity_array_type}[] NOT NULL
)
"""

V1_MSN_COLUMNS = [
    "scan_number",
    "source_index",
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

MSN_COLUMNS = [
    "scan_number",
    "source_index",
    "instrument_configuration_ref",
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
    "precursor_scan_number",
    "base_peak_mz",
    "base_peak_intensity",
    "tic",
    "lowest_mz",
    "highest_mz",
    "ion_injection_time",
    "monoisotopic_mz",
    "scan_window_lower",
    "scan_window_upper",
    "mz_array",
    "intensity_array",
]

MSN_DDL_TEMPLATE = """
CREATE TABLE {table_name} (
    scan_number                INTEGER NOT NULL,
    source_index               INTEGER NOT NULL,
    instrument_configuration_ref VARCHAR,
    ms_level                   UTINYINT NOT NULL,
    rt                         FLOAT NOT NULL,
    precursor_mz               DOUBLE,
    precursor_charge           TINYINT,
    precursor_intensity        FLOAT,
    collision_energy           FLOAT,
    activation_type            VARCHAR,
    isolation_window_target    DOUBLE,
    isolation_window_lower     FLOAT,
    isolation_window_upper     FLOAT,
    precursor_scan_number      INTEGER,
    base_peak_mz               FLOAT,
    base_peak_intensity        FLOAT,
    tic                        FLOAT,
    lowest_mz                  FLOAT,
    highest_mz                 FLOAT,
    ion_injection_time         FLOAT,
    monoisotopic_mz            DOUBLE,
    scan_window_lower          FLOAT,
    scan_window_upper          FLOAT,
    mz_array                   {mz_array_type}[] NOT NULL,
    intensity_array            {intensity_array_type}[] NOT NULL
)
"""

TEXT_OVERRIDE_COLUMNS = ["scan_number", "field_name", "value"]
TEXT_OVERRIDE_DDL = """
CREATE TABLE spectrum_text_overrides (
    scan_number      INTEGER NOT NULL,
    field_name       VARCHAR NOT NULL,
    value            VARCHAR NOT NULL,
    PRIMARY KEY(scan_number, field_name)
)
"""

EXTRA_PARAM_COLUMNS = [
    "scan_number",
    "scope",
    "ordinal",
    "accession",
    "name",
    "value",
    "unit_accession",
    "unit_name",
    "cv_ref",
]
EXTRA_PARAM_DDL = """
CREATE TABLE spectrum_extra_params (
    scan_number      INTEGER NOT NULL,
    scope            VARCHAR NOT NULL,
    ordinal          INTEGER NOT NULL,
    accession        VARCHAR,
    name             VARCHAR NOT NULL,
    value            VARCHAR,
    unit_accession   VARCHAR,
    unit_name        VARCHAR,
    cv_ref           VARCHAR,
    PRIMARY KEY(scan_number, scope, ordinal)
)
"""

V2_REQUIRED_MS1_COLUMNS = [
    column for column in MS1_COLUMNS if column != "instrument_configuration_ref"
]
V2_REQUIRED_MS2_COLUMNS = [
    column for column in MS2_COLUMNS if column != "instrument_configuration_ref"
]
V2_REQUIRED_MSN_COLUMNS = [
    column for column in MSN_COLUMNS if column != "instrument_configuration_ref"
]

SPECTRUM_SUMMARY_COLUMNS = [
    "scan_number",
    "source_index",
    "ms_level",
    "table_name",
    "rt",
    "peak_count",
    "native_id",
    "included_in_mgf",
]

INDEX_MGF_SCAN_SQL = "CREATE INDEX idx_mgf_scan_number ON ms2_spectra(scan_number)"
MSN_TABLE_RE = re.compile(r"^ms([3-9]\d*)_spectra$")


def create_schema(
    conn,
    *,
    include_ms1: bool,
    include_ms2: bool,
    msn_levels=(),
    mz_array_type="FLOAT",
    intensity_array_type="FLOAT",
):
    """Create the selected mzDuck v2 tables and compatibility views."""
    include_text_support = include_ms2 or bool(tuple(msn_levels))
    conn.execute(RUN_METADATA_DDL)
    if include_ms1:
        create_ms1_table(
            conn,
            mz_array_type=mz_array_type,
            intensity_array_type=intensity_array_type,
        )
    if include_ms2:
        create_ms2_table(
            conn,
            mz_array_type=mz_array_type,
            intensity_array_type=intensity_array_type,
        )
    for level in sorted({int(level) for level in msn_levels if int(level) >= 3}):
        create_msn_table(
            conn,
            level,
            mz_array_type=mz_array_type,
            intensity_array_type=intensity_array_type,
        )
    if include_text_support:
        create_text_override_table(conn)
        create_extra_param_table(conn)
    if include_ms2:
        create_mgf_view(conn)


def create_ms1_table(conn, *, mz_array_type="FLOAT", intensity_array_type="FLOAT"):
    conn.execute(
        MS1_DDL_TEMPLATE.format(
            mz_array_type=normalize_storage_type(mz_array_type),
            intensity_array_type=normalize_storage_type(intensity_array_type),
        )
    )


def create_ms2_table(conn, *, mz_array_type="FLOAT", intensity_array_type="FLOAT"):
    conn.execute(
        MS2_DDL_TEMPLATE.format(
            mz_array_type=normalize_storage_type(mz_array_type),
            intensity_array_type=normalize_storage_type(intensity_array_type),
        )
    )


def create_msn_table(conn, level, *, mz_array_type="FLOAT", intensity_array_type="FLOAT"):
    level = int(level)
    if level < 3:
        raise ValueError(f"MSn detail tables require ms_level >= 3, got {level}")
    conn.execute(
        MSN_DDL_TEMPLATE.format(
            table_name=msn_table_name(level),
            mz_array_type=normalize_storage_type(mz_array_type),
            intensity_array_type=normalize_storage_type(intensity_array_type),
        )
    )


def create_text_override_table(conn):
    conn.execute(TEXT_OVERRIDE_DDL)


def create_extra_param_table(conn):
    conn.execute(EXTRA_PARAM_DDL)


def create_mgf_view(conn):
    conn.execute(
        """
        CREATE VIEW mgf AS
        WITH meta AS (
            SELECT
                COALESCE(
                    MAX(CASE WHEN key = 'mgf_title_source' THEN value END),
                    'mzduck'
                ) AS mgf_title_source
            FROM run_metadata
        )
        SELECT
            s.scan_number,
            meta.mgf_title_source || '.'
                || CAST(s.scan_number AS VARCHAR) || '.'
                || CAST(s.scan_number AS VARCHAR) || '.'
                || CAST(COALESCE(s.precursor_charge, 0) AS VARCHAR) AS title,
            s.rt,
            s.precursor_mz,
            s.precursor_intensity,
            s.precursor_charge,
            s.mz_array,
            s.intensity_array
        FROM ms2_spectra s
        CROSS JOIN meta
        WHERE s.ms_level = 2
        """
    )


def create_scan_index(conn):
    """Create the optional scan-number index for the MGF compatibility view."""
    if base_table_exists(conn, "ms2_spectra"):
        conn.execute(INDEX_MGF_SCAN_SQL)


def normalize_storage_type(value):
    text = str(value).upper()
    if text not in {"FLOAT", "DOUBLE"}:
        raise ValueError(f"Unsupported mzDuck array storage type: {value!r}")
    return text


def msn_table_name(level):
    level = int(level)
    if level < 3:
        raise ValueError(f"MSn detail tables require ms_level >= 3, got {level}")
    return f"ms{level}_spectra"


def relation_type(conn, table_name):
    row = conn.execute(
        """
        SELECT table_type
        FROM information_schema.tables
        WHERE table_schema = 'main' AND table_name = ?
        """,
        [table_name],
    ).fetchone()
    if row is None:
        return None
    return str(row[0]).upper()


def table_exists(conn, table_name):
    return relation_type(conn, table_name) is not None


def base_table_exists(conn, table_name):
    return relation_type(conn, table_name) == "BASE TABLE"


def data_table_names(conn):
    names = [
        row[0]
        for row in conn.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
            ORDER BY table_name
            """
        ).fetchall()
    ]
    return [
        name
        for name in names
        if name == "mgf"
        or name == "ms1_spectra"
        or name == "ms2_spectra"
        or MSN_TABLE_RE.match(name)
    ]


def msn_levels_present(conn):
    levels = []
    for table_name in data_table_names(conn):
        match = MSN_TABLE_RE.match(table_name)
        if match and base_table_exists(conn, table_name):
            levels.append(int(match.group(1)))
    return sorted(levels)


def get_table_columns(conn, table_name):
    rows = conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    return [row[1] for row in rows]


def schema_version(conn):
    row = conn.execute(
        "SELECT value FROM run_metadata WHERE key = 'schema_version'"
    ).fetchone()
    if row is None:
        raise ValueError("Missing run_metadata schema_version")
    return str(row[0]).split(".", 1)[0]


def validate_required_schema(conn):
    """Validate that a connection exposes a supported mzDuck schema."""
    if not table_exists(conn, "run_metadata"):
        raise ValueError("Missing required mzDuck table: run_metadata")

    metadata_missing = {"key", "value"} - set(get_table_columns(conn, "run_metadata"))
    if metadata_missing:
        raise ValueError(
            "Missing required run_metadata column(s): "
            + ", ".join(sorted(metadata_missing))
        )

    version = schema_version(conn)
    if version not in SUPPORTED_SCHEMA_VERSIONS:
        raise ValueError(f"Unsupported mzDuck schema_version: {version}")
    if version == "1":
        validate_v1_schema(conn)
        return
    validate_v2_schema(conn)


def validate_v1_schema(conn):
    data_tables = set(data_table_names(conn)) - {"spectrum_summary"}
    if not data_tables:
        raise ValueError("mzDuck file does not contain a data table")
    if "mgf" in data_tables:
        missing = set(MGF_COLUMNS) - set(get_table_columns(conn, "mgf"))
        if missing:
            raise ValueError("Missing mgf column(s): " + ", ".join(sorted(missing)))
    if "ms1_spectra" in data_tables:
        missing = set(MS1_COLUMNS) - set(get_table_columns(conn, "ms1_spectra"))
        if missing:
            raise ValueError(
                "Missing ms1_spectra column(s): " + ", ".join(sorted(missing))
            )
    if "ms2_spectra" in data_tables:
        missing = set(V1_MS2_COLUMNS) - set(get_table_columns(conn, "ms2_spectra"))
        if missing:
            raise ValueError(
                "Missing ms2_spectra column(s): " + ", ".join(sorted(missing))
            )
    for table_name in data_tables:
        if MSN_TABLE_RE.match(table_name):
            missing = set(V1_MSN_COLUMNS) - set(get_table_columns(conn, table_name))
            if missing:
                raise ValueError(
                    f"Missing {table_name} column(s): "
                    + ", ".join(sorted(missing))
                )


def validate_v2_schema(conn):
    base_tables = {name for name in data_table_names(conn) if base_table_exists(conn, name)}
    if not base_tables:
        raise ValueError("mzDuck file does not contain a base data table")
    if "ms1_spectra" in base_tables:
        missing = set(V2_REQUIRED_MS1_COLUMNS) - set(
            get_table_columns(conn, "ms1_spectra")
        )
        if missing:
            raise ValueError(
                "Missing ms1_spectra column(s): " + ", ".join(sorted(missing))
            )
    if "ms2_spectra" in base_tables:
        missing = set(V2_REQUIRED_MS2_COLUMNS) - set(
            get_table_columns(conn, "ms2_spectra")
        )
        if missing:
            raise ValueError(
                "Missing ms2_spectra column(s): " + ", ".join(sorted(missing))
            )
        if not table_exists(conn, "mgf"):
            raise ValueError("Missing required compatibility view: mgf")
        missing = set(MGF_COLUMNS) - set(get_table_columns(conn, "mgf"))
        if missing:
            raise ValueError(
                "Missing mgf compatibility column(s): " + ", ".join(sorted(missing))
            )
        if not base_table_exists(conn, "spectrum_text_overrides"):
            raise ValueError("Missing required v2 table: spectrum_text_overrides")
        if not base_table_exists(conn, "spectrum_extra_params"):
            raise ValueError("Missing required v2 table: spectrum_extra_params")
    for table_name in base_tables:
        if MSN_TABLE_RE.match(table_name):
            missing = set(V2_REQUIRED_MSN_COLUMNS) - set(
                get_table_columns(conn, table_name)
            )
            if missing:
                raise ValueError(
                    f"Missing {table_name} column(s): "
                    + ", ".join(sorted(missing))
                )


def upsert_metadata(conn, metadata):
    """Insert or replace metadata key/value pairs."""
    rows = [
        (str(key), None if value is None else str(value))
        for key, value in metadata.items()
    ]
    if not rows:
        return
    conn.executemany(
        """
        INSERT INTO run_metadata(key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        rows,
    )


def metadata_json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def table_count(conn, table_name):
    if not table_exists(conn, table_name):
        return 0
    return int(conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])


def table_peak_count(conn, table_name):
    if not table_exists(conn, table_name):
        return 0
    columns = set(get_table_columns(conn, table_name))
    if "mz_array" not in columns:
        return 0
    return int(
        conn.execute(
            f"SELECT COALESCE(SUM(len(mz_array)), 0) FROM {table_name}"
        ).fetchone()[0]
    )


def table_registry(conn):
    index_rows = conn.execute(
        "SELECT table_name, index_name FROM duckdb_indexes()"
    ).fetchall()
    indexed_by_table = {}
    for table_name, index_name in index_rows:
        indexed_by_table.setdefault(table_name, []).append(index_name)

    registry = []
    for table_name in [
        "mgf",
        "ms1_spectra",
        "ms2_spectra",
        "spectrum_text_overrides",
        "spectrum_extra_params",
    ]:
        if table_exists(conn, table_name):
            registry.append(registry_entry(conn, table_name, indexed_by_table))

    msn_tables = [
        row[0]
        for row in conn.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
              AND regexp_matches(table_name, '^ms[3-9][0-9]*_spectra$')
            ORDER BY table_name
            """
        ).fetchall()
    ]
    for table_name in msn_tables:
        registry.append(registry_entry(conn, table_name, indexed_by_table))
    return registry


def registry_entry(conn, table_name, indexed_by_table):
    columns = set(get_table_columns(conn, table_name))
    row_count = table_count(conn, table_name)
    peak_count = table_peak_count(conn, table_name)
    rel_type = relation_type(conn, table_name)
    ms_level = None
    if table_name == "mgf":
        ms_level = 2
        role = "derived MGF compatibility view"
    elif table_name == "ms1_spectra":
        ms_level = 1
        role = "MS1 spectra and peaks"
    elif table_name == "ms2_spectra":
        ms_level = 2
        role = "canonical MS2 spectra and peaks"
    elif table_name == "spectrum_text_overrides":
        role = "sparse exact text fallbacks"
    elif table_name == "spectrum_extra_params":
        role = "extra mzML parameters outside typed columns"
    else:
        ms_level = int(table_name[2:].split("_", 1)[0])
        role = f"canonical MS{ms_level} spectra and peaks"
    indexed_columns = []
    if table_name in indexed_by_table and "scan_number" in columns:
        indexed_columns.append("scan_number")
    return {
        "table": table_name,
        "relation_type": rel_type,
        "role": role,
        "ms_level": ms_level,
        "row_count": row_count,
        "peak_count": peak_count,
        "indexed_columns": indexed_columns,
        "derived": rel_type == "VIEW",
        "included": True,
    }


def default_metadata_values():
    return {
        "schema_version": SCHEMA_VERSION,
        "mgf_title_template": DEFAULT_MGF_TITLE_TEMPLATE,
    }
