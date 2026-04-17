"""DuckDB schema contract for mzDuck."""

from __future__ import annotations

import json
import re

SCHEMA_VERSION = "1"

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

MGF_DDL_TEMPLATE = """
CREATE TABLE mgf (
    scan_number          INTEGER NOT NULL,
    title                VARCHAR NOT NULL,
    rt                   FLOAT,
    precursor_mz         DOUBLE,
    precursor_intensity  FLOAT,
    precursor_charge     TINYINT,
    mz_array             {mz_array_type}[] NOT NULL,
    intensity_array      {intensity_array_type}[] NOT NULL
)
"""

MS1_COLUMNS = [
    "scan_number",
    "source_index",
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

MS2_COLUMNS = [
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

MS2_DDL = """
CREATE TABLE ms2_spectra (
    scan_number                INTEGER NOT NULL,
    source_index               INTEGER NOT NULL,
    native_id                  VARCHAR,
    ms_level                   UTINYINT NOT NULL,
    rt                         FLOAT NOT NULL,
    collision_energy           FLOAT,
    activation_type            VARCHAR,
    isolation_window_target    DOUBLE,
    isolation_window_lower     FLOAT,
    isolation_window_upper     FLOAT,
    spectrum_ref               VARCHAR,
    base_peak_mz               FLOAT,
    base_peak_intensity        FLOAT,
    tic                        FLOAT,
    lowest_mz                  FLOAT,
    highest_mz                 FLOAT,
    filter_string              VARCHAR,
    ion_injection_time         FLOAT,
    monoisotopic_mz            DOUBLE,
    scan_window_lower          FLOAT,
    scan_window_upper          FLOAT
)
"""

MSN_COLUMNS = [
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

MSN_DDL_TEMPLATE = """
CREATE TABLE {table_name} (
    scan_number                INTEGER NOT NULL,
    source_index               INTEGER NOT NULL,
    native_id                  VARCHAR,
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
    spectrum_ref               VARCHAR,
    base_peak_mz               FLOAT,
    base_peak_intensity        FLOAT,
    tic                        FLOAT,
    lowest_mz                  FLOAT,
    highest_mz                 FLOAT,
    filter_string              VARCHAR,
    ion_injection_time         FLOAT,
    monoisotopic_mz            DOUBLE,
    scan_window_lower          FLOAT,
    scan_window_upper          FLOAT,
    mz_array                   {mz_array_type}[] NOT NULL,
    intensity_array            {intensity_array_type}[] NOT NULL
)
"""

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

SPECTRUM_SUMMARY_DDL = """
CREATE TABLE spectrum_summary (
    scan_number      INTEGER NOT NULL,
    source_index     INTEGER NOT NULL,
    ms_level         UTINYINT NOT NULL,
    table_name       VARCHAR NOT NULL,
    rt               FLOAT NOT NULL,
    peak_count       INTEGER NOT NULL,
    native_id        VARCHAR,
    included_in_mgf  BOOLEAN NOT NULL
)
"""

INDEX_MGF_SCAN_SQL = "CREATE INDEX idx_mgf_scan_number ON mgf(scan_number)"

MSN_TABLE_RE = re.compile(r"^ms([3-9]\d*)_spectra$")


def create_schema(
    conn,
    *,
    include_mgf: bool,
    include_ms1: bool,
    include_ms2: bool,
    msn_levels=(),
    include_summary: bool = True,
    mz_array_type="FLOAT",
    intensity_array_type="FLOAT",
):
    """Create the selected mzDuck tables."""
    conn.execute(RUN_METADATA_DDL)
    if include_mgf:
        create_mgf_table(
            conn,
            mz_array_type=mz_array_type,
            intensity_array_type=intensity_array_type,
        )
    if include_ms1:
        create_ms1_table(
            conn,
            mz_array_type=mz_array_type,
            intensity_array_type=intensity_array_type,
        )
    if include_ms2:
        conn.execute(MS2_DDL)
    for level in sorted({int(level) for level in msn_levels if int(level) >= 3}):
        create_msn_table(
            conn,
            level,
            mz_array_type=mz_array_type,
            intensity_array_type=intensity_array_type,
        )
    if include_summary:
        conn.execute(SPECTRUM_SUMMARY_DDL)


def create_mgf_table(conn, *, mz_array_type="FLOAT", intensity_array_type="FLOAT"):
    conn.execute(
        MGF_DDL_TEMPLATE.format(
            mz_array_type=normalize_storage_type(mz_array_type),
            intensity_array_type=normalize_storage_type(intensity_array_type),
        )
    )


def create_ms1_table(conn, *, mz_array_type="FLOAT", intensity_array_type="FLOAT"):
    conn.execute(
        MS1_DDL_TEMPLATE.format(
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


def create_scan_index(conn):
    """Create the optional scan-number index on the MGF contract table only."""
    if table_exists(conn, "mgf"):
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


def table_exists(conn, table_name):
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = 'main' AND table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(row and row[0])


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
        or name == "spectrum_summary"
    ]


def msn_levels_present(conn):
    levels = []
    for table_name in data_table_names(conn):
        match = MSN_TABLE_RE.match(table_name)
        if match:
            levels.append(int(match.group(1)))
    return sorted(levels)


def get_table_columns(conn, table_name):
    rows = conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    return [row[1] for row in rows]


def validate_required_schema(conn):
    """Validate that a connection exposes the mzDuck table contract."""
    if not table_exists(conn, "run_metadata"):
        raise ValueError("Missing required mzDuck table: run_metadata")

    metadata_missing = {"key", "value"} - set(get_table_columns(conn, "run_metadata"))
    if metadata_missing:
        raise ValueError(
            "Missing required run_metadata column(s): "
            + ", ".join(sorted(metadata_missing))
        )

    version_row = conn.execute(
        "SELECT value FROM run_metadata WHERE key = 'schema_version'"
    ).fetchone()
    if version_row is None:
        raise ValueError("Missing run_metadata schema_version")
    version = str(version_row[0])
    if version.split(".", 1)[0] != SCHEMA_VERSION:
        raise ValueError(f"Unsupported mzDuck schema_version: {version}")

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
        missing = set(MS2_COLUMNS) - set(get_table_columns(conn, "ms2_spectra"))
        if missing:
            raise ValueError(
                "Missing ms2_spectra column(s): " + ", ".join(sorted(missing))
            )
    for table_name in data_tables:
        if MSN_TABLE_RE.match(table_name):
            missing = set(MSN_COLUMNS) - set(get_table_columns(conn, table_name))
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
