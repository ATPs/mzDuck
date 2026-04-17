"""DuckDB schema contract for mzDuck v1."""

from __future__ import annotations

SCHEMA_VERSION = "1"

SPECTRA_COLUMNS = [
    "scan_number",
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

PEAK_COLUMNS = ["scan_number", "peak_index", "mz", "intensity"]

SPECTRA_DDL_TEMPLATE = """
CREATE TABLE spectra (
    scan_number                INTEGER   NOT NULL,
    native_id                  VARCHAR,

    ms_level                   UTINYINT  NOT NULL,
    rt                         FLOAT     NOT NULL,

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

RUN_METADATA_DDL = """
CREATE TABLE run_metadata (
    key      VARCHAR PRIMARY KEY,
    value    VARCHAR
)
"""

PEAKS_VIEW_SQL = """
CREATE VIEW peaks AS
SELECT
    scan_number,
    generate_subscripts(mz_array, 1) - 1 AS peak_index,
    UNNEST(mz_array)        AS mz,
    UNNEST(intensity_array) AS intensity
FROM spectra
"""

SPECTRUM_PEAKS_VIEW_SQL = """
CREATE VIEW spectrum_peaks AS
SELECT
    scan_number,
    rt,
    precursor_mz,
    precursor_charge,
    generate_subscripts(mz_array, 1) - 1 AS peak_index,
    UNNEST(mz_array)        AS mz,
    UNNEST(intensity_array) AS intensity
FROM spectra
"""

INDEX_SCAN_NUMBER_SQL = "CREATE INDEX idx_spectra_scan_number ON spectra(scan_number)"


def create_schema(conn, *, mz_array_type="FLOAT", intensity_array_type="FLOAT"):
    """Create the required v1 tables and views."""
    conn.execute(
        SPECTRA_DDL_TEMPLATE.format(
            mz_array_type=normalize_storage_type(mz_array_type),
            intensity_array_type=normalize_storage_type(intensity_array_type),
        )
    )
    conn.execute(RUN_METADATA_DDL)
    conn.execute(PEAKS_VIEW_SQL)
    conn.execute(SPECTRUM_PEAKS_VIEW_SQL)


def create_scan_number_index(conn):
    """Create the optional v1 scan-number index."""
    conn.execute(INDEX_SCAN_NUMBER_SQL)


def normalize_storage_type(value):
    text = str(value).upper()
    if text not in {"FLOAT", "DOUBLE"}:
        raise ValueError(f"Unsupported mzDuck array storage type: {value!r}")
    return text


def get_table_columns(conn, table_name):
    rows = conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    return [row[1] for row in rows]


def validate_required_schema(conn):
    """Validate that a connection exposes the mzDuck v1 contract."""
    tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
    missing_tables = {"spectra", "peaks", "run_metadata"} - tables
    if missing_tables:
        missing = ", ".join(sorted(missing_tables))
        raise ValueError(f"Missing required mzDuck table/view(s): {missing}")

    spectra_missing = set(SPECTRA_COLUMNS) - set(get_table_columns(conn, "spectra"))
    peaks_missing = set(PEAK_COLUMNS) - set(get_table_columns(conn, "peaks"))
    metadata_missing = {"key", "value"} - set(get_table_columns(conn, "run_metadata"))
    errors = []
    if spectra_missing:
        errors.append("spectra: " + ", ".join(sorted(spectra_missing)))
    if peaks_missing:
        errors.append("peaks: " + ", ".join(sorted(peaks_missing)))
    if metadata_missing:
        errors.append("run_metadata: " + ", ".join(sorted(metadata_missing)))
    if errors:
        raise ValueError("Missing required mzDuck column(s): " + "; ".join(errors))

    version_row = conn.execute(
        "SELECT value FROM run_metadata WHERE key = 'schema_version'"
    ).fetchone()
    if version_row is None:
        raise ValueError("Missing run_metadata schema_version")
    version = str(version_row[0])
    if version.split(".", 1)[0] != SCHEMA_VERSION:
        raise ValueError(f"Unsupported mzDuck schema_version: {version}")


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
