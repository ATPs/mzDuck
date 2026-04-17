"""DuckDB schema contract for mzDuck v1."""

SCHEMA_VERSION = "1"

SPECTRA_COLUMNS = [
    "scan_id",
    "source_index",
    "native_id",
    "scan_number",
    "ms_level",
    "rt",
    "rt_unit",
    "precursor_mz",
    "precursor_charge",
    "precursor_intensity",
    "selected_ion_mz",
    "selected_ion_intensity",
    "selected_ion_charge",
    "collision_energy",
    "activation_type",
    "activation_cv",
    "isolation_window_target",
    "isolation_window_lower",
    "isolation_window_upper",
    "spectrum_ref",
    "base_peak_mz",
    "base_peak_intensity",
    "tic",
    "lowest_mz",
    "highest_mz",
    "num_peaks",
    "polarity",
    "centroided",
    "filter_string",
    "ion_injection_time",
    "ion_injection_time_unit",
    "monoisotopic_mz",
    "scan_window_lower",
    "scan_window_upper",
]

PEAK_COLUMNS = ["scan_id", "peak_index", "mz", "intensity"]

CREATE_TABLES_SQL = [
    """
    CREATE TABLE spectra (
        scan_id                    BIGINT PRIMARY KEY,
        source_index               BIGINT NOT NULL,
        native_id                  VARCHAR NOT NULL,
        scan_number                BIGINT,
        ms_level                   INTEGER NOT NULL,

        rt                         DOUBLE NOT NULL,
        rt_unit                    VARCHAR NOT NULL,

        precursor_mz               DOUBLE,
        precursor_charge           INTEGER,
        precursor_intensity        DOUBLE,
        selected_ion_mz            DOUBLE,
        selected_ion_intensity     DOUBLE,
        selected_ion_charge        INTEGER,
        collision_energy           DOUBLE,
        activation_type            VARCHAR,
        activation_cv              VARCHAR,
        isolation_window_target    DOUBLE,
        isolation_window_lower     DOUBLE,
        isolation_window_upper     DOUBLE,
        spectrum_ref               VARCHAR,

        base_peak_mz               DOUBLE,
        base_peak_intensity        DOUBLE,
        tic                        DOUBLE,
        lowest_mz                  DOUBLE,
        highest_mz                 DOUBLE,
        num_peaks                  BIGINT NOT NULL,

        polarity                   VARCHAR,
        centroided                 BOOLEAN,
        filter_string              VARCHAR,
        ion_injection_time         DOUBLE,
        ion_injection_time_unit    VARCHAR,
        monoisotopic_mz            DOUBLE,
        scan_window_lower          DOUBLE,
        scan_window_upper          DOUBLE
    )
    """,
    """
    CREATE TABLE peaks (
        scan_id      BIGINT NOT NULL,
        peak_index   BIGINT NOT NULL,
        mz           DOUBLE NOT NULL,
        intensity    REAL NOT NULL,
        PRIMARY KEY (scan_id, peak_index)
    )
    """,
    """
    CREATE TABLE run_metadata (
        key      VARCHAR PRIMARY KEY,
        value    VARCHAR
    )
    """,
]

INDEX_SQL = [
    "CREATE INDEX idx_peaks_scan_id ON peaks(scan_id)",
    "CREATE INDEX idx_peaks_mz ON peaks(mz)",
    "CREATE INDEX idx_spectra_rt ON spectra(rt)",
    "CREATE INDEX idx_spectra_precursor_mz ON spectra(precursor_mz)",
    "CREATE INDEX idx_spectra_native_id ON spectra(native_id)",
    "CREATE INDEX idx_spectra_scan_number ON spectra(scan_number)",
]

VIEW_SQL = """
CREATE VIEW spectrum_peaks AS
SELECT
    s.scan_id,
    s.native_id,
    s.rt,
    s.precursor_mz,
    s.precursor_charge,
    p.peak_index,
    p.mz,
    p.intensity
FROM spectra s
JOIN peaks p ON p.scan_id = s.scan_id
"""


def create_schema(conn):
    """Create the required v1 tables."""
    for statement in CREATE_TABLES_SQL:
        conn.execute(statement)


def create_indexes(conn):
    """Create v1 secondary indexes and the optional convenience view."""
    for statement in INDEX_SQL:
        conn.execute(statement)
    conn.execute(VIEW_SQL)


def get_table_columns(conn, table_name):
    rows = conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    return [row[1] for row in rows]


def validate_required_schema(conn):
    """Validate that a connection exposes the mzDuck v1 contract."""
    tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
    missing_tables = {"spectra", "peaks", "run_metadata"} - tables
    if missing_tables:
        missing = ", ".join(sorted(missing_tables))
        raise ValueError(f"Missing required mzDuck table(s): {missing}")

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
    rows = [(str(key), None if value is None else str(value)) for key, value in metadata.items()]
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
