"""Command line interface for mzDuck."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .export_mgf import export_mgf_parquet
from .file import MzDuckFile
from .import_mzml import convert_mzml_to_mgf_parquet, convert_mzml_to_parquet


class HelpFormatter(argparse.RawDescriptionHelpFormatter, argparse.ArgumentDefaultsHelpFormatter):
    """Keep examples readable while still showing defaults."""


MAIN_DESCRIPTION = """\
mzDuck stores centroid mzML spectra in a compact relational container.

The default container is DuckDB-backed `.mzduck`, but `mzduck convert` can also
write physical tables as Parquet files in a folder or in an uncompressed zip,
and `mzduck mzml-mgf` can write one self-describing single-file MGF parquet.
The v2 layout stores MS2 payload in a physical `mgf` table, keeps richer mzML
detail in `ms2_spectra` when requested, stores sparse exact fallbacks only when
reconstruction is not exact, and writes DuckDB output through a fresh-file
compact-copy step so the final file stays small.
"""


MAIN_EPILOG = """\
Layout:
  run_metadata
    run-level provenance, header fragments, templates, filter-string policy,
    counts, table registry, `container_format`, and `source_compression`.
  mgf
    physical MS2 payload table. It stores scan_number, source order, RT,
    precursor payload, and peak arrays.
  ms1_spectra
    physical MS1 spectra plus arrays when MS1 is included.
  ms2_spectra
    physical MS2 detail-only table keyed by scan_number. It stores mzML detail
    that is not part of the MGF-native payload.
  spectrum_text_overrides
    sparse exact text fallbacks for native_id, spectrum_ref, and filter_string.
    Empty optional tables are omitted from final output.
  spectrum_extra_params
    mzML params not covered by typed columns, grouped by scope.
    Empty optional tables are omitted from final output.

Reconstructed vs stored fields:
  TITLE is always derived from run metadata plus scan_number/charge.
  native_id and spectrum_ref use run-level templates when exact; otherwise
  mzDuck stores only the original rows that need exact fallback.
  filter_string uses per-run encoder detection. An encoder is accepted only
  when it reproduces every selected spectrum exactly. If not, mzDuck stores the
  original strings instead of inventing an approximate rule.
  In `--ms2-mgf-only`, only the MGF-native payload is stored. That mode keeps
  `run_metadata` plus physical `mgf` and intentionally omits richer mzML
  detail tables.

Compaction:
  mzDuck writes DuckDB output to a staging database and then copies into a
  fresh final file. Parquet output modes export only physical relations that
  are actually present, and empty optional tables are omitted there too.

Round-trip scope:
  mzDuck targets semantic round-trip for supported centroid mzML content.
  Exported mzML is not expected to be byte-identical XML, but typed fields,
  templates, exact text overrides, and stored extra params are used to keep the
  reconstructed spectra faithful to the source run when detail tables are
  present. `--ms2-mgf-only` preserves the MGF-native contract, not full mzML
  detail.

Examples:
  mzduck convert input.mzML -o output.mzduck --overwrite
  mzduck convert input.mzML.gz -o output.mzduck --overwrite
  mzduck convert input.mzML.gz -o out-dir --parquet --overwrite
  mzduck convert input.mzML.gz -o out.parquet.zip --parquet-zip --overwrite
  mzduck convert input.withMS1.mzML -o output.mzduck --index-scan
  mzduck convert input.mzML -o output.mzduck --ms2-mgf-only --start-scan 100 --end-scan 500
  mzduck convert input.withMS1.mzML -o ms1-only.mzduck --ms1-only
  mzduck convert input.withMS1.mzML -o ms2-only.mzduck --ms2-only
  mzduck convert mzduck/example_data/tiny.mzML -o /tmp/tiny.mzduck --batch-size 1 --no-sha256
  mzduck mzml-mgf input.mzML.gz -o output.mgf.parquet --overwrite
  mzduck inspect /tmp/tiny.mzduck --json
  mzduck export-mgf /tmp/tiny.mzduck -o /tmp/tiny.mgf --overwrite
  mzduck export-mzml /tmp/tiny.mzduck -o /tmp/tiny.mzML --mz64 --inten32 --overwrite
  mzduck export-mzml /tmp/tiny.mzduck -o /tmp/tiny.float32.mzML --32 --overwrite

Output paths:
  Commands that write files accept either a positional output path or -o/--out.
  For example, these are equivalent:
    mzduck convert input.mzML output.mzduck
    mzduck convert input.mzML -o output.mzduck

mzML precision flags for export-mzml:
  --32       write both m/z and intensity binary arrays as 32-bit floats
  --64       write both m/z and intensity binary arrays as 64-bit floats
  --mz32     write only m/z arrays as 32-bit floats
  --mz64     write only m/z arrays as 64-bit floats
  --inten32  write only intensity arrays as 32-bit floats
  --inten64  write only intensity arrays as 64-bit floats

Defaults:
  Default convert mode keeps physical `mgf`, detail `ms2_spectra`, MS1 when
  present, MSn when present, run_metadata, and non-empty fallback tables.
  --ms2-only keeps `run_metadata`, physical `mgf`, detail `ms2_spectra`, and
  any non-empty fallback tables.
  --ms2-mgf-only keeps only `run_metadata` and physical `mgf`.
  --index-scan creates `idx_mgf_scan_number` on `mgf(scan_number)` so exact
  scan lookups stay fast.
  mzml-mgf writes a single self-describing parquet file that includes the MGF
  payload plus derived `title`, `rt_unit`, and `rt_seconds`. In that parquet,
  `title` stores the file-name title source, and export-mgf reconstructs the
  full per-spectrum TITLE from title + scan_number + charge.
  mzDuck stores peak arrays in the source dtype where possible.
  mzML export defaults to the stored/source dtype unless precision flags are used.
"""


def build_parser():
    parser = argparse.ArgumentParser(
        prog="mzduck",
        formatter_class=HelpFormatter,
        description=MAIN_DESCRIPTION,
        epilog=MAIN_EPILOG,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    convert = subparsers.add_parser(
        "convert",
        help="convert mzML to mzDuck or Parquet",
        formatter_class=HelpFormatter,
        description="Convert one centroid mzML or mzML.gz file into DuckDB-backed mzDuck or physical Parquet output.",
        epilog="""\
Examples:
  mzduck convert input.mzML -o output.mzduck --overwrite
  mzduck convert input.mzML.gz -o output.mzduck --overwrite
  mzduck convert input.withMS1.mzML -o output.mzduck --index-scan
  mzduck convert input.mzML -o mgf-only.mzduck --ms2-mgf-only --overwrite
  mzduck convert input.mzML.gz -o out-dir --parquet --overwrite
  mzduck convert input.mzML.gz -o out.parquet.zip --parquet-zip --overwrite
  mzduck convert input.withMS1.mzML -o no-ms1.mzduck --no-ms1
  mzduck convert input.withMS1.mzML -o scan-window.mzduck --start-scan 1000 --end-scan 2000
  mzduck convert mzduck/example_data/tiny.mzML /tmp/tiny.mzduck --batch-size 1 --no-sha256

Table modes:
  default         run_metadata, physical mgf, detail ms2_spectra,
                  MS1 when present, MSn tables when present, and non-empty
                  fallback tables
  --ms2-mgf-only  run_metadata plus physical mgf only
  --ms2-only      run_metadata, physical mgf, detail ms2_spectra, and any
                  non-empty fallback tables
  --ms1-only      run_metadata plus ms1_spectra only

Container modes:
  default         write one DuckDB-backed .mzduck file
  --parquet       write one parquet file per physical relation into a folder
  --parquet-zip   write the same parquet members into one zip file with no
                  zip-level compression
""",
    )
    convert.add_argument("input_mzml", metavar="input.mzML[.gz]")
    convert.add_argument("output_mzduck", nargs="?", metavar="output")
    convert.add_argument("-o", "--out", dest="out", metavar="output")
    convert.add_argument("--overwrite", action="store_true", help="replace an existing output file or folder")
    convert.add_argument("--batch-size", type=int, default=5000, help="number of spectra per insert batch")
    convert.add_argument("--no-sha256", action="store_true", help="skip source file SHA-256 hashing")
    convert.add_argument(
        "--compression",
        choices=["zstd", "auto", "uncompressed"],
        default="zstd",
        help="compression policy for DuckDB output and Parquet members",
    )
    convert.add_argument(
        "--compression-level",
        type=int,
        default=6,
        help="requested zstd compression level when zstd is used",
    )
    convert.add_argument(
        "--index-scan-number",
        action="store_true",
        help="deprecated alias for --index-scan",
    )
    convert.add_argument(
        "--index-scan",
        action="store_true",
        help="create idx_mgf_scan_number on physical mgf(scan_number)",
    )
    container = convert.add_argument_group("container selection")
    container.add_argument(
        "--parquet",
        action="store_true",
        help="write one parquet file per physical relation into an output folder",
    )
    container.add_argument(
        "--parquet-zip",
        action="store_true",
        help="write parquet members into one uncompressed zip archive",
    )
    mode = convert.add_argument_group("table selection")
    mode.add_argument(
        "--ms2-mgf-only",
        action="store_true",
        help="build an MS2-only compact file with run_metadata plus physical mgf only",
    )
    mode.add_argument(
        "--no-ms1",
        action="store_true",
        help="skip MS1 spectra in default mode",
    )
    mode.add_argument(
        "--ms2-only",
        action="store_true",
        help="build an MS2-only compact file with physical mgf plus detail ms2_spectra",
    )
    mode.add_argument(
        "--ms1-only",
        action="store_true",
        help="keep only MS1 spectra and run_metadata",
    )
    mode.add_argument(
        "--start-scan",
        type=int,
        help="inclusive lower scan_number bound",
    )
    mode.add_argument(
        "--end-scan",
        type=int,
        help="inclusive upper scan_number bound",
    )

    mzml_mgf = subparsers.add_parser(
        "mzml-mgf",
        help="convert mzML to a single self-describing MGF parquet file",
        formatter_class=HelpFormatter,
        description=(
            "Convert one centroid mzML or mzML.gz file into one self-describing "
            "MGF parquet file."
        ),
        epilog="""\
Examples:
  mzduck mzml-mgf input.mzML -o output.mgf.parquet --overwrite
  mzduck mzml-mgf input.mzML.gz -o output.mgf.parquet --overwrite
  mzduck mzml-mgf input.withMS1.mzML -o window.mgf.parquet --start-scan 1000 --end-scan 2000

Notes:
  This command writes one parquet file, not a parquet container. The output
  includes the physical MGF payload columns plus derived `title`, `rt_unit`,
  and `rt_seconds`. The `title` column stores the file-name title source only;
  export-mgf reconstructs the full per-spectrum TITLE from title,
  scan_number, and charge, so no separate run_metadata file is needed.
  `mzduck convert --parquet` still writes physical relations as-is.
""",
    )
    mzml_mgf.add_argument("input_mzml", metavar="input.mzML[.gz]")
    mzml_mgf.add_argument("output_mgf_parquet", nargs="?", metavar="output.mgf.parquet")
    mzml_mgf.add_argument("-o", "--out", dest="out", metavar="output.mgf.parquet")
    mzml_mgf.add_argument(
        "--overwrite",
        action="store_true",
        help="replace an existing output file",
    )
    mzml_mgf.add_argument(
        "--batch-size",
        type=int,
        default=5000,
        help="number of spectra per insert batch",
    )
    mzml_mgf.add_argument(
        "--compression",
        choices=["zstd", "auto", "uncompressed"],
        default="zstd",
        help="compression policy for the output parquet file",
    )
    mzml_mgf.add_argument(
        "--compression-level",
        type=int,
        default=6,
        help="requested zstd compression level when zstd is used",
    )
    mzml_mgf.add_argument(
        "--start-scan",
        type=int,
        help="inclusive lower scan_number bound",
    )
    mzml_mgf.add_argument(
        "--end-scan",
        type=int,
        help="inclusive upper scan_number bound",
    )

    export_mgf = subparsers.add_parser(
        "export-mgf",
        help="export mzDuck or self-describing MGF parquet to MGF",
        formatter_class=HelpFormatter,
        description=(
            "Export spectra from a .mzduck file or self-describing .mgf.parquet "
            "file to Mascot Generic Format."
        ),
        epilog="""\
Examples:
  mzduck export-mgf input.mzduck -o output.mgf --overwrite
  mzduck export-mgf input.mgf.parquet -o output.mgf --overwrite
  mzduck export-mgf mzduck/example_data/tiny.mzduck /tmp/tiny.mgf

Notes:
  `.mzduck` input uses the stored physical mgf table and reconstructs TITLE
  from run metadata plus scan_number/charge.
  `.mgf.parquet` input is supported only for the self-describing parquet files
  produced by `mzduck mzml-mgf`, where `title` stores only the title source.
""",
    )
    export_mgf.add_argument("input_source", metavar="input")
    export_mgf.add_argument("output_mgf", nargs="?", metavar="output.mgf")
    export_mgf.add_argument("-o", "--out", dest="out", metavar="output.mgf")
    export_mgf.add_argument("--overwrite", action="store_true", help="replace an existing output file")

    export_mzml = subparsers.add_parser(
        "export-mzml",
        help="export mzDuck to mzML",
        formatter_class=HelpFormatter,
        description=(
            "Export spectra from a .mzduck file to semantic mzML with psims, "
            "using typed columns plus exact fallback metadata when needed."
        ),
        epilog="""\
Examples:
  mzduck export-mzml input.mzduck -o output.mzML --overwrite
  mzduck export-mzml input.mzduck -o output.float32.mzML --32
  mzduck export-mzml input.mzduck -o output.mz64-int32.mzML --mz64 --inten32
  mzduck export-mzml input.mzduck -o output.mz32-int64.mzML --mz32 --inten64

Notes:
  native_id, spectrum_ref, and filter_string may be reconstructed from
  templates/encoders or taken from exact stored overrides when no exact rule
  exists for that file. In `--ms2-mgf-only`, export uses the physical mgf
  payload and whatever run-level reconstruction metadata is still available.
""",
    )
    export_mzml.add_argument("input_mzduck", metavar="input.mzduck")
    export_mzml.add_argument("output_mzml", nargs="?", metavar="output.mzML")
    export_mzml.add_argument("-o", "--out", dest="out", metavar="output.mzML")
    export_mzml.add_argument("--overwrite", action="store_true", help="replace an existing output file")
    add_precision_arguments(export_mzml)

    inspect = subparsers.add_parser(
        "inspect",
        help="summarize an mzDuck file",
        formatter_class=HelpFormatter,
        description=(
            "Print counts, ranges, storage metadata, reconstruction policy, and "
            "table registry details for a .mzduck file."
        ),
        epilog="""\
Examples:
  mzduck inspect input.mzduck
  mzduck inspect input.mzduck --json

Notes:
  v2 stores summary counts in run_metadata instead of a physical
  spectrum_summary table. It also omits empty optional tables from the final
  file, so inspect reports only the physical relations that actually remain.
""",
    )
    inspect.add_argument("input_mzduck", metavar="input.mzduck")
    inspect.add_argument("--json", action="store_true", help="write machine-readable JSON")
    return parser


def add_precision_arguments(parser):
    group = parser.add_argument_group("mzML binary array precision")
    group.add_argument("--32", dest="precision32", action="store_true", help="write m/z and intensity as 32-bit floats")
    group.add_argument("--64", dest="precision64", action="store_true", help="write m/z and intensity as 64-bit floats")
    group.add_argument("--mz32", action="store_true", help="write m/z arrays as 32-bit floats")
    group.add_argument("--mz64", action="store_true", help="write m/z arrays as 64-bit floats")
    group.add_argument("--inten32", action="store_true", help="write intensity arrays as 32-bit floats")
    group.add_argument("--inten64", action="store_true", help="write intensity arrays as 64-bit floats")


def resolve_output(args, positional_name, parser):
    positional = getattr(args, positional_name)
    option = getattr(args, "out", None)
    if positional and option:
        parser.error("provide the output path either positionally or with -o/--out, not both")
    output = option or positional
    if output is None:
        parser.error("missing output path; provide one positionally or with -o/--out")
    return output


def resolve_precision(args, parser):
    if args.precision32 and args.precision64:
        parser.error("--32 and --64 cannot be used together")
    if args.mz32 and args.mz64:
        parser.error("--mz32 and --mz64 cannot be used together")
    if args.inten32 and args.inten64:
        parser.error("--inten32 and --inten64 cannot be used together")

    mz_precision = None
    intensity_precision = None
    if args.precision32:
        mz_precision = 32
        intensity_precision = 32
    if args.precision64:
        mz_precision = 64
        intensity_precision = 64
    if args.mz32:
        mz_precision = 32
    if args.mz64:
        mz_precision = 64
    if args.inten32:
        intensity_precision = 32
    if args.inten64:
        intensity_precision = 64
    return mz_precision, intensity_precision


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "convert":
            if args.parquet and args.parquet_zip:
                parser.error("--parquet and --parquet-zip cannot be used together")
            output = resolve_output(args, "output_mzduck", parser)
            if args.parquet or args.parquet_zip:
                convert_mzml_to_parquet(
                    args.input_mzml,
                    output,
                    overwrite=args.overwrite,
                    batch_size=args.batch_size,
                    compression=args.compression,
                    compression_level=args.compression_level,
                    index_scan=args.index_scan,
                    index_scan_number=args.index_scan_number,
                    compute_sha256=not args.no_sha256,
                    ms2_mgf_only=args.ms2_mgf_only,
                    no_ms1=args.no_ms1,
                    ms2_only=args.ms2_only,
                    ms1_only=args.ms1_only,
                    start_scan=args.start_scan,
                    end_scan=args.end_scan,
                    zip_output=args.parquet_zip,
                )
            else:
                handle = MzDuckFile.from_mzml(
                    args.input_mzml,
                    output,
                    overwrite=args.overwrite,
                    batch_size=args.batch_size,
                    compression=args.compression,
                    compression_level=args.compression_level,
                    index_scan=args.index_scan,
                    index_scan_number=args.index_scan_number,
                    compute_sha256=not args.no_sha256,
                    ms2_mgf_only=args.ms2_mgf_only,
                    no_ms1=args.no_ms1,
                    ms2_only=args.ms2_only,
                    ms1_only=args.ms1_only,
                    start_scan=args.start_scan,
                    end_scan=args.end_scan,
                )
                handle.close()
            return 0
        if args.command == "mzml-mgf":
            output = resolve_output(args, "output_mgf_parquet", parser)
            convert_mzml_to_mgf_parquet(
                args.input_mzml,
                output,
                overwrite=args.overwrite,
                batch_size=args.batch_size,
                compression=args.compression,
                compression_level=args.compression_level,
                start_scan=args.start_scan,
                end_scan=args.end_scan,
            )
            return 0
        if args.command == "export-mgf":
            output = resolve_output(args, "output_mgf", parser)
            input_path = Path(args.input_source)
            if input_path.name.lower().endswith(".parquet"):
                export_mgf_parquet(
                    input_path,
                    output,
                    overwrite=args.overwrite,
                )
            else:
                with MzDuckFile.open(input_path, read_only=True) as handle:
                    handle.to_mgf(output, overwrite=args.overwrite)
            return 0
        if args.command == "export-mzml":
            output = resolve_output(args, "output_mzml", parser)
            mz_precision, intensity_precision = resolve_precision(args, parser)
            with MzDuckFile.open(args.input_mzduck, read_only=True) as handle:
                handle.to_mzml(
                    output,
                    overwrite=args.overwrite,
                    mz_precision=mz_precision,
                    intensity_precision=intensity_precision,
                )
            return 0
        if args.command == "inspect":
            with MzDuckFile.open(args.input_mzduck, read_only=True) as handle:
                summary = handle.inspect()
            if args.json:
                print(json.dumps(summary, indent=2, sort_keys=True))
            else:
                for key, value in summary.items():
                    print(f"{key}: {value}")
            return 0
    except Exception as exc:
        print(f"mzduck: error: {exc}", file=sys.stderr)
        return 1
    parser.error(f"Unhandled command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
