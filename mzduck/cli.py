"""Command line interface for mzDuck."""

from __future__ import annotations

import argparse
import json
import sys

from .file import MzDuckFile


class HelpFormatter(argparse.RawDescriptionHelpFormatter, argparse.ArgumentDefaultsHelpFormatter):
    """Keep examples readable while still showing defaults."""


MAIN_DESCRIPTION = """\
mzDuck stores centroid mzML spectra in a DuckDB-backed .mzduck file.

The v2 layout keeps one canonical MS2 storage table, exposes an MGF
compatibility view for SQL and export, stores sparse exact text fallbacks when
reconstruction is not trustworthy, and writes the final database through a
fresh-file compact-copy step so the on-disk file stays small.
"""


MAIN_EPILOG = """\
Layout:
  run_metadata
    run-level provenance, header fragments, templates, filter-string policy,
    counts, and table registry.
  ms1_spectra
    physical MS1 spectra plus arrays when MS1 is included.
  ms2_spectra
    canonical physical MS2 spectra plus arrays. This is the v2 storage anchor.
  mgf
    derived compatibility view with computed TITLE values. It is queryable and
    exportable, but it is not physically stored in v2.
  spectrum_text_overrides
    sparse exact text fallbacks for native_id, spectrum_ref, and filter_string.
  spectrum_extra_params
    mzML params not covered by typed columns, grouped by scope.

Reconstructed vs stored fields:
  TITLE is always derived from run metadata plus scan_number/charge.
  native_id and spectrum_ref use run-level templates when exact; otherwise
  mzDuck stores only the original rows that need exact fallback.
  filter_string uses per-run encoder detection. An encoder is accepted only
  when it reproduces every selected spectrum exactly. If not, mzDuck stores the
  original strings instead of inventing an approximate rule.

Compaction:
  mzDuck writes to a staging DuckDB file and then copies the database into a
  fresh final file. This avoids carrying free blocks from the first write and
  is part of the normal v2 output path.

Round-trip scope:
  mzDuck targets semantic round-trip for supported centroid mzML content.
  Exported mzML is not expected to be byte-identical XML, but typed fields,
  templates, exact text overrides, and stored extra params are used to keep the
  reconstructed spectra faithful to the source run.

Examples:
  mzduck convert input.mzML -o output.mzduck --overwrite
  mzduck convert input.withMS1.mzML -o output.mzduck --index-scan
  mzduck convert input.mzML -o output.mzduck --ms2-mgf-only --start-scan 100 --end-scan 500
  mzduck convert input.withMS1.mzML -o ms1-only.mzduck --ms1-only
  mzduck convert input.withMS1.mzML -o ms2-only.mzduck --ms2-only
  mzduck convert mzduck/example_data/tiny.mzML -o /tmp/tiny.mzduck --batch-size 1 --no-sha256
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
  Default convert mode keeps canonical MS2 storage, MS1 when present, MSn when
  present, run_metadata, and the v2 fallback tables.
  In v2, --ms2-mgf-only and --ms2-only both produce an MS2-only compact file.
  The difference is mainly user intent and metadata; the old physical mgf-only
  split no longer exists because mgf is derived from canonical MS2 storage.
  --index-scan creates idx_mgf_scan_number on the canonical MS2 table so exact
  scan lookups through the mgf compatibility view stay fast.
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
        help="convert mzML to mzDuck",
        formatter_class=HelpFormatter,
        description="Convert one centroid mzML file into one .mzduck file.",
        epilog="""\
Examples:
  mzduck convert input.mzML -o output.mzduck --overwrite
  mzduck convert input.withMS1.mzML -o output.mzduck --index-scan
  mzduck convert input.mzML -o mgf-only.mzduck --ms2-mgf-only --overwrite
  mzduck convert input.withMS1.mzML -o no-ms1.mzduck --no-ms1
  mzduck convert input.withMS1.mzML -o scan-window.mzduck --start-scan 1000 --end-scan 2000
  mzduck convert mzduck/example_data/tiny.mzML /tmp/tiny.mzduck --batch-size 1 --no-sha256

Table modes:
  default         run_metadata, canonical ms2_spectra, derived mgf view,
                  MS1 when present, and MSn tables when present
  --ms2-mgf-only  MS2-only compact v2 file; mgf remains a derived view
  --ms2-only      MS2-only compact v2 file; equivalent storage shape to
                  --ms2-mgf-only in v2
  --ms1-only      run_metadata plus ms1_spectra only
""",
    )
    convert.add_argument("input_mzml", metavar="input.mzML")
    convert.add_argument("output_mzduck", nargs="?", metavar="output.mzduck")
    convert.add_argument("-o", "--out", dest="out", metavar="output.mzduck")
    convert.add_argument("--overwrite", action="store_true", help="replace an existing output file")
    convert.add_argument("--batch-size", type=int, default=5000, help="number of spectra per insert batch")
    convert.add_argument("--no-sha256", action="store_true", help="skip source file SHA-256 hashing")
    convert.add_argument(
        "--compression",
        choices=["zstd", "auto", "uncompressed"],
        default="zstd",
        help="force DuckDB column compression before writing",
    )
    convert.add_argument(
        "--compression-level",
        type=int,
        default=6,
        help="requested zstd compression level, recorded if DuckDB ignores it",
    )
    convert.add_argument(
        "--index-scan-number",
        action="store_true",
        help="deprecated alias for --index-scan",
    )
    convert.add_argument(
        "--index-scan",
        action="store_true",
        help="create idx_mgf_scan_number on canonical ms2_spectra(scan_number)",
    )
    mode = convert.add_argument_group("table selection")
    mode.add_argument(
        "--ms2-mgf-only",
        action="store_true",
        help="build an MS2-only compact v2 file with a derived mgf view",
    )
    mode.add_argument(
        "--no-ms1",
        action="store_true",
        help="skip MS1 spectra in default mode",
    )
    mode.add_argument(
        "--ms2-only",
        action="store_true",
        help="build an MS2-only compact v2 file with canonical ms2_spectra storage",
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

    export_mgf = subparsers.add_parser(
        "export-mgf",
        help="export mzDuck to MGF",
        formatter_class=HelpFormatter,
        description=(
            "Export spectra from a .mzduck file to Mascot Generic Format using "
            "the public mgf compatibility view."
        ),
        epilog="""\
Examples:
  mzduck export-mgf input.mzduck -o output.mgf --overwrite
  mzduck export-mgf mzduck/example_data/tiny.mzduck /tmp/tiny.mgf

Notes:
  In v2 the mgf relation is a derived view, not a stored base table.
  TITLE values are reconstructed from run metadata and scan_number/charge.
""",
    )
    export_mgf.add_argument("input_mzduck", metavar="input.mzduck")
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
  exists for that file.
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
  spectrum_summary table. The inspect command reports the derived view of the
  file layout regardless of storage version.
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
            output = resolve_output(args, "output_mzduck", parser)
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
        if args.command == "export-mgf":
            output = resolve_output(args, "output_mgf", parser)
            with MzDuckFile.open(args.input_mzduck, read_only=True) as handle:
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
