"""Command line interface for mzDuck."""

from __future__ import annotations

import argparse
import json
import sys

from .file import MzDuckFile


class HelpFormatter(argparse.RawDescriptionHelpFormatter, argparse.ArgumentDefaultsHelpFormatter):
    """Keep examples readable while still showing defaults."""


MAIN_DESCRIPTION = """\
mzDuck stores centroid MS2 mzML data in a DuckDB-backed .mzduck file.

It imports one mzML run into one .mzduck file, lets you inspect/query that
database, and exports semantic MS2 spectra back to MGF or mzML.
"""


MAIN_EPILOG = """\
Examples:
  mzduck convert input.mzML -o output.mzduck --overwrite
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
  mzDuck stores peak m/z as DOUBLE and intensity as REAL.
  mzML export defaults to --mz64 --inten32.
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
        description="Convert one centroid MS2 mzML file into one .mzduck file.",
        epilog="""\
Examples:
  mzduck convert input.mzML -o output.mzduck --overwrite
  mzduck convert mzduck/example_data/tiny.mzML /tmp/tiny.mzduck --batch-size 1 --no-sha256
""",
    )
    convert.add_argument("input_mzml", metavar="input.mzML")
    convert.add_argument("output_mzduck", nargs="?", metavar="output.mzduck")
    convert.add_argument("-o", "--out", dest="out", metavar="output.mzduck")
    convert.add_argument("--overwrite", action="store_true", help="replace an existing output file")
    convert.add_argument("--batch-size", type=int, default=5000, help="number of spectra per insert batch")
    convert.add_argument("--no-sha256", action="store_true", help="skip source file SHA-256 hashing")

    export_mgf = subparsers.add_parser(
        "export-mgf",
        help="export mzDuck to MGF",
        formatter_class=HelpFormatter,
        description="Export spectra from a .mzduck file to Mascot Generic Format.",
        epilog="""\
Examples:
  mzduck export-mgf input.mzduck -o output.mgf --overwrite
  mzduck export-mgf mzduck/example_data/tiny.mzduck /tmp/tiny.mgf
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
        description="Export spectra from a .mzduck file to semantic mzML with psims.",
        epilog="""\
Examples:
  mzduck export-mzml input.mzduck -o output.mzML --overwrite
  mzduck export-mzml input.mzduck -o output.float32.mzML --32
  mzduck export-mzml input.mzduck -o output.mz64-int32.mzML --mz64 --inten32
  mzduck export-mzml input.mzduck -o output.mz32-int64.mzML --mz32 --inten64
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
        description="Print counts, ranges, metadata, and precursor summaries for a .mzduck file.",
        epilog="""\
Examples:
  mzduck inspect input.mzduck
  mzduck inspect input.mzduck --json
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

    mz_precision = 64
    intensity_precision = 32
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
                compute_sha256=not args.no_sha256,
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
