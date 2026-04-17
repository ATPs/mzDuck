"""Command line interface for mzDuck."""

from __future__ import annotations

import argparse
import json
import sys

from .file import MzDuckFile


def build_parser():
    parser = argparse.ArgumentParser(prog="mzduck")
    subparsers = parser.add_subparsers(dest="command", required=True)

    convert = subparsers.add_parser("convert", help="convert mzML or mzMLb to mzDuck")
    convert.add_argument("input_mzml")
    convert.add_argument("output_mzduck")
    convert.add_argument("--overwrite", action="store_true")
    convert.add_argument("--batch-size", type=int, default=5000)
    convert.add_argument("--no-sha256", action="store_true")

    export_mgf = subparsers.add_parser("export-mgf", help="export mzDuck to MGF")
    export_mgf.add_argument("input_mzduck")
    export_mgf.add_argument("output_mgf")
    export_mgf.add_argument("--overwrite", action="store_true")

    export_mzml = subparsers.add_parser("export-mzml", help="export mzDuck to mzML")
    export_mzml.add_argument("input_mzduck")
    export_mzml.add_argument("output_mzml")
    export_mzml.add_argument("--overwrite", action="store_true")

    export_mzmlb = subparsers.add_parser("export-mzmlb", help="export mzDuck to mzMLb")
    export_mzmlb.add_argument("input_mzduck")
    export_mzmlb.add_argument("output_mzmlb")
    export_mzmlb.add_argument("--overwrite", action="store_true")

    inspect = subparsers.add_parser("inspect", help="summarize an mzDuck file")
    inspect.add_argument("input_mzduck")
    inspect.add_argument("--json", action="store_true")
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "convert":
            handle = MzDuckFile.from_mzml(
                args.input_mzml,
                args.output_mzduck,
                overwrite=args.overwrite,
                batch_size=args.batch_size,
                compute_sha256=not args.no_sha256,
            )
            handle.close()
            return 0
        if args.command == "export-mgf":
            with MzDuckFile.open(args.input_mzduck, read_only=True) as handle:
                handle.to_mgf(args.output_mgf, overwrite=args.overwrite)
            return 0
        if args.command == "export-mzml":
            with MzDuckFile.open(args.input_mzduck, read_only=True) as handle:
                handle.to_mzml(args.output_mzml, overwrite=args.overwrite)
            return 0
        if args.command == "export-mzmlb":
            with MzDuckFile.open(args.input_mzduck, read_only=True) as handle:
                handle.to_mzmlb(args.output_mzmlb, overwrite=args.overwrite)
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
