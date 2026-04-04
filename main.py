#!/usr/bin/env python3

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "api"))


def run_extractor(args):
    from pipeline.extract import main
    main()


def build_parser():
    parser = argparse.ArgumentParser(prog="anton", description="Anton RX project CLI")
    sub = parser.add_subparsers(dest="command", metavar="<command>")
    sub.required = True

    sub.add_parser("extract", help="Run the extractor pipeline").set_defaults(func=run_extractor)

    return parser


if __name__ == "__main__":
    args = build_parser().parse_args()
    args.func(args)
