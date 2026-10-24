"""Shared setup for the examples: argument parsing and a client."""

import argparse
from pathlib import Path

import tpuf_arrow as ta

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"


def parser(doc: str | None) -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=doc, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument(
        "--namespace", default="tpuf-arrow-sample", help="namespace to read (default: %(default)s)"
    )
    return ap


def client() -> ta.Client:
    """TURBOPUFFER_API_KEY from the environment, or from the repo's .env if it exists."""
    env = ROOT / ".env"
    return ta.connect(env_file=env if env.is_file() else None)
