#!/usr/bin/env python3
"""
Plot every chosen block referenced by a multi-session chosen-block manifest.
"""

import argparse
import json
from pathlib import Path

from plot_chunk_mats import DEFAULT_TX_KEY, process_mat


THIS_DIR = Path(__file__).resolve().parent
DEFAULT_PLOT_ROOT = THIS_DIR / "output" / "block_plots"


def resolve_feature_path(root_derived: Path, session: str, block: str) -> Path:
    feature_dir = root_derived / session / "ns5_block_features"
    for suffix in (".mat", ".npz"):
        candidate = feature_dir / f"{block}{suffix}"
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"Missing feature output for session={session} block={block} under {feature_dir}"
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "manifest_json",
        help="Multi-session chosen-block manifest JSON, typically selected_session_blocks_manifest.json",
    )
    ap.add_argument(
        "--root-derived",
        default="",
        help="Feature root containing <session>/ns5_block_features/<block>.mat",
    )
    ap.add_argument(
        "--plot-root",
        default=str(DEFAULT_PLOT_ROOT),
        help="Root output directory; plots are written under <plot-root>/<session>/",
    )
    ap.add_argument(
        "--tx-key",
        default=DEFAULT_TX_KEY,
        help=f"Specific tx field. Default: {DEFAULT_TX_KEY}",
    )
    args = ap.parse_args()

    manifest_path = Path(args.manifest_json).resolve()
    with manifest_path.open() as f:
        manifest = json.load(f)

    root_derived = Path(args.root_derived or manifest["root_derived"]).resolve()
    plot_root = Path(args.plot_root).resolve()

    for entry in manifest["entries"]:
        session = entry["session"]
        block = str(entry["chosen_block"])
        feature_path = resolve_feature_path(root_derived, session, block)
        outdir = plot_root / session
        pdf_path = process_mat(str(feature_path), str(outdir), args.tx_key)
        print(f"wrote: {pdf_path}")


if __name__ == "__main__":
    main()
