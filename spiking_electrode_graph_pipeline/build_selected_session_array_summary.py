#!/usr/bin/env python3
import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Sequence


THIS_DIR = Path(__file__).resolve().parent
DEFAULT_PLOT_SCRIPT = THIS_DIR / "plot_array_firing_summary.py"
DEFAULT_OUTPUT_DIR = THIS_DIR / "output"
DEFAULT_INPUT_MAT_ROOT = THIS_DIR / "input_mats"


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Build a single array firing summary across one chosen block from each session in a manifest."
    )
    ap.add_argument("manifest_json", type=Path)
    ap.add_argument(
        "--root-derived",
        type=Path,
        default=DEFAULT_INPUT_MAT_ROOT,
    )
    ap.add_argument("--plot-script", type=Path, default=DEFAULT_PLOT_SCRIPT)
    ap.add_argument("--tx-key", type=str, default="tx_from_ns5_45")
    ap.add_argument("--window-sec", type=float, default=30.0)
    ap.add_argument("--firing-threshold-hz", type=float, default=2.0)
    ap.add_argument("--array-size", type=int, default=64)
    ap.add_argument("--x-label", type=str, default="Session index")
    ap.add_argument(
        "--plot-title",
        type=str,
        default=None,
    )
    ap.add_argument(
        "--out-prefix",
        type=Path,
        default=None,
        help="Defaults to <this pipeline>/output/selected_session_array_firing_summary.",
    )
    return ap.parse_args()


def load_entries(manifest_path: Path) -> Sequence[Dict[str, object]]:
    with manifest_path.open() as f:
        manifest = json.load(f)

    entries = manifest.get("entries")
    if not isinstance(entries, list) or not entries:
        raise ValueError(f"Manifest does not contain a non-empty 'entries' list: {manifest_path}")
    return entries


def block_feature_path(root_derived: Path, session: str, block_id: str) -> Path:
    return root_derived / session / "ns5_block_features" / f"{block_id}.mat"


def main() -> None:
    args = parse_args()

    entries = load_entries(args.manifest_json)
    mat_paths: List[Path] = []
    session_labels: List[str] = []
    for idx, entry in enumerate(entries, start=1):
        session = str(entry["session"])
        block_id = str(entry["chosen_block"])
        mat_path = block_feature_path(args.root_derived, session, block_id)
        if not mat_path.exists():
            raise FileNotFoundError(
                f"Missing block feature for session {session} block {block_id}: {mat_path}"
            )
        mat_paths.append(mat_path)
        session_labels.append(session)
        print(f"{idx}: session={session} block={block_id} mat={mat_path}")

    if len(session_labels) != len(set(session_labels)):
        raise ValueError(f"Manifest contains duplicate session entries: {args.manifest_json}")

    out_prefix = args.out_prefix or (DEFAULT_OUTPUT_DIR / "selected_session_array_firing_summary")
    out_prefix.parent.mkdir(parents=True, exist_ok=True)
    plot_title = args.plot_title or f"Firing electrodes per array across {len(entries)} sessions"

    cmd = [
        sys.executable,
        os.fspath(args.plot_script),
        *[os.fspath(p) for p in mat_paths],
        "--tx-key",
        args.tx_key,
        "--window-sec",
        str(float(args.window_sec)),
        "--firing-threshold-hz",
        str(float(args.firing_threshold_hz)),
        "--array-size",
        str(int(args.array_size)),
        "--x-label",
        args.x_label,
        "--plot-title",
        plot_title,
        "--out-prefix",
        os.fspath(out_prefix),
        "--x-tick-label-mode",
        "index",
        "--x-tick-label-start",
        "1",
        "--labels",
        *session_labels,
    ]

    print("running:", " ".join(cmd))
    subprocess.check_call(cmd)


if __name__ == "__main__":
    main()
