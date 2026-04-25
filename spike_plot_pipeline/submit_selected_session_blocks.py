#!/usr/bin/env python3
"""
Spike-plot multi-session chosen-block manifest builder.

This is a thin wrapper around the selected-session graph pipeline's
multi-session chosen-block manifest builder. It preserves the same
session/block selection criteria while defaulting the manifest and worker paths
needed by the spike plotting flow.
"""

import subprocess
import sys
from pathlib import Path


def main() -> None:
    script_dir = Path(__file__).resolve().parent
    chunks_dir = script_dir.parent
    graph_dir = chunks_dir / "spiking_electrode_graph_pipeline"
    other_dir = chunks_dir / "other"
    target_script = graph_dir / "submit_selected_session_blocks.py"

    default_args = [
        "--root-derived", str(graph_dir / "input_mats"),
        "--repo-dir", str(other_dir),
        "--script-path", str(graph_dir / "run_selected_session_blocks.sbatch"),
        "--manifest-out", str(graph_dir / "selected_session_blocks_manifest.json"),
    ]
    cmd = [sys.executable, str(target_script), *default_args, *sys.argv[1:]]
    raise SystemExit(subprocess.call(cmd))


if __name__ == "__main__":
    main()
