#!/usr/bin/env python3
"""Show the mapping from file-order channels to physical electrode order."""

import argparse
import sys
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--save-prefix",
        type=str,
        default="",
        help="If set, write <prefix>_mapping.png instead of opening an interactive window.",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo_root / "other"))

    import matplotlib

    if args.save_prefix:
        matplotlib.use("Agg")

    import matplotlib.pyplot as plt
    import numpy as np
    from session_featurize_to_mat import _CHAN_TO_ELEC_1IDX

    file_order = np.arange(1, 129, dtype=np.int32)
    electrode_order = np.asarray(_CHAN_TO_ELEC_1IDX, dtype=np.int32)

    print("First 32 channels in file order and their mapped electrode numbers:")
    for idx in range(32):
        print(
            f"  file_order_channel={int(file_order[idx]):3d} "
            f"-> physical_electrode={int(electrode_order[idx]):3d}"
        )

    fig, axes = plt.subplots(2, 1, figsize=(10, 8), constrained_layout=True)

    axes[0].plot(file_order, electrode_order, marker="o", linewidth=1)
    axes[0].set_title("File-order channel index to physical electrode number")
    axes[0].set_xlabel("File-order channel index (1-based)")
    axes[0].set_ylabel("Mapped physical electrode")

    grid = np.full((8, 16), np.nan, dtype=np.float32)
    for file_idx, electrode in enumerate(electrode_order, start=1):
        zero_based = int(electrode) - 1
        row = zero_based // 16
        col = zero_based % 16
        grid[row, col] = file_idx

    image = axes[1].imshow(grid, origin="lower", cmap="viridis")
    axes[1].set_title("Which file-order channel lands at each electrode position")
    axes[1].set_xlabel("Approximate electrode-layout column")
    axes[1].set_ylabel("Approximate electrode-layout row")
    fig.colorbar(image, ax=axes[1], label="File-order channel index")

    if args.save_prefix:
        out_path = Path(args.save_prefix).with_suffix("")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(str(out_path) + "_mapping.png", dpi=150)
        print(f"wrote: {out_path}_mapping.png")
    else:
        plt.show()


if __name__ == "__main__":
    main()
