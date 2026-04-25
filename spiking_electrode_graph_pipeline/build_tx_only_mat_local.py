#!/usr/bin/env python3
import argparse
import os
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

import numpy as np

THIS_DIR = Path(__file__).resolve().parent
OTHER_DIR = THIS_DIR.parent / "other"
import sys
sys.path.insert(0, os.fspath(OTHER_DIR))

from ns5_featurizer import (
    apply_lrr_approx,
    bandpass_filter,
    compute_binned_tx,
    decimate_data,
    load_ns5_data,
)
from session_featurize_to_mat import (
    default_gemini_sets,
    ensure_dir,
    find_block_ns5_urls,
    gsutil_cp,
    save_mat_any,
    unscramble_channels,
)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Build a minimal tx-only .mat for one selected block using chunked local NS5 processing."
    )
    ap.add_argument("--gsutil", type=str, default=os.path.expanduser("~/google-cloud-sdk/bin/gsutil"))
    ap.add_argument("--bucket", type=str, default="exp_sessions_nearline")
    ap.add_argument("--subject", type=str, required=True)
    ap.add_argument("--session", type=str, required=True)
    ap.add_argument("--block", type=str, required=True)
    ap.add_argument("--root-data", type=Path, required=True)
    ap.add_argument("--root-derived", type=Path, required=True)
    ap.add_argument("--local-ns5-subdir", type=str, default=os.path.join("Data", "NSP_Files"))
    ap.add_argument("--hub-prefixes", type=str, default="Hub1,Hub2")
    ap.add_argument("--bin-ms", type=float, default=20.0)
    ap.add_argument("--tx-thresh", type=float, default=-4.5)
    ap.add_argument("--voltage-scale", type=float, default=4.0)
    ap.add_argument("--chunk-sec", type=float, default=5.0)
    ap.add_argument("--download", action="store_true")
    return ap.parse_args()


def local_ns5_path(local_ns5_dir: Path, url: str) -> Path:
    return local_ns5_dir / url.rstrip("/").split("/")[-1]


def resolve_ns5_paths(args: argparse.Namespace, hub_prefixes: Sequence[str]) -> List[Tuple[str, Path]]:
    session_gs = f"gs://{args.bucket}/{args.subject}/{args.session}"
    urls_by_hub = find_block_ns5_urls(args.gsutil, session_gs, args.block, hub_prefixes, dry_run=False)
    if not urls_by_hub:
        raise RuntimeError(f"No NS5 URLs found for session={args.session} block={args.block}")

    session_dir = args.root_data / args.session
    local_ns5_dir = session_dir / args.local_ns5_subdir
    ensure_dir(os.fspath(local_ns5_dir))

    out: List[Tuple[str, Path]] = []
    for hub in hub_prefixes:
        if hub not in urls_by_hub:
            continue
        src_url = urls_by_hub[hub]
        dst_path = local_ns5_path(local_ns5_dir, src_url)
        if args.download and not dst_path.exists():
            print(f"downloading {hub}: {src_url} -> {dst_path}")
            gsutil_cp(args.gsutil, src_url, os.fspath(dst_path), parallel=False, dry_run=False)
        if not dst_path.exists():
            raise FileNotFoundError(f"Missing local NS5 for hub={hub}: {dst_path}")
        out.append((hub, dst_path))

    if not out:
        raise RuntimeError(f"No local NS5 paths resolved for session={args.session} block={args.block}")
    return out


def extract_tx_only_chunk(
    ns5_path: Path,
    start_time_s: float,
    data_time_s: float,
    bin_ms: float,
    tx_thresh: float,
    voltage_scale: float,
) -> np.ndarray:
    data, raw_sr, _ = load_ns5_data(
        os.fspath(ns5_path),
        start_time_s=start_time_s,
        data_time_s=data_time_s,
    )
    if data.size == 0:
        return np.empty((0, 0), dtype=np.float32)

    spike_data = data.astype(np.float32) * float(voltage_scale)
    if int(raw_sr) == 30000:
        spike_data = decimate_data(spike_data, 2)
        raw_sr = 15000.0

    spike_data = bandpass_filter(spike_data, raw_sr, 250.0, 4900.0)
    spike_data, _ = apply_lrr_approx(spike_data, default_gemini_sets(spike_data.shape[1]), ridge=0.0)
    spike_bin_samp = int(round((float(bin_ms) / 1000.0) * float(raw_sr)))
    tx = compute_binned_tx(spike_data, [float(tx_thresh)], spike_bin_samp)[0]
    return unscramble_channels(np.asarray(tx, dtype=np.float32))


def build_hub_tx(
    ns5_path: Path,
    chunk_sec: float,
    bin_ms: float,
    tx_thresh: float,
    voltage_scale: float,
) -> np.ndarray:
    chunks: List[np.ndarray] = []
    start_s = 0.0
    while True:
        tx = extract_tx_only_chunk(
            ns5_path=ns5_path,
            start_time_s=start_s,
            data_time_s=chunk_sec,
            bin_ms=bin_ms,
            tx_thresh=tx_thresh,
            voltage_scale=voltage_scale,
        )
        if tx.size == 0:
            break
        chunks.append(tx)
        print(
            f"{ns5_path.name}: start={start_s:.1f}s chunk_bins={tx.shape[0]} channels={tx.shape[1]}"
        )
        if tx.shape[0] < int(round(chunk_sec * 1000.0 / bin_ms)):
            break
        start_s += chunk_sec

    if not chunks:
        return np.empty((0, 0), dtype=np.float32)
    return np.vstack(chunks).astype(np.float32, copy=False)


def pad_to_len(arr: np.ndarray, desired_len: int) -> np.ndarray:
    out = np.full((desired_len, arr.shape[1]), np.nan, dtype=np.float32)
    out[: arr.shape[0]] = arr
    return out


def main() -> None:
    args = parse_args()
    hub_prefixes = [s.strip() for s in args.hub_prefixes.split(",") if s.strip()]
    ns5_paths_by_hub = resolve_ns5_paths(args, hub_prefixes)

    hub_txs: List[Tuple[str, np.ndarray, Path]] = []
    for hub, ns5_path in ns5_paths_by_hub:
        tx = build_hub_tx(
            ns5_path=ns5_path,
            chunk_sec=float(args.chunk_sec),
            bin_ms=float(args.bin_ms),
            tx_thresh=float(args.tx_thresh),
            voltage_scale=float(args.voltage_scale),
        )
        if tx.size == 0:
            raise RuntimeError(f"No TX data extracted for {ns5_path}")
        hub_txs.append((hub, tx, ns5_path))

    desired_len = max(tx.shape[0] for _, tx, _ in hub_txs)
    aligned = [pad_to_len(tx, desired_len) for _, tx, _ in hub_txs]
    tx_all = np.hstack(aligned).astype(np.float32, copy=False)

    out_dir = args.root_derived / args.session / "ns5_block_features"
    ensure_dir(os.fspath(out_dir))
    out_path = out_dir / f"{args.block}.mat"
    save_mat_any(
        os.fspath(out_path),
        {
            "tx_from_ns5_45": tx_all,
            "ns5_featurizer_bin_ms": np.array([float(args.bin_ms)], dtype=np.float32),
            "session_name": np.array([args.session], dtype=object),
            "block_number": np.array([str(args.block)], dtype=object),
            "ns5_source_hubs": np.array([hub for hub, _, _ in hub_txs], dtype=object),
            "ns5_source_paths": np.array([os.fspath(path) for _, _, path in hub_txs], dtype=object),
            "ns5_chunk_mode": np.array(["full_block_chunked_tx_only"], dtype=object),
        },
    )
    print(f"saved: {out_path}")


if __name__ == "__main__":
    main()
