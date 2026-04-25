#!/usr/bin/env python3
"""
Pick N consecutive sessions starting from a given session, choose one block
per session based on full-recording duration, write a multi-session
chosen-block manifest, and submit an N-task SLURM array.

Selection rule per session:
  - choose the shortest block longer than a minimum duration threshold
  - if no block exceeds that threshold, choose the longest block

Each array task runs:
  - session_featurize_to_mat.py on exactly one selected block
  - ../spike_plot_pipeline/plot_chunk_mats.py on the resulting .mat

Example:
  python3 submit_selected_session_blocks.py \
    --bucket exp_sessions_nearline \
    --subject t12 \
    --start-session t12.2025.11.04 \
    --root-data /path/to/repo/other \
    --root-derived /path/to/derived \
    --min-duration-s 300 \
    --submit
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
import re
import struct
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Tuple


SESSION_RE_TEMPLATE = r"^{subject}\.\d{{4}}\.\d{{2}}\.\d{{2}}$"
BLOCK_RE = re.compile(r"\((\d+\.?\d*)\)")
NSX_SAMPLE_RESOLUTION = 30000.0
NSX_BASIC_HEADER_FMT = "<2BI16s256sII8HI"
NSX_BASIC_HEADER_SIZE = struct.calcsize(NSX_BASIC_HEADER_FMT)
THIS_DIR = Path(__file__).resolve().parent
PIPELINE_ROOT = THIS_DIR.parent / "other"
DEFAULT_MANIFEST_OUT = THIS_DIR / "selected_session_blocks_manifest.json"
DEFAULT_SCRIPT_PATH = THIS_DIR / "run_selected_session_blocks.sbatch"


def run(cmd: List[str]) -> str:
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    if p.returncode != 0:
        raise RuntimeError(
            "Command failed:\n  %s\nSTDERR:\n%s" % (" ".join(cmd), p.stderr)
        )
    return p.stdout


def gsutil_ls(gsutil: str, url_glob: str) -> List[str]:
    out = run([gsutil, "ls", url_glob])
    return [ln.strip() for ln in out.splitlines() if ln.strip()]


def gsutil_cat_range(gsutil: str, url: str, start: int, end: int) -> bytes:
    if end < start:
        return b""
    cmd = [gsutil, "cat", "-r", f"{int(start)}-{int(end)}", url]
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if p.returncode != 0:
        raise RuntimeError(
            "Command failed:\n  %s\nSTDERR:\n%s"
            % (" ".join(cmd), p.stderr.decode("utf-8", errors="replace"))
        )
    return p.stdout


def gsutil_size_bytes(gsutil: str, url: str) -> int:
    out = run([gsutil, "ls", "-l", url])
    for ln in out.splitlines():
        parts = ln.strip().split()
        if len(parts) >= 2 and parts[-1] == url and parts[0].isdigit():
            return int(parts[0])
    raise RuntimeError(f"Could not parse size from `gsutil ls -l` output for {url!r}")


def parse_nsx_header_prefix(blob: bytes) -> Dict[str, object]:
    if len(blob) < 8:
        raise ValueError("NSx header prefix too short")
    file_type_id = blob[:8].decode("latin-1")
    if file_type_id == "NEURALSG":
        if len(blob) < 8 + 24:
            raise ValueError("NEURALSG header prefix too short")
        _, period, channel_count = struct.unpack("<16sII", blob[8:8 + 24])
        return {
            "file_type_id": file_type_id,
            "file_spec": "2.1",
            "bytes_in_header": 32 + 4 * int(channel_count),
            "period": int(period),
            "channel_count": int(channel_count),
        }

    if len(blob) < 8 + NSX_BASIC_HEADER_SIZE:
        raise ValueError("NSx basic header prefix too short")
    vals = struct.unpack(NSX_BASIC_HEADER_FMT, blob[8:8 + NSX_BASIC_HEADER_SIZE])
    return {
        "file_type_id": file_type_id,
        "file_spec": f"{vals[0]}.{vals[1]}",
        "bytes_in_header": int(vals[2]),
        "period": int(vals[5]),
        "channel_count": int(vals[-1]),
    }


def estimate_ns5_duration_seconds(gsutil: str, url: str) -> float:
    prefix = gsutil_cat_range(gsutil, url, 0, 4095)
    header = parse_nsx_header_prefix(prefix)
    bytes_in_header = int(header["bytes_in_header"])
    period = int(header["period"])
    channel_count = int(header["channel_count"])
    sample_rate = NSX_SAMPLE_RESOLUTION / float(period)
    size_bytes = gsutil_size_bytes(gsutil, url)
    remaining = size_bytes - bytes_in_header
    if remaining <= 0:
        raise RuntimeError(f"NS5 file is shorter than its header: {url}")

    if str(header["file_type_id"]) == "NEURALSG":
        n_samples = remaining // (channel_count * 2)
        return float(n_samples) / sample_rate

    packet_hdr = gsutil_cat_range(gsutil, url, bytes_in_header, bytes_in_header + 12)
    if len(packet_hdr) < 13:
        raise RuntimeError(f"Could not read first NS5 packet header from {url}")

    num_data_points = struct.unpack("<I", packet_hdr[9:13])[0]
    if num_data_points == 1:
        packet_size = 1 + 8 + 4 + (channel_count * 2)
        n_samples = remaining / float(packet_size)
        return n_samples / sample_rate

    segment_size = 1 + 8 + 4 + (int(num_data_points) * channel_count * 2)
    n_segments = remaining / float(segment_size)
    return (n_segments * float(num_data_points)) / sample_rate


def list_sessions(gsutil: str, bucket: str, subject: str) -> List[str]:
    url = f"gs://{bucket}/{subject}/"
    lines = gsutil_ls(gsutil, url)
    session_re = re.compile(SESSION_RE_TEMPLATE.format(subject=re.escape(subject)))

    sessions = []
    for ln in lines:
        name = ln.rstrip("/").split("/")[-1]
        if session_re.match(name):
            sessions.append(name)

    sessions = sorted(
        set(sessions),
        key=lambda s: tuple(map(int, s.split(".")[1:])),
    )
    return sessions


def choose_consecutive_sessions(sessions_sorted: List[str], start_session: str, n: int) -> List[str]:
    if start_session not in sessions_sorted:
        raise ValueError(f"Start session {start_session} not found.")
    i0 = sessions_sorted.index(start_session)
    chosen = sessions_sorted[i0:i0 + n]
    if len(chosen) < n:
        raise ValueError(
            f"Only found {len(chosen)} consecutive sessions starting at {start_session}; need {n}."
        )
    return chosen


def numeric_block_sort_key(x: str) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0


def list_blocks_with_urls(
    gsutil: str,
    bucket: str,
    subject: str,
    session: str,
    preferred_prefixes: Optional[Tuple[str, ...]] = None,
) -> Dict[str, str]:
    prefix = f"gs://{bucket}/{subject}/{session}/Data/_NSP1"
    urls = gsutil_ls(gsutil, prefix.rstrip("/") + "/**/*.ns5")
    preferred_prefixes = preferred_prefixes or ("Hub1", "Hub2", "NSP")
    ranked: Dict[str, Tuple[int, str]] = {}

    for u in urls:
        m = BLOCK_RE.search(u)
        if not m:
            continue
        block_id = m.group(1)
        fname = u.rstrip("/").split("/")[-1]
        rank = len(preferred_prefixes)
        for i, prefix_name in enumerate(preferred_prefixes):
            if fname.startswith(prefix_name):
                rank = i
                break
        prev = ranked.get(block_id)
        if prev is None or rank < prev[0]:
            ranked[block_id] = (rank, u)

    return {block_id: url for block_id, (_, url) in ranked.items()}


def choose_block_by_duration(block_durations_s: Dict[str, float], min_duration_s: float) -> Tuple[str, str]:
    eligible = [(block_id, dur) for block_id, dur in block_durations_s.items() if dur > min_duration_s]
    if eligible:
        chosen_block, _ = min(eligible, key=lambda item: (item[1], numeric_block_sort_key(item[0])))
        return chosen_block, "shortest_above_threshold"

    chosen_block, _ = max(
        block_durations_s.items(),
        key=lambda item: (item[1], -numeric_block_sort_key(item[0])),
    )
    return chosen_block, "longest_fallback"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", default="exp_sessions_nearline")
    ap.add_argument("--subject", required=True)
    ap.add_argument("--start-session", required=True)
    ap.add_argument(
        "--n-sessions",
        type=int,
        default=10,
        help="Number of consecutive sessions to include starting at --start-session.",
    )
    ap.add_argument("--gsutil", default=str(Path("~/google-cloud-sdk/bin/gsutil").expanduser()))

    ap.add_argument("--root-data", required=True)
    ap.add_argument("--root-derived", required=True)
    ap.add_argument("--repo-dir", default=str(PIPELINE_ROOT))
    ap.add_argument("--script-path", default=str(DEFAULT_SCRIPT_PATH))

    ap.add_argument("--min-duration-s", type=float, default=300.0)
    ap.add_argument("--align-by-timestamp", action="store_true", default=True)

    ap.add_argument("--manifest-out", default=str(DEFAULT_MANIFEST_OUT))

    ap.add_argument("--partition", default="normal")
    ap.add_argument("--time", default="12:00:00")
    ap.add_argument("--mem", default="32G")
    ap.add_argument("--cpus", type=int, default=4)
    ap.add_argument(
        "--duration-workers",
        type=int,
        default=1,
        help="Number of parallel workers to use when estimating per-block NS5 durations within a session.",
    )

    ap.add_argument("--submit", action="store_true")
    args = ap.parse_args()

    if args.n_sessions <= 0:
        raise SystemExit("--n-sessions must be a positive integer.")
    if args.duration_workers <= 0:
        raise SystemExit("--duration-workers must be a positive integer.")

    if not os.path.exists(args.gsutil):
        raise SystemExit(f"gsutil not found at: {args.gsutil}")

    sessions_all = list_sessions(args.gsutil, args.bucket, args.subject)
    chosen_sessions = choose_consecutive_sessions(
        sessions_all, args.start_session, args.n_sessions
    )

    entries = []
    for sess in chosen_sessions:
        block_urls = list_blocks_with_urls(args.gsutil, args.bucket, args.subject, sess)
        if not block_urls:
            raise RuntimeError(f"No block NS5 URLs found for session {sess}.")

        block_ids = sorted(block_urls.keys(), key=numeric_block_sort_key)
        if args.duration_workers == 1 or len(block_ids) == 1:
            block_durations_s = {
                block_id: estimate_ns5_duration_seconds(args.gsutil, block_urls[block_id])
                for block_id in block_ids
            }
        else:
            with ThreadPoolExecutor(max_workers=int(args.duration_workers)) as ex:
                durations = ex.map(
                    lambda block_id: (
                        block_id,
                        estimate_ns5_duration_seconds(args.gsutil, block_urls[block_id]),
                    ),
                    block_ids,
                )
                block_durations_s = dict(durations)
        chosen_block, chosen_reason = choose_block_by_duration(
            block_durations_s,
            float(args.min_duration_s),
        )
        entries.append(
            {
                "session": sess,
                "blocks_all": block_ids,
                "chosen_block": chosen_block,
                "chosen_duration_s": float(block_durations_s[chosen_block]),
                "selection_rule": chosen_reason,
                "block_durations_s": {
                    block_id: float(block_durations_s[block_id]) for block_id in block_ids
                },
            }
        )

    manifest = {
        "bucket": args.bucket,
        "subject": args.subject,
        "start_session": args.start_session,
        "n_sessions": args.n_sessions,
        "root_data": os.path.abspath(args.root_data),
        "root_derived": os.path.abspath(args.root_derived),
        "repo_dir": os.path.abspath(args.repo_dir),
        "script_path": os.path.abspath(args.script_path),
        "gsutil": os.path.abspath(args.gsutil),
        "min_duration_s": float(args.min_duration_s),
        "align_by_timestamp": bool(args.align_by_timestamp),
        "slurm": {
            "partition": args.partition,
            "time": args.time,
            "mem": args.mem,
            "cpus": int(args.cpus),
        },
        "entries": entries,
    }

    manifest_path = os.path.abspath(args.manifest_out)
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    print(f"Wrote manifest: {manifest_path}")
    for i, e in enumerate(entries):
        print(
            f"[{i}] {e['session']} -> block {e['chosen_block']} "
            f"dur_s={e['chosen_duration_s']:.3f} rule={e['selection_rule']}"
        )

    if args.submit:
        array_spec = f"0-{len(entries)-1}"
        cmd = [
            "sbatch",
            f"--array={array_spec}",
            f"--partition={args.partition}",
            f"--time={args.time}",
            f"--mem={args.mem}",
            f"--cpus-per-task={args.cpus}",
            args.script_path,
            manifest_path,
        ]
        out = run(cmd)
        print(out)


if __name__ == "__main__":
    main()
