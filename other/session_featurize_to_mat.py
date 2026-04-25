#!/usr/bin/env python3
"""
Session-level pipeline to:
  1) Download per-block .ns5 files from GCS (via gsutil)
  2) Featurize each .ns5 (spike-band power, TX rates, LFP envelopes, LMP)
  3) Save standalone NS5-derived MAT outputs suitable for plotting

Optional:
  - Partial-chunk featurization per block via:
      --chunk-start-s
      --chunk-duration-s
    When enabled, only that chunk is featurized and written to the output MAT.
"""

import argparse
import os
import re
import subprocess
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import numpy as np
from scipy import io as spio
from scipy import signal

from ns5_featurizer import extract_ns5_features, load_ns5_data


BLOCK_RE = re.compile(r"\((\d+\.?\d*)\)")
_NEG_CSV_NUMBER_RE = re.compile(r"^-?\d+(\.\d+)?(,-?\d+(\.\d+)?)*$")


def run_cmd(cmd: List[str], dry_run: bool = False) -> None:
    if dry_run:
        print("[dry-run] " + " ".join(cmd))
        return
    subprocess.check_call(cmd)


def gsutil_ls(gsutil: str, url_glob: str, dry_run: bool = False) -> List[str]:
    cmd = [gsutil, "ls", url_glob]
    if dry_run:
        print("[dry-run] " + " ".join(cmd))
        return []
    out = subprocess.check_output(cmd, universal_newlines=True)
    return [line.strip() for line in out.splitlines() if line.strip()]


def gsutil_cp(
    gsutil: str,
    src: str,
    dst: str,
    recursive: bool = False,
    parallel: bool = True,
    dry_run: bool = False,
    allow_missing: bool = False,
) -> None:
    cmd = [gsutil]
    if parallel:
        cmd.append("-m")
    cmd.extend(["cp"])
    if recursive:
        cmd.append("-r")
    cmd.extend([src, dst])
    if dry_run:
        run_cmd(cmd, dry_run=True)
        return

    if allow_missing:
        p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        if p.returncode != 0:
            err = (p.stderr or "") + (p.stdout or "")
            if "No URLs matched" in err:
                print("warning: missing in GCS, skipping:", src)
                return
            raise subprocess.CalledProcessError(p.returncode, cmd, output=p.stdout, stderr=p.stderr)
        return

    subprocess.check_call(cmd)


def parse_block_id_from_path(path: str) -> Optional[str]:
    m = BLOCK_RE.search(path)
    if not m:
        return None
    return m.group(1)


def list_blocks(gsutil: str, session_gs_prefix: str, dry_run: bool = False) -> List[str]:
    ns5_urls = gsutil_ls(gsutil, session_gs_prefix.rstrip("/") + "/**/*.ns5", dry_run=dry_run)
    ids = []
    for u in ns5_urls:
        bid = parse_block_id_from_path(u)
        if bid is not None:
            ids.append(bid)

    def _key(x: str) -> float:
        try:
            return float(x)
        except Exception:
            return 0.0

    return sorted(sorted(set(ids)), key=_key)


def find_block_ns5_urls(
    gsutil: str,
    session_gs_prefix: str,
    block_id: str,
    hub_prefixes: Sequence[str],
    dry_run: bool = False,
) -> Dict[str, str]:
    urls = gsutil_ls(gsutil, session_gs_prefix.rstrip("/") + "/Data/**/*.ns5", dry_run=dry_run)
    wanted = {}
    for u in urls:
        if ("(" + block_id + ")") not in u:
            continue
        fname = u.rstrip("/").split("/")[-1]
        for hub in hub_prefixes:
            if fname.startswith(hub):
                wanted[hub] = u
    return wanted


def find_block_ns5_urls_for_prefixes(
    gsutil: str,
    session_gs_prefix: str,
    block_id: str,
    prefixes: Sequence[str],
    dry_run: bool = False,
) -> Dict[str, str]:
    return find_block_ns5_urls(gsutil, session_gs_prefix, block_id, prefixes, dry_run=dry_run)


def ensure_dir(path: str) -> None:
    if not os.path.exists(path):
        os.makedirs(path)


def default_gemini_sets(n_ch: int) -> List[List[int]]:
    """
    MATLAB uses: {[1:32,97:128], 33:96, 128+[1:32,97:128], 128+(33:96)} (1-indexed).
    """
    sets = []

    def a(start_1: int, end_1: int) -> List[int]:
        return list(range(start_1 - 1, end_1))

    left = a(1, 32) + a(97, 128)
    mid = a(33, 96)
    if n_ch >= 128:
        sets.append(left)
        sets.append(mid)
    if n_ch >= 256:
        sets.append([x + 128 for x in left])
        sets.append([x + 128 for x in mid])
    return sets if sets else [list(range(n_ch))]


_CHAN_TO_ELEC_1IDX = np.array(
    [
        63, 64, 62, 61, 59, 58, 60, 54, 57, 50, 53, 49, 52, 45, 55, 44, 56, 39, 51, 43,
        46, 38, 48, 37, 47, 36, 42, 35, 41, 34, 40, 33, 96, 90, 95, 89, 94, 88, 93, 87,
        92, 82, 86, 81, 91, 77, 85, 83, 84, 78, 80, 73, 79, 74, 75, 76, 71, 72, 68, 69,
        66, 70, 65, 67, 128, 120, 127, 119, 126, 118, 125, 117, 124, 116, 123, 115, 122, 114, 121, 113,
        112, 111, 109, 110, 107, 108, 106, 105, 104, 103, 102, 101, 100, 99, 97, 98, 32, 30, 31, 29,
        28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 16, 17, 7, 15, 6, 14, 5, 13, 4, 12, 3, 11, 2, 10, 1, 9, 8,
    ],
    dtype=np.int32,
)


def unscramble_128(x: np.ndarray) -> np.ndarray:
    if x.shape[1] != 128:
        return x
    out = np.zeros_like(x)
    dest0 = _CHAN_TO_ELEC_1IDX - 1
    out[:, dest0] = x
    return out


def unscramble_channels(x: np.ndarray) -> np.ndarray:
    if x.ndim != 2:
        return x
    c = x.shape[1]
    if c == 128:
        return unscramble_128(x)
    if c == 256:
        return np.concatenate([unscramble_128(x[:, :128]), unscramble_128(x[:, 128:256])], axis=1)
    return x


def estimate_best_lag(
    ref: np.ndarray,
    sig: np.ndarray,
) -> int:
    """
    Estimate the lag (in bins) between ref and sig using median lag across channels.
    """
    t = min(ref.shape[0], sig.shape[0])
    if t <= 2:
        return 0
    ref = ref[:t]
    sig = sig[:t]

    lags = []
    for ch in range(ref.shape[1]):
        a = ref[:, ch].astype(np.float64)
        b = sig[:, ch].astype(np.float64)
        a -= a.mean()
        b -= b.mean()
        c = signal.correlate(a, b, mode="full", method="fft")
        lag_vec = np.arange(-t + 1, t)
        best = int(lag_vec[int(np.argmax(c))])
        lags.append(best)

    return int(np.median(np.asarray(lags, dtype=np.int32)))


def build_alignment_trace(feat: np.ndarray, max_chans: int = 64) -> np.ndarray:
    """
    Build a 1D hub-level activity trace for xcorr alignment.

    Different hubs do not share channel identities, so align on an aggregate,
    per-channel-normalized SBP trace rather than channel-wise correspondence.
    """
    arr = np.asarray(feat, dtype=np.float32)
    if arr.ndim != 2 or arr.size == 0:
        return np.zeros((0,), dtype=np.float32)

    x = arr.astype(np.float64, copy=False)
    mu = np.nanmean(x, axis=0)
    sigma = np.nanstd(x, axis=0)
    valid = np.isfinite(mu) & np.isfinite(sigma) & (sigma > 1e-6)

    if np.any(valid):
        x = (x[:, valid] - mu[valid]) / sigma[valid]
    else:
        x = x - np.nanmean(x, axis=0, keepdims=True)

    if x.ndim != 2 or x.shape[1] == 0:
        return np.zeros((arr.shape[0],), dtype=np.float32)

    if x.shape[1] > max_chans:
        idx = np.linspace(0, x.shape[1] - 1, num=max_chans, dtype=int)
        x = x[:, idx]

    trace = np.nanmean(x, axis=1)
    trace = np.nan_to_num(trace, nan=0.0, posinf=0.0, neginf=0.0)
    return trace.astype(np.float32, copy=False)


def estimate_best_lag_1d(ref_trace: np.ndarray, sig_trace: np.ndarray) -> int:
    """
    Estimate the lag between two 1D traces.

    Positive return value means `sig` leads `ref`; negative means `sig` lags
    `ref`. To place `sig` on the reference timeline, use `start_bin = -lag`.
    """
    ref = np.asarray(ref_trace, dtype=np.float64).reshape(-1)
    sig = np.asarray(sig_trace, dtype=np.float64).reshape(-1)

    t = min(ref.size, sig.size)
    if t <= 2:
        return 0

    ref = ref[:t]
    sig = sig[:t]
    ref -= ref.mean()
    sig -= sig.mean()

    ref_energy = float(np.dot(ref, ref))
    sig_energy = float(np.dot(sig, sig))
    if ref_energy <= 0.0 or sig_energy <= 0.0:
        return 0

    c = signal.correlate(ref, sig, mode="full", method="fft")
    lag_vec = np.arange(-t + 1, t)
    return int(lag_vec[int(np.argmax(c))])


def _flatten_1d(x: Any) -> np.ndarray:
    a = np.asarray(x)
    return a.reshape(-1)


def align_by_timestamp(
    src_feat: np.ndarray,
    src_ts0_ns: int,
    bin_ms: float,
    dst_ts_ns: np.ndarray,
    desired_len: int,
    fill_value: float = 0.0,
) -> Tuple[np.ndarray, int]:
    """
    Align binned features to RedisMat bins using timestamps.

    For each source bin i, compute a source timestamp:
      ts_src[i] = ts0 + i * bin_ms * 1e6   (nanoseconds)
    Then place it into the closest destination bin from `dst_ts_ns`.

    Returns:
      aligned_feat: [desired_len x C]
      est_lag_bins: median(dst_idx - src_idx) over bins within ~1 bin duration
    """
    src_feat = np.asarray(src_feat)
    if src_feat.ndim != 2:
        raise ValueError("src_feat must be 2D [bins x chans]")
    if desired_len <= 0:
        return np.full((0, src_feat.shape[1]), fill_value, dtype=src_feat.dtype), 0

    dst_ts = _flatten_1d(dst_ts_ns).astype(np.int64, copy=False)
    if dst_ts.size != desired_len:
        desired_len = int(min(desired_len, dst_ts.size))
        dst_ts = dst_ts[:desired_len]

    step_ns = int(round(float(bin_ms) * 1e6))
    n_src = int(src_feat.shape[0])
    ts_src = np.asarray(src_ts0_ns, dtype=np.int64) + (
        np.arange(n_src, dtype=np.int64) * step_ns
    )

    if dst_ts.size == 0:
        return np.full((desired_len, src_feat.shape[1]), fill_value, dtype=src_feat.dtype), 0

    sort_idx = np.argsort(dst_ts)
    dst_sorted = dst_ts[sort_idx]

    pos = np.searchsorted(dst_sorted, ts_src, side="left")
    pos0 = np.clip(pos - 1, 0, dst_sorted.size - 1)
    pos1 = np.clip(pos, 0, dst_sorted.size - 1)
    d0 = np.abs(dst_sorted[pos0] - ts_src)
    d1 = np.abs(dst_sorted[pos1] - ts_src)
    choose_pos = np.where(d1 < d0, pos1, pos0)
    dst_idx = sort_idx[choose_pos].astype(np.int64)
    err = np.minimum(d0, d1).astype(np.int64)

    best_src = np.full(desired_len, -1, dtype=np.int64)
    best_err = np.full(desired_len, np.iinfo(np.int64).max, dtype=np.int64)
    for i in range(n_src):
        j = int(dst_idx[i])
        e = int(err[i])
        if e < best_err[j]:
            best_err[j] = e
            best_src[j] = i

    aligned = np.full((desired_len, src_feat.shape[1]), fill_value, dtype=src_feat.dtype)
    valid = best_src >= 0
    if np.any(valid):
        aligned[valid] = src_feat[best_src[valid]]

    good = valid & (best_err <= step_ns)
    est_lag = 0
    if np.any(good):
        est_lag = int(np.median(np.flatnonzero(good) - best_src[good]))
    return aligned, est_lag


def compute_binned_rms_envelope_from_ns5(
    ns5_path: str,
    bin_ms: float,
    detrend_type: str = "linear",
    start_time_s: float = 0.0,
    data_time_s: Union[str, float] = "all",
    initial_decimate_to_hz: Optional[float] = None,
) -> Tuple[np.ndarray, float, Optional[int]]:
    """
    Load an NS5 (optionally only a chunk) and compute a binned RMS envelope per channel:
      env_bin[t,c] = sqrt(mean( detrend(x)^2 )) over samples in the bin.
    """
    x, raw_sr, ts0 = load_ns5_data(
        ns5_path,
        start_time_s=start_time_s,
        data_time_s=data_time_s,
        initial_decimate_to_hz=initial_decimate_to_hz,
    )
    if x.size == 0:
        return np.empty((0, 0), dtype=np.float32), raw_sr, ts0

    x = x.astype(np.float32)
    x = signal.detrend(x, axis=0, type=str(detrend_type)).astype(np.float32, copy=False)

    n_samp = int(round((float(bin_ms) / 1000.0) * float(raw_sr)))
    if n_samp <= 0:
        raise ValueError("bin_ms too small for raw_sr")
    n_bins = x.shape[0] // n_samp
    if n_bins <= 0:
        return np.empty((0, x.shape[1]), dtype=np.float32), raw_sr, ts0

    use_len = n_bins * n_samp
    x2 = (x[:use_len] ** 2).reshape(n_bins, n_samp, x.shape[1]).mean(axis=1)
    env = np.sqrt(x2).astype(np.float32)
    return env, raw_sr, ts0


def _align_block_features_xcorr(
    ref_sbp: np.ndarray,
    sbp: np.ndarray,
    tx_list: Sequence[np.ndarray],
    lfp_env: Dict[str, np.ndarray],
    lmp: np.ndarray,
    desired_len: int,
) -> Tuple[np.ndarray, List[np.ndarray], Dict[str, np.ndarray], int]:
    best_lag = estimate_best_lag(ref_sbp, sbp)
    aligned_sbp = time_shift_features(sbp, best_lag, desired_len)
    aligned_tx = [time_shift_features(t, best_lag, desired_len) for t in tx_list]

    aligned_lfp = {}
    for name in ["delta", "theta", "beta", "gamma"]:
        if name in lfp_env:
            aligned_lfp[name] = time_shift_features(lfp_env[name], best_lag, desired_len)
    aligned_lfp["lmp"] = time_shift_features(lmp, best_lag, desired_len)
    return aligned_sbp, aligned_tx, aligned_lfp, best_lag


def _align_block_features_timestamp(
    sbp: np.ndarray,
    tx_list: Sequence[np.ndarray],
    lfp_env: Dict[str, np.ndarray],
    lmp: np.ndarray,
    desired_len: int,
    bin_ms: float,
    ns5_ts0: int,
    redis_ts: np.ndarray,
    ref_sbp_for_xcorr: Optional[np.ndarray] = None,
    fill_value: float = 0.0,
) -> Tuple[np.ndarray, List[np.ndarray], Dict[str, np.ndarray], int, Optional[int]]:
    aligned_sbp, lag_ts = align_by_timestamp(
        sbp, ns5_ts0, bin_ms, redis_ts, desired_len, fill_value=fill_value
    )

    aligned_tx = [
        align_by_timestamp(t, ns5_ts0, bin_ms, redis_ts, desired_len, fill_value=fill_value)[0]
        for t in tx_list
    ]

    aligned_lfp = {}
    for name in ["delta", "theta", "beta", "gamma"]:
        if name in lfp_env:
            aligned_lfp[name] = align_by_timestamp(
                lfp_env[name], ns5_ts0, bin_ms, redis_ts, desired_len, fill_value=fill_value
            )[0]
    aligned_lfp["lmp"] = align_by_timestamp(
        lmp, ns5_ts0, bin_ms, redis_ts, desired_len, fill_value=fill_value
    )[0]

    lag_xcorr = None
    if ref_sbp_for_xcorr is not None:
        lag_xcorr = estimate_best_lag(ref_sbp_for_xcorr, sbp)
    return aligned_sbp, aligned_tx, aligned_lfp, lag_ts, lag_xcorr


def time_shift_features(feat: np.ndarray, best_lag: int, desired_len: int) -> np.ndarray:
    """
    MATLAB `timeShiftFeatures` port, generalized to arbitrary channel count.
    """
    n_ch = feat.shape[1]
    if best_lag < 0:
        start = -(best_lag - 1)
        shifted = feat[start:, :]
    elif best_lag > 0:
        shifted = np.vstack([np.zeros((best_lag + 1, n_ch), dtype=feat.dtype), feat])
    else:
        shifted = feat

    if shifted.shape[0] < desired_len:
        pad = np.zeros((desired_len - shifted.shape[0], n_ch), dtype=feat.dtype)
        shifted = np.vstack([shifted, pad])
    elif shifted.shape[0] > desired_len:
        shifted = shifted[:desired_len]
    return shifted


def place_chunk_into_full(
    feat: np.ndarray,
    desired_len: int,
    start_bin: int,
    fill_value: float = np.nan,
) -> np.ndarray:
    """
    Place a chunk feature matrix [chunk_bins x C] into a full-block matrix [desired_len x C].
    """
    feat = np.asarray(feat, dtype=np.float32)
    out = np.full((desired_len, feat.shape[1]), fill_value, dtype=np.float32)

    if feat.size == 0 or desired_len <= 0:
        return out

    src_start = max(0, -start_bin)
    dst_start = max(0, start_bin)
    n = min(feat.shape[0] - src_start, desired_len - dst_start)

    if n > 0:
        out[dst_start:dst_start + n] = feat[src_start:src_start + n]

    return out


def place_chunk_bundle_into_full(
    sbp: np.ndarray,
    tx_list: Sequence[np.ndarray],
    lfp_env: Dict[str, np.ndarray],
    lmp: np.ndarray,
    desired_len: int,
    start_bin: int,
    fill_value: float = np.nan,
) -> Tuple[np.ndarray, List[np.ndarray], Dict[str, np.ndarray]]:
    aligned_sbp = place_chunk_into_full(sbp, desired_len, start_bin, fill_value=fill_value)
    aligned_tx = [
        place_chunk_into_full(t, desired_len, start_bin, fill_value=fill_value)
        for t in tx_list
    ]

    aligned_lfp = {}
    for name in ["delta", "theta", "beta", "gamma"]:
        if name in lfp_env:
            aligned_lfp[name] = place_chunk_into_full(
                lfp_env[name], desired_len, start_bin, fill_value=fill_value
            )
    aligned_lfp["lmp"] = place_chunk_into_full(
        lmp, desired_len, start_bin, fill_value=fill_value
    )

    return aligned_sbp, aligned_tx, aligned_lfp


def load_mat_any(path: str) -> Dict[str, Any]:
    """
    Load a MAT file. Uses scipy for v5/v7.2; for v7.3 requires `hdf5storage`.
    """
    try:
        d = spio.loadmat(path, squeeze_me=False, struct_as_record=False)
        return {k: v for k, v in d.items() if not k.startswith("__")}
    except NotImplementedError:
        pass
    except ValueError:
        pass

    try:
        import hdf5storage  # type: ignore

        d = hdf5storage.loadmat(path)
        return {k: v for k, v in d.items() if not k.startswith("__")}
    except Exception as e:
        raise RuntimeError(
            "Failed to load .mat (likely v7.3). Install `hdf5storage` to read v7.3 mats."
        ) from e


def save_mat_any(path: str, data: Dict[str, Any]) -> None:
    """
    Save MAT file. Uses scipy (v5) by default; if `hdf5storage` is available, uses v7.3.
    """
    try:
        import hdf5storage  # type: ignore

        hdf5storage.savemat(path, data, format="7.3")
        return
    except Exception:
        pass
    spio.savemat(path, data, do_compression=True)


def pad_time_axis(arr: np.ndarray, desired_len: int, fill_value: float = np.nan) -> np.ndarray:
    arr = np.asarray(arr, dtype=np.float32)
    if arr.ndim != 2:
        raise ValueError(f"Expected 2D array to pad, got shape {arr.shape}")
    if desired_len < 0:
        raise ValueError("desired_len must be non-negative")
    if arr.shape[0] == desired_len:
        return arr

    out = np.full((desired_len, arr.shape[1]), fill_value, dtype=np.float32)
    use_len = min(desired_len, arr.shape[0])
    if use_len > 0:
        out[:use_len] = arr[:use_len]
    return out


def process_block(
    session_dir: str,
    derived_dir: str,
    block_id: str,
    ns5_paths_by_hub: List[Tuple[str, str]],
    bin_ms: float,
    tx_thresh: Sequence[float],
    voltage_scale: float,
    apply_lrr: bool,
    lrr_approx: bool,
    apply_car: bool,
    audio_ns5_path: Optional[str] = None,
    audio_detrend: str = "linear",
    attach_audio: bool = True,
    dry_run: bool = False,
    chunk_start_s: float = 0.0,
    chunk_duration_s: Optional[float] = None,
    initial_decimate_to_hz: Optional[float] = None,
) -> None:
    if dry_run:
        print("[dry-run] would process block %s from %d NS5 file(s)" % (block_id, len(ns5_paths_by_hub)))
        return

    chunk_mode = chunk_duration_s is not None
    fill_value = np.nan
    hub_features: List[Dict[str, Any]] = []
    total_ch = 0

    for hub, ns5_path in ns5_paths_by_hub:
        feats = extract_ns5_features(
            ns5_path=ns5_path,
            start_time_s=float(chunk_start_s) if chunk_mode else 0.0,
            data_time_s=float(chunk_duration_s) if chunk_mode else "all",
            initial_decimate_to_hz=initial_decimate_to_hz,
            bin_ms=bin_ms,
            tx_thresh=tx_thresh,
            voltage_scale=voltage_scale,
            apply_lrr_filter=apply_lrr,
            lrr_sets=default_gemini_sets(256),
            lrr_approx=lrr_approx,
            apply_car_filter=apply_car,
            car_sets=default_gemini_sets(256),
        )

        sbp = unscramble_channels(np.asarray(feats.spike_band_power, dtype=np.float32))
        tx_list = [unscramble_channels(np.asarray(t, dtype=np.float32)) for t in feats.binned_tx]
        lfp_env = {k: unscramble_channels(np.asarray(v, dtype=np.float32)) for k, v in feats.lfp_env.items()}
        lmp = unscramble_channels(np.asarray(feats.lmp, dtype=np.float32))

        c_feat = int(sbp.shape[1])
        total_ch += c_feat
        hub_n_bins = max(
            [int(sbp.shape[0]), int(lmp.shape[0])]
            + [int(t.shape[0]) for t in tx_list]
            + [int(v.shape[0]) for v in lfp_env.values()]
        )

        hub_features.append(
            {
                "hub": hub,
                "path": ns5_path,
                "sbp": sbp,
                "tx_list": tx_list,
                "lfp_env": lfp_env,
                "lmp": lmp,
                "hub_n_bins": hub_n_bins,
                "alignment_trace": build_alignment_trace(sbp),
            }
        )
        print("block %s hub %s: extracted %d bins before hub xcorr alignment" % (block_id, hub, int(sbp.shape[0])))

    if not hub_features:
        raise RuntimeError("No NS5-derived features were extracted for block %s" % block_id)

    ref_hub = str(hub_features[0]["hub"])
    ref_trace = np.asarray(hub_features[0]["alignment_trace"], dtype=np.float32)
    raw_start_bins: List[int] = []
    lag_bins: List[int] = []
    max_end = 0
    min_start = 0
    for idx, hub_entry in enumerate(hub_features):
        if idx == 0:
            best_lag = 0
        else:
            best_lag = estimate_best_lag_1d(ref_trace, hub_entry["alignment_trace"])
        start_bin = -int(best_lag)
        hub_entry["best_lag_bins"] = int(best_lag)
        hub_entry["start_bin"] = int(start_bin)
        lag_bins.append(int(best_lag))
        raw_start_bins.append(int(start_bin))
        min_start = min(min_start, int(start_bin))
        max_end = max(max_end, int(start_bin) + int(hub_entry["hub_n_bins"]))
        print(
            "block %s hub %s: align=xcorr ref_hub=%s bestLag=%d startBin=%d n_bins=%d"
            % (
                block_id,
                hub_entry["hub"],
                ref_hub,
                int(best_lag),
                int(start_bin),
                int(hub_entry["hub_n_bins"]),
            )
        )

    desired_len = max_end - min_start
    if desired_len <= 0:
        raise RuntimeError("No non-empty NS5-derived feature arrays for block %s" % block_id)

    aligned_sbp = np.full((desired_len, total_ch), fill_value, dtype=np.float32)
    aligned_tx = [np.full((desired_len, total_ch), fill_value, dtype=np.float32) for _ in tx_thresh]
    lfp_names = ["lmp", "delta", "theta", "beta", "gamma"]
    aligned_lfp = {
        name: np.full((desired_len, total_ch), fill_value, dtype=np.float32)
        for name in lfp_names
    }

    chan_cursor = 0
    for hub_entry in hub_features:
        c_feat = int(hub_entry["sbp"].shape[1])
        sl = slice(chan_cursor, chan_cursor + c_feat)
        out_start = int(hub_entry["start_bin"]) - min_start
        a_sbp, a_tx, a_lfp = place_chunk_bundle_into_full(
            hub_entry["sbp"],
            hub_entry["tx_list"],
            hub_entry["lfp_env"],
            hub_entry["lmp"],
            desired_len,
            out_start,
            fill_value=fill_value,
        )
        aligned_sbp[:, sl] = a_sbp
        for i, mat in enumerate(a_tx):
            aligned_tx[i][:, sl] = mat
        for name, mat in a_lfp.items():
            aligned_lfp[name][:, sl] = mat
        chan_cursor += c_feat

    aligned_audio = None
    if attach_audio and audio_ns5_path:
        env, audio_sr, audio_ts0 = compute_binned_rms_envelope_from_ns5(
            audio_ns5_path,
            bin_ms=bin_ms,
            detrend_type=audio_detrend,
            start_time_s=float(chunk_start_s) if chunk_mode else 0.0,
            data_time_s=float(chunk_duration_s) if chunk_mode else "all",
            initial_decimate_to_hz=initial_decimate_to_hz,
        )
        if env.size:
            ref_out_start = -min_start
            aligned_audio = place_chunk_into_full(
                env,
                desired_len,
                ref_out_start,
                fill_value=fill_value,
            )
            print(
                "block %s audio: anchored to ref_hub=%s startBin=%d n_bins=%d"
                % (block_id, ref_hub, int(ref_out_start), int(env.shape[0]))
            )
        else:
            print("warning: audio ns5 loaded but produced empty envelope:", audio_ns5_path)

    out: Dict[str, Any] = {}
    out["spike_band_power_from_ns5"] = aligned_sbp
    for i, thr in enumerate(tx_thresh):
        key = "tx_from_ns5_%d" % int(abs(float(thr)) * 10)
        out[key] = aligned_tx[i]
    out["lfpFeatures"] = aligned_lfp
    out["session_name"] = np.array([os.path.basename(session_dir)], dtype=object)
    out["block_number"] = np.array([str(block_id)], dtype=object)
    out["ns5_source_hubs"] = np.array([entry["hub"] for entry in hub_features], dtype=object)
    out["ns5_source_paths"] = np.array([entry["path"] for entry in hub_features], dtype=object)
    out["ns5_xcorr_reference_hub"] = np.array([ref_hub], dtype=object)
    out["ns5_hub_alignment_lag_bins"] = np.asarray(lag_bins, dtype=np.int32)
    out["ns5_hub_alignment_start_bins"] = np.asarray(raw_start_bins, dtype=np.int32)
    out["ns5_total_channels"] = np.array([total_ch], dtype=np.int32)
    out["ns5_total_bins"] = np.array([desired_len], dtype=np.int32)
    out["ns5_featurizer_bin_ms"] = np.array([bin_ms], dtype=np.float32)
    out["ns5_chunk_start_s"] = np.array([float(chunk_start_s)], dtype=np.float32)
    if chunk_duration_s is None:
        out["ns5_chunk_duration_s"] = np.array([-1.0], dtype=np.float32)
        out["ns5_chunk_mode"] = np.array(["full_block"], dtype=object)
    else:
        out["ns5_chunk_duration_s"] = np.array([float(chunk_duration_s)], dtype=np.float32)
        out["ns5_chunk_mode"] = np.array(["partial_chunk_native_timebase"], dtype=object)
    if initial_decimate_to_hz is not None:
        out["ns5_initial_decimate_to_hz"] = np.array([float(initial_decimate_to_hz)], dtype=np.float32)

    if aligned_audio is not None:
        out["audio_envelope_from_ns5"] = aligned_audio.astype(np.float32, copy=False)
        out["audio_ns5_path"] = np.array([str(audio_ns5_path)], dtype=object)
        out["audio_envelope_detrend"] = np.array([str(audio_detrend)], dtype=object)
        if audio_ts0 is not None:
            out["audio_ns5_timestamp"] = np.array([int(audio_ts0)], dtype=np.int64)

    ensure_dir(os.path.join(derived_dir, "ns5_block_features"))
    mat_out = os.path.join(derived_dir, "ns5_block_features", str(block_id) + ".mat")
    try:
        save_mat_any(mat_out, out)
        print("saved:", mat_out)
    except Exception as e:
        npz_out = os.path.splitext(mat_out)[0] + ".npz"
        np.savez_compressed(npz_out, **out)
        print("failed to write .mat (%s); wrote sidecar: %s" % (e, npz_out))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap._negative_number_matcher = _NEG_CSV_NUMBER_RE  # type: ignore[attr-defined]

    ap.add_argument(
        "--gsutil",
        type=str,
        default=os.path.expanduser("~/google-cloud-sdk/bin/gsutil"),
        help="Path to gsutil (default: ~/google-cloud-sdk/bin/gsutil).",
    )
    ap.add_argument("--bucket", type=str, default="exp_sessions_nearline")
    ap.add_argument("--subject", type=str, required=True, help="e.g. t12 or t20")
    ap.add_argument("--session", type=str, required=True, help="e.g. t12.2025.06.17")
    ap.add_argument("--root-data", type=str, required=True, help="local root data folder")
    ap.add_argument("--root-derived", type=str, required=True, help="local derived output folder")
    ap.add_argument(
        "--local-ns5-subdir",
        type=str,
        default=os.path.join("Data", "NSP_Files"),
        help="Session-relative folder where downloaded NS5 files are stored and read from.",
    )
    ap.add_argument("--hub-prefixes", type=str, default="Hub1,Hub2")
    ap.add_argument("--audio-prefixes", type=str, default="NSP")
    ap.add_argument("--no-audio", action="store_true", help="Disable downloading/attaching NSP audio envelope.")
    ap.add_argument("--download", action="store_true", help="download NS5s before processing")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--blocks", type=str, default="all", help="comma list (e.g. 1,2,3) or 'all'")
    ap.add_argument(
        "--initial-decimate-to-hz",
        type=float,
        default=None,
        help="If set, decimate loaded NS5 data to this sample rate before downstream feature processing.",
    )

    ap.add_argument("--bin-ms", type=float, default=20.0)
    ap.add_argument(
        "--tx-thresh",
        type=str,
        default="-3.5,-4.5,-5.5,-6.5",
        help="Comma-separated thresholds (negative values need quoting or `--tx-thresh=-3.5,...` on some argparse versions).",
    )
    ap.add_argument("--voltage-scale", type=float, default=4.0)
    # LRR is required for all production pipeline runs; keep it enabled by default.
    ap.add_argument("--apply-lrr", action="store_true", default=True)
    ap.add_argument("--lrr-approx", action="store_true")
    ap.add_argument("--apply-car", action="store_true")
    ap.add_argument(
        "--align-by-timestamp",
        action="store_true",
        help="Deprecated. RedisMat-based alignment has been removed from this pipeline.",
    )
    ap.add_argument(
        "--timestamp-field",
        type=str,
        default="binned_neural_nsp_timestamp",
        help="Deprecated. RedisMat-based alignment has been removed from this pipeline.",
    )
    ap.add_argument(
        "--audio-detrend",
        type=str,
        default="linear",
        choices=["constant", "linear"],
        help="Detrend type for NSP audio envelope computation (no bandpass filtering).",
    )
    ap.add_argument(
        "--chunk-start-s",
        type=float,
        default=0.0,
        help="Start time in seconds within the block for partial-chunk featurization. Ignored unless --chunk-duration-s is set.",
    )
    ap.add_argument(
        "--chunk-duration-s",
        type=float,
        default=None,
        help="If set, featurize only this many seconds from each block, starting at --chunk-start-s. Default: full block.",
    )

    ap.add_argument(
        "--redis-prefix",
        action="append",
        default=None,
        help="Deprecated. RedisMat usage has been removed from this pipeline.",
    )

    args = ap.parse_args()

    if args.gsutil and args.gsutil != "gsutil" and not os.path.exists(args.gsutil):
        raise RuntimeError("gsutil not found at: %s" % args.gsutil)
    if args.align_by_timestamp:
        raise RuntimeError("RedisMat-based alignment has been removed from this pipeline.")
    if args.redis_prefix:
        raise RuntimeError("RedisMat usage has been removed from this pipeline.")

    session_gs = "gs://%s/%s/%s" % (args.bucket, args.subject, args.session)
    session_dir = os.path.join(args.root_data, args.session)
    derived_dir = os.path.join(args.root_derived, args.session)
    ensure_dir(session_dir)
    ensure_dir(derived_dir)

    hub_prefixes = [s.strip() for s in args.hub_prefixes.split(",") if s.strip()]
    # NSP files should be ignored by the download path; keep only non-NSP audio prefixes.
    audio_prefixes = [
        s.strip()
        for s in args.audio_prefixes.split(",")
        if s.strip() and s.strip().upper() != "NSP"
    ]
    tx_thresh = [float(x) for x in args.tx_thresh.split(",") if x.strip()]

    block_ids = (
        list_blocks(args.gsutil, session_gs, dry_run=args.dry_run)
        if args.blocks == "all"
        else [b.strip() for b in args.blocks.split(",") if b.strip()]
    )
    if args.dry_run:
        print("[dry-run] blocks:", block_ids)
        return

    for block_id in block_ids:
        urls_by_hub = find_block_ns5_urls(args.gsutil, session_gs, block_id, hub_prefixes, dry_run=args.dry_run)
        if not urls_by_hub:
            print("no ns5 urls found for block", block_id)
            continue

        audio_url = None
        if not args.no_audio and audio_prefixes:
            audio_urls = find_block_ns5_urls_for_prefixes(
                args.gsutil, session_gs, block_id, audio_prefixes, dry_run=args.dry_run
            )
            if audio_urls:
                audio_url = audio_urls[sorted(audio_urls.keys())[0]]

        local_ns5_dir = os.path.join(session_dir, args.local_ns5_subdir)
        ensure_dir(local_ns5_dir)

        ns5_paths_by_hub = []
        for hub in hub_prefixes:
            if hub not in urls_by_hub:
                continue
            url = urls_by_hub[hub]
            fname = url.rstrip("/").split("/")[-1]
            local_path = os.path.join(local_ns5_dir, fname)
            if args.download and not os.path.exists(local_path):
                gsutil_cp(args.gsutil, url, local_path, recursive=False, parallel=True, dry_run=args.dry_run)
            ns5_paths_by_hub.append((hub, local_path))

        audio_local_path = None
        if audio_url:
            audio_fname = audio_url.rstrip("/").split("/")[-1]
            audio_local_path = os.path.join(local_ns5_dir, audio_fname)
            if args.download and not os.path.exists(audio_local_path):
                gsutil_cp(args.gsutil, audio_url, audio_local_path, recursive=False, parallel=True, dry_run=args.dry_run)

        process_block(
            session_dir=session_dir,
            derived_dir=derived_dir,
            block_id=block_id,
            ns5_paths_by_hub=ns5_paths_by_hub,
            bin_ms=args.bin_ms,
            tx_thresh=tx_thresh,
            voltage_scale=args.voltage_scale,
            apply_lrr=args.apply_lrr,
            lrr_approx=args.lrr_approx,
            apply_car=args.apply_car,
            audio_ns5_path=audio_local_path,
            audio_detrend=args.audio_detrend,
            attach_audio=(not args.no_audio),
            dry_run=args.dry_run,
            chunk_start_s=args.chunk_start_s,
            chunk_duration_s=args.chunk_duration_s,
            initial_decimate_to_hz=args.initial_decimate_to_hz,
        )


if __name__ == "__main__":
    main()
