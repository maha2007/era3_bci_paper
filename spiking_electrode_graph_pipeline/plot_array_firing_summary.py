#!/usr/bin/env python3
import argparse
import html
import json
import re
import warnings
from pathlib import Path
from typing import Iterable, List, Sequence

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from scipy.io import loadmat

THIS_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = THIS_DIR / "output"
SERIES_COLORS = [
    "#4e79a7",
    "#f28e2b",
    "#59a14f",
    "#e15759",
    "#76b7b2",
    "#edc948",
    "#b07aa1",
    "#ff9da7",
    "#9c755f",
    "#bab0ab",
]


def natural_sort_key(path: Path):
    parts = re.split(r"(\d+)", path.stem)
    out = []
    for part in parts:
        if part.isdigit():
            out.append(int(part))
        else:
            out.append(part)
    return out


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Summarize per-array firing counts from NS5 threshold-crossing .mat files."
    )
    ap.add_argument(
        "inputs",
        type=Path,
        nargs="+",
        help="One directory containing .mat files, or one or more explicit .mat files.",
    )
    ap.add_argument(
        "--pattern",
        type=str,
        default="*.mat",
        help="Glob pattern used to find input files inside input_dir.",
    )
    ap.add_argument(
        "--tx-key",
        type=str,
        default="tx_from_ns5_45",
        help="Threshold-crossing matrix key to use.",
    )
    ap.add_argument(
        "--window-sec",
        type=float,
        default=30.0,
        help="Window size in seconds for mean frequency calculation.",
    )
    ap.add_argument(
        "--firing-threshold-hz",
        type=float,
        default=2.0,
        help="Median 30-second mean frequency threshold for calling an electrode firing.",
    )
    ap.add_argument(
        "--array-size",
        type=int,
        default=64,
        help="Number of electrodes per array.",
    )
    ap.add_argument(
        "--limit",
        type=int,
        default=None,
        help="If set, analyze only the first N naturally sorted files.",
    )
    ap.add_argument(
        "--x-label",
        type=str,
        default="Session / Block",
        help="Label for the x axis.",
    )
    ap.add_argument(
        "--plot-title",
        type=str,
        default=None,
        help="Optional explicit plot title.",
    )
    ap.add_argument(
        "--out-prefix",
        type=Path,
        default=None,
        help="Output prefix. Defaults to <this pipeline>/output/array_firing_summary.",
    )
    ap.add_argument(
        "--label-mode",
        type=str,
        choices=("stem", "index"),
        default="stem",
        help="Use input file stems or sequential numeric labels on the x axis.",
    )
    ap.add_argument(
        "--label-start",
        type=int,
        default=0,
        help="Starting value for sequential labels when --label-mode index is used.",
    )
    ap.add_argument(
        "--x-tick-label-mode",
        type=str,
        choices=("label", "index"),
        default="label",
        help="Use data labels or sequential numeric indices for x-axis tick labels.",
    )
    ap.add_argument(
        "--x-tick-label-start",
        type=int,
        default=1,
        help="Starting value for x-axis tick labels when --x-tick-label-mode index is used.",
    )
    ap.add_argument(
        "--labels",
        type=str,
        nargs="+",
        default=None,
        help="Optional explicit data labels. Must match the number of resolved inputs.",
    )
    return ap.parse_args()


def infer_bin_ms(data: dict) -> float:
    if "ns5_featurizer_bin_ms" not in data:
        raise KeyError("ns5_featurizer_bin_ms not found in .mat file.")
    return float(np.asarray(data["ns5_featurizer_bin_ms"]).reshape(-1)[0])


def load_tx_time_by_channel(mat_path: Path, tx_key: str) -> tuple[np.ndarray, float]:
    data = loadmat(mat_path)
    if tx_key not in data:
        available = sorted(k for k in data.keys() if not k.startswith("__"))
        raise KeyError(f"{tx_key} not found in {mat_path}. Available keys: {available}")

    tx = np.asarray(data[tx_key], dtype=np.float32)
    if tx.ndim != 2:
        raise ValueError(f"{tx_key} in {mat_path} must be 2D, got {tx.shape}")

    if tx.shape[0] <= tx.shape[1]:
        tx = tx.T

    return tx, infer_bin_ms(data)


def compute_window_mean_rates_hz(
    tx_time_by_channel: np.ndarray,
    bin_ms: float,
    window_sec: float,
) -> np.ndarray:
    bins_per_window = int(round((window_sec * 1000.0) / bin_ms))
    if bins_per_window <= 0:
        raise ValueError("window_sec and bin_ms produce zero bins per window.")

    n_windows = tx_time_by_channel.shape[0] // bins_per_window
    if n_windows <= 0:
        raise ValueError(
            f"Input has only {tx_time_by_channel.shape[0]} bins, fewer than one {window_sec:.1f}s window."
        )

    use_len = n_windows * bins_per_window
    tx_used = tx_time_by_channel[:use_len]
    tx_windows = tx_used.reshape(n_windows, bins_per_window, tx_time_by_channel.shape[1])

    # Partial-chunk .mat files pad non-covered bins with NaN. Treat those bins as
    # missing data instead of zero activity when computing per-window rates.
    window_counts = np.nansum(tx_windows, axis=1)
    valid_bins = np.isfinite(tx_windows).sum(axis=1).astype(np.float32, copy=False)
    valid_sec = valid_bins * float(bin_ms) / 1000.0
    return np.divide(
        window_counts,
        valid_sec,
        out=np.full_like(window_counts, np.nan, dtype=np.float32),
        where=valid_sec > 0,
    )


def count_firing_electrodes_per_array(
    tx_time_by_channel: np.ndarray,
    bin_ms: float,
    window_sec: float,
    firing_threshold_hz: float,
    array_size: int,
) -> tuple[np.ndarray, np.ndarray]:
    n_channels = tx_time_by_channel.shape[1]
    n_arrays, remainder = divmod(n_channels, array_size)
    if n_arrays <= 0:
        raise ValueError(
            f"Channel count {n_channels} is smaller than array_size={array_size}; no full arrays available."
        )
    if remainder:
        warnings.warn(
            f"Ignoring {remainder} trailing channels from input with {n_channels} channels; "
            f"using {n_arrays} full arrays of size {array_size}.",
            stacklevel=2,
        )
        tx_time_by_channel = tx_time_by_channel[:, : n_arrays * array_size]

    rates_hz = compute_window_mean_rates_hz(tx_time_by_channel, bin_ms=bin_ms, window_sec=window_sec)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)
        median_rate_hz = np.nanmedian(rates_hz, axis=0)
    firing_mask = median_rate_hz > float(firing_threshold_hz)
    counts = firing_mask.reshape(n_arrays, array_size).sum(axis=1).astype(np.int32)
    return counts, median_rate_hz


def write_summary_csv(
    out_csv: Path,
    labels: Sequence[str],
    input_files: Sequence[Path],
    counts_by_label: np.ndarray,
) -> None:
    lines = ["label,input_file,array_index,array_name,firing_electrodes"]
    for label, input_file, counts in zip(labels, input_files, counts_by_label):
        for array_idx, count in enumerate(counts, start=1):
            if np.isnan(count):
                continue
            lines.append(f"{label},{input_file},{array_idx},Array {array_idx},{int(count)}")
    out_csv.write_text("\n".join(lines) + "\n")


def plot_counts(
    out_png: Path,
    x_tick_labels: Sequence[str],
    counts_by_label: np.ndarray,
    x_label: str,
    title: str,
) -> None:
    x = np.arange(len(x_tick_labels), dtype=np.int32)
    n_arrays = counts_by_label.shape[1]

    plt.figure(figsize=(13, 7))
    for array_idx in range(n_arrays):
        valid_mask = np.isfinite(counts_by_label[:, array_idx])
        if not np.any(valid_mask):
            continue
        plt.plot(
            x[valid_mask],
            counts_by_label[valid_mask, array_idx],
            marker="o",
            linewidth=2,
            markersize=6,
            label=f"Array {array_idx + 1}",
        )

    plt.xticks(x, x_tick_labels, rotation=0, ha="center")
    plt.ylim(0, np.nanmax(counts_by_label) + 5)
    plt.ylabel("Number of electrodes firing")
    plt.xlabel(x_label)
    plt.title(title)
    plt.grid(True, axis="y", alpha=0.25)
    plt.legend(ncol=2, frameon=False)
    plt.tight_layout()
    plt.savefig(out_png, dpi=200)
    plt.close()


def write_interactive_html(
    out_html: Path,
    labels: Sequence[str],
    x_tick_labels: Sequence[str],
    input_files: Sequence[Path],
    counts_by_label: np.ndarray,
    x_label: str,
    title: str,
) -> None:
    n_arrays = counts_by_label.shape[1]
    finite_counts = counts_by_label[np.isfinite(counts_by_label)]
    y_max = int(np.ceil(float(finite_counts.max()) + 5.0)) if finite_counts.size else 1
    y_max = max(1, y_max)

    series = []
    for array_idx in range(n_arrays):
        values = []
        for value in counts_by_label[:, array_idx]:
            values.append(None if np.isnan(value) else int(value))
        series.append(
            {
                "name": f"Array {array_idx + 1}",
                "color": SERIES_COLORS[array_idx % len(SERIES_COLORS)],
                "values": values,
            }
        )

    payload = {
        "title": title,
        "xLabel": x_label,
        "yLabel": "Number of electrodes firing",
        "yMax": y_max,
        "sessions": [
            {
                "label": str(label),
                "tickLabel": str(tick_label),
                "inputFile": str(input_file),
            }
            for label, tick_label, input_file in zip(labels, x_tick_labels, input_files)
        ],
        "series": series,
    }
    safe_json = (
        json.dumps(payload, separators=(",", ":"))
        .replace("&", "\\u0026")
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
    )
    escaped_title = html.escape(title)

    out_html.write_text(
        f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{escaped_title}</title>
<style>
:root {{
  color-scheme: light;
  --axis: #2f3437;
  --grid: #d9dee3;
  --text: #1f2328;
  --muted: #66717a;
  --select: #111827;
}}
* {{
  box-sizing: border-box;
}}
body {{
  margin: 0;
  font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  color: var(--text);
  background: #ffffff;
}}
main {{
  width: min(1180px, 100vw);
  margin: 0 auto;
  padding: 24px 20px 18px;
}}
h1 {{
  margin: 0 0 12px;
  font-size: 22px;
  font-weight: 680;
  letter-spacing: 0;
}}
.toolbar {{
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  margin: 0 0 8px;
  color: var(--muted);
  font-size: 14px;
}}
button {{
  border: 1px solid #c9d1d9;
  background: #ffffff;
  color: var(--text);
  border-radius: 6px;
  padding: 6px 10px;
  font: inherit;
  cursor: pointer;
}}
button:hover {{
  border-color: #8c959f;
  background: #f6f8fa;
}}
svg {{
  display: block;
  width: 100%;
  height: auto;
}}
.axis-line {{
  stroke: var(--axis);
  stroke-width: 1.3;
}}
.grid-line {{
  stroke: var(--grid);
  stroke-width: 1;
}}
.tick text,
.axis-label,
.legend text {{
  fill: var(--text);
  font-size: 13px;
}}
.axis-label {{
  font-weight: 620;
}}
.series-line {{
  fill: none;
  stroke-width: 2.4;
}}
.marker {{
  cursor: pointer;
  stroke: #ffffff;
  stroke-width: 1.5;
  outline: none;
}}
.marker:hover,
.marker:focus {{
  stroke: var(--select);
  stroke-width: 3;
}}
.marker.active {{
  stroke: var(--select);
  stroke-width: 3.4;
}}
.selection-line {{
  stroke: var(--select);
  stroke-width: 1.8;
  stroke-dasharray: 7 6;
  opacity: 0.72;
  pointer-events: none;
}}
</style>
</head>
<body>
<main>
  <h1>{escaped_title}</h1>
  <div class="toolbar">
    <div id="selection-status" aria-live="polite">No session selected</div>
    <button type="button" id="clear-selection">Clear</button>
  </div>
  <svg id="chart" viewBox="0 0 1100 680" role="img" aria-labelledby="chart-title">
    <title id="chart-title">{escaped_title}</title>
  </svg>
</main>
<script type="application/json" id="plot-data">{safe_json}</script>
<script>
const data = JSON.parse(document.getElementById("plot-data").textContent);
const svg = document.getElementById("chart");
const statusEl = document.getElementById("selection-status");
const clearButton = document.getElementById("clear-selection");
const ns = "http://www.w3.org/2000/svg";
const width = 1100;
const height = 680;
const margin = {{ top: 28, right: 170, bottom: 92, left: 74 }};
const plotWidth = width - margin.left - margin.right;
const plotHeight = height - margin.top - margin.bottom;
const selectedSessions = new Set();

function makeSvg(tag, attrs = {{}}, text = null) {{
  const el = document.createElementNS(ns, tag);
  for (const [key, value] of Object.entries(attrs)) {{
    el.setAttribute(key, value);
  }}
  if (text !== null) {{
    el.textContent = text;
  }}
  return el;
}}

function xForIndex(index) {{
  if (data.sessions.length <= 1) {{
    return margin.left + plotWidth / 2;
  }}
  return margin.left + (index / (data.sessions.length - 1)) * plotWidth;
}}

function yForValue(value) {{
  return margin.top + plotHeight - (value / data.yMax) * plotHeight;
}}

function buildPath(values) {{
  let d = "";
  values.forEach((value, index) => {{
    if (value === null) {{
      return;
    }}
    d += `${{d ? " L" : " M"}} ${{xForIndex(index)}} ${{yForValue(value)}}`;
  }});
  return d.trim();
}}

const gridG = makeSvg("g");
const selectionG = makeSvg("g");
const seriesG = makeSvg("g");
const axesG = makeSvg("g");
svg.append(gridG, selectionG, seriesG, axesG);

const yTickCount = 5;
for (let i = 0; i <= yTickCount; i += 1) {{
  const value = Math.round((data.yMax / yTickCount) * i);
  const y = yForValue(value);
  gridG.append(makeSvg("line", {{
    x1: margin.left,
    y1: y,
    x2: margin.left + plotWidth,
    y2: y,
    class: "grid-line"
  }}));
  const tick = makeSvg("g", {{ class: "tick" }});
  tick.append(makeSvg("text", {{
    x: margin.left - 12,
    y: y + 4,
    "text-anchor": "end"
  }}, String(value)));
  axesG.append(tick);
}}

axesG.append(makeSvg("line", {{
  x1: margin.left,
  y1: margin.top + plotHeight,
  x2: margin.left + plotWidth,
  y2: margin.top + plotHeight,
  class: "axis-line"
}}));
axesG.append(makeSvg("line", {{
  x1: margin.left,
  y1: margin.top,
  x2: margin.left,
  y2: margin.top + plotHeight,
  class: "axis-line"
}}));

const xTickStep = Math.max(1, Math.ceil(data.sessions.length / 28));
data.sessions.forEach((session, index) => {{
  if (index % xTickStep !== 0 && index !== data.sessions.length - 1) {{
    return;
  }}
  const x = xForIndex(index);
  axesG.append(makeSvg("line", {{
    x1: x,
    y1: margin.top + plotHeight,
    x2: x,
    y2: margin.top + plotHeight + 6,
    class: "axis-line"
  }}));
  axesG.append(makeSvg("text", {{
    x: x,
    y: margin.top + plotHeight + 24,
    "text-anchor": "middle",
    class: "tick"
  }}, session.tickLabel));
}});

axesG.append(makeSvg("text", {{
  x: margin.left + plotWidth / 2,
  y: height - 24,
  "text-anchor": "middle",
  class: "axis-label"
}}, data.xLabel));
axesG.append(makeSvg("text", {{
  x: 22,
  y: margin.top + plotHeight / 2,
  transform: `rotate(-90 22 ${{margin.top + plotHeight / 2}})`,
  "text-anchor": "middle",
  class: "axis-label"
}}, data.yLabel));

function markerLabel(series, sessionIndex, value) {{
  const session = data.sessions[sessionIndex];
  return `${{session.label}}, ${{series.name}}: ${{value}} firing electrodes`;
}}

data.series.forEach((series, seriesIndex) => {{
  const group = makeSvg("g", {{ class: "series" }});
  group.append(makeSvg("path", {{
    d: buildPath(series.values),
    class: "series-line",
    stroke: series.color
  }}));

  series.values.forEach((value, sessionIndex) => {{
    if (value === null) {{
      return;
    }}
    const marker = makeSvg("circle", {{
      cx: xForIndex(sessionIndex),
      cy: yForValue(value),
      r: 5.2,
      fill: series.color,
      class: "marker",
      tabindex: "0",
      role: "button",
      "data-session": String(sessionIndex),
      "data-array": String(seriesIndex + 1),
      "aria-label": markerLabel(series, sessionIndex, value)
    }});
    marker.append(makeSvg("title", {{}}, markerLabel(series, sessionIndex, value)));
    marker.addEventListener("click", () => toggleSession(sessionIndex));
    marker.addEventListener("keydown", (event) => {{
      if (event.key === "Enter" || event.key === " ") {{
        event.preventDefault();
        toggleSession(sessionIndex);
      }}
    }});
    group.append(marker);
  }});
  seriesG.append(group);
}});

const legendX = margin.left + plotWidth + 28;
const legendY = margin.top + 6;
const legend = makeSvg("g", {{ class: "legend" }});
data.series.forEach((series, index) => {{
  const y = legendY + index * 24;
  legend.append(makeSvg("line", {{
    x1: legendX,
    y1: y,
    x2: legendX + 20,
    y2: y,
    stroke: series.color,
    "stroke-width": 2.8
  }}));
  legend.append(makeSvg("circle", {{
    cx: legendX + 10,
    cy: y,
    r: 4.4,
    fill: series.color,
    stroke: "#ffffff",
    "stroke-width": 1.3
  }}));
  legend.append(makeSvg("text", {{
    x: legendX + 30,
    y: y + 4
  }}, series.name));
}});
axesG.append(legend);

function renderSelections() {{
  selectionG.replaceChildren();
  document.querySelectorAll(".marker").forEach((marker) => {{
    marker.classList.toggle("active", selectedSessions.has(Number(marker.dataset.session)));
  }});

  const sessionIndexes = Array.from(selectedSessions).sort((a, b) => a - b);
  sessionIndexes.forEach((sessionIndex) => {{
    const x = xForIndex(sessionIndex);
    const line = makeSvg("line", {{
      x1: x,
      y1: margin.top,
      x2: x,
      y2: margin.top + plotHeight,
      class: "selection-line"
    }});
    line.append(makeSvg("title", {{}}, data.sessions[sessionIndex].label));
    selectionG.append(line);
  }});

  statusEl.textContent = sessionIndexes.length
    ? sessionIndexes.map((index) => data.sessions[index].label).join(", ")
    : "No session selected";
}}

function toggleSession(sessionIndex) {{
  if (selectedSessions.has(sessionIndex)) {{
    selectedSessions.delete(sessionIndex);
  }} else {{
    selectedSessions.add(sessionIndex);
  }}
  renderSelections();
}}

clearButton.addEventListener("click", () => {{
  selectedSessions.clear();
  renderSelections();
}});
</script>
</body>
</html>
""",
        encoding="utf-8",
    )


def find_input_files(input_dir: Path, pattern: str) -> List[Path]:
    files = [p for p in input_dir.glob(pattern) if p.is_file()]
    files.sort(key=natural_sort_key)
    return files


def resolve_input_files(inputs: Sequence[Path], pattern: str) -> List[Path]:
    if len(inputs) == 1 and inputs[0].is_dir():
        return find_input_files(inputs[0], pattern)

    files: List[Path] = []
    for path in inputs:
        if path.is_dir():
            files.extend(find_input_files(path, pattern))
        elif path.is_file():
            files.append(path)
        else:
            raise FileNotFoundError(f"Input path not found: {path}")

    # Preserve the caller's explicit file order; only de-duplicate repeats.
    seen = set()
    ordered: List[Path] = []
    for path in files:
        resolved = path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        ordered.append(path)
    return ordered


def build_labels(
    input_files: Sequence[Path],
    label_mode: str,
    label_start: int,
    explicit_labels: Sequence[str] | None = None,
) -> List[str]:
    if explicit_labels is not None:
        labels = [str(label) for label in explicit_labels]
        if len(labels) != len(input_files):
            raise ValueError(
                f"--labels provided {len(labels)} values, but resolved {len(input_files)} input files."
            )
        return labels
    if label_mode == "index":
        return [str(label_start + idx) for idx in range(len(input_files))]
    return [path.stem for path in input_files]


def build_x_tick_labels(labels: Sequence[str], x_tick_label_mode: str, x_tick_label_start: int) -> List[str]:
    if x_tick_label_mode == "index":
        return [str(x_tick_label_start + idx) for idx in range(len(labels))]
    return [str(label) for label in labels]


def main() -> None:
    args = parse_args()

    input_files = resolve_input_files(args.inputs, args.pattern)
    if args.limit is not None:
        input_files = input_files[: args.limit]

    if not input_files:
        joined = ", ".join(str(x) for x in args.inputs)
        raise FileNotFoundError(f"No input .mat files resolved from: {joined}")

    counts_list = []
    labels = build_labels(
        input_files,
        args.label_mode,
        args.label_start,
        explicit_labels=args.labels,
    )
    x_tick_labels = build_x_tick_labels(
        labels,
        args.x_tick_label_mode,
        args.x_tick_label_start,
    )
    for path, label in zip(input_files, labels):
        tx, bin_ms = load_tx_time_by_channel(path, args.tx_key)
        counts, _ = count_firing_electrodes_per_array(
            tx_time_by_channel=tx,
            bin_ms=bin_ms,
            window_sec=args.window_sec,
            firing_threshold_hz=args.firing_threshold_hz,
            array_size=args.array_size,
        )
        counts_list.append(counts)
        print(
            f"{path.name}: bin_ms={bin_ms:g}, channels={tx.shape[1]}, "
            f"arrays={counts.shape[0]}, label={label}, firing_counts={counts.tolist()}"
        )

    max_arrays = max(counts.shape[0] for counts in counts_list)
    counts_by_label = np.full((len(counts_list), max_arrays), np.nan, dtype=np.float32)
    for row_idx, counts in enumerate(counts_list):
        counts_by_label[row_idx, : counts.shape[0]] = counts.astype(np.float32, copy=False)
    out_prefix = args.out_prefix or (DEFAULT_OUTPUT_DIR / "array_firing_summary")
    out_prefix.parent.mkdir(parents=True, exist_ok=True)
    out_csv = out_prefix.with_suffix(".csv")
    out_png = out_prefix.with_suffix(".png")
    out_html = out_prefix.with_suffix(".html")

    title = args.plot_title or (
        f"Firing electrodes per array "
        f"(median 30 s mean rate > {args.firing_threshold_hz:g} Hz)"
    )

    write_summary_csv(out_csv, labels, input_files, counts_by_label)
    plot_counts(
        out_png=out_png,
        x_tick_labels=x_tick_labels,
        counts_by_label=counts_by_label,
        x_label=args.x_label,
        title=title,
    )
    write_interactive_html(
        out_html=out_html,
        labels=labels,
        x_tick_labels=x_tick_labels,
        input_files=input_files,
        counts_by_label=counts_by_label,
        x_label=args.x_label,
        title=title,
    )

    print(f"Wrote CSV: {out_csv}")
    print(f"Wrote plot: {out_png}")
    print(f"Wrote interactive plot: {out_html}")


if __name__ == "__main__":
    main()
