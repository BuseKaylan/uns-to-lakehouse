"""Experiment 01: how much space does sensor data really take?

Simulates the virtual factory and writes the same readings in several formats:
raw MQTT-style JSON lines, CSV, and Parquet with different compression, sort order
and encodings. Reports file size, bytes per reading, write time and a one-year
projection for the plant. It also answers two follow-up questions:

* Where do the bytes go? A per-column breakdown of the Parquet variants.
* What does report-by-exception save? The same plant with every sample published.

    python benchmarks/storage_footprint.py --duration 1d --repeats 3

Results land in benchmarks/results/01_storage_footprint/ (CSV, JSON and, if
matplotlib is installed, PNG charts).
"""

from __future__ import annotations

import argparse
import dataclasses
import gzip
import json
import sys
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "benchmarks"))

from common import environment, markdown, save, timed  # noqa: E402
from unslake.cli import parse_duration  # noqa: E402
from unslake.config import load_config  # noqa: E402
from unslake.sim import VirtualFactory  # noqa: E402

SORT_KEYS = [("area", "ascending"), ("line", "ascending"), ("machine", "ascending"),
             ("metric", "ascending"), ("event_ts", "ascending")]
DELTA = {"event_ts": "DELTA_BINARY_PACKED", "delivered_ts": "DELTA_BINARY_PACKED", "delay_ms": "DELTA_BINARY_PACKED"}


# ------------------------------------------------------------------ writers

def write_jsonl(t: pa.Table, path: Path, compress: bool) -> None:
    """What you get if you log raw UNS messages: one JSON document per message."""
    opener = (lambda p: gzip.open(p, "wt")) if compress else (lambda p: open(p, "w"))
    with opener(path) as f:
        for r in t.to_pylist():
            payload = {"value": r["value_str"] if r["value_str"] is not None else r["value"],
                       "unit": r["unit"], "timestamp": r["event_ts"]}
            topic = f"{r['site']}/{r['area']}/{r['line']}/{r['machine']}/{r['metric']}"
            f.write(json.dumps({"topic": topic, "payload": payload}) + "\n")


def write_csv(t: pa.Table, path: Path, compress: bool) -> None:
    if compress:
        with pa.CompressedOutputStream(str(path), "gzip") as out:
            pacsv.write_csv(t, out)
    else:
        pacsv.write_csv(t, str(path))


def write_parquet(t: pa.Table, path: Path, compression: str, delta: bool = False, split_floats: bool = False) -> None:
    """delta: delta-encode timestamps; split_floats: byte-stream-split encoding for `value`."""
    if not (delta or split_floats):
        pq.write_table(t, path, compression=compression)
        return
    encodings = {k: v for k, v in DELTA.items() if delta and k in t.column_names}
    if split_floats:
        encodings["value"] = "BYTE_STREAM_SPLIT"
    # everything else keeps dictionary encoding (great for the repetitive hierarchy strings)
    dict_cols = [c for c in t.column_names if c not in encodings]
    pq.write_table(t, path, compression=compression, use_dictionary=dict_cols, column_encoding=encodings)


def with_delay(t: pa.Table) -> pa.Table:
    """Replace delivered_ts by the delivery delay in ms, which is 0 for almost every row."""
    delay = pc.subtract(t["delivered_ts"], t["event_ts"]).cast(pa.int32())
    i = t.column_names.index("delivered_ts")
    return t.remove_column(i).add_column(i, "delay_ms", delay)


def variants(t_arrival: pa.Table, t_sorted: pa.Table):
    t_delay = with_delay(t_sorted)
    return [
        ("JSON lines (raw MQTT messages)", "jsonl", lambda p: write_jsonl(t_arrival, p, False)),
        ("JSON lines + gzip", "jsonl.gz", lambda p: write_jsonl(t_arrival, p, True)),
        ("CSV", "csv", lambda p: write_csv(t_arrival, p, False)),
        ("CSV + gzip", "csv.gz", lambda p: write_csv(t_arrival, p, True)),
        ("Parquet, uncompressed", "parquet", lambda p: write_parquet(t_arrival, p, "none")),
        ("Parquet, snappy", "parquet", lambda p: write_parquet(t_arrival, p, "snappy")),
        ("Parquet, zstd", "parquet", lambda p: write_parquet(t_arrival, p, "zstd")),
        ("Parquet, zstd, sorted by sensor + time", "parquet", lambda p: write_parquet(t_sorted, p, "zstd")),
        ("Parquet, zstd, sorted + delta timestamps", "parquet",
         lambda p: write_parquet(t_sorted, p, "zstd", delta=True)),
        ("Parquet, zstd, sorted + delta + byte-stream-split values", "parquet",
         lambda p: write_parquet(t_sorted, p, "zstd", delta=True, split_floats=True)),
        ("Parquet, zstd, sorted + delta + delay instead of delivered_ts", "parquet",
         lambda p: write_parquet(t_delay, p, "zstd", delta=True)),
    ]


def column_sizes(path: Path) -> dict[str, int]:
    md = pq.ParquetFile(path).metadata
    out: dict[str, int] = {}
    for rg in range(md.num_row_groups):
        for c in range(md.num_columns):
            col = md.row_group(rg).column(c)
            out[col.path_in_schema] = out.get(col.path_in_schema, 0) + col.total_compressed_size
    return out


# ------------------------------------------------------------------ simulation

def simulate(cfg, duration: int) -> tuple[pa.Table, VirtualFactory]:
    factory = VirtualFactory(cfg, duration)
    return pa.concat_tables(c.table for c in factory.chunks()), factory


def every_sample_config(cfg):
    """The same plant, but every sample is published (no report-by-exception)."""
    def no_rbe(p):
        return dataclasses.replace(p, deadband=0.0, max_silence=p.interval)
    cfg2 = dataclasses.replace(cfg)
    cfg2.machines = [dataclasses.replace(m, sensors=tuple(no_rbe(p) for p in m.sensors)) for m in cfg.machines]
    return cfg2


# ------------------------------------------------------------------ charts

def charts(results, columns, rbe, out_dir: Path) -> list[Path]:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not installed, skipping charts (pip install -e '.[dev]')", file=sys.stderr)
        return []

    bg, fg, muted, grid = "#0E1525", "#F4F2EC", "#B4B2A9", "#26314B"
    pal = {"json": "#F0997B", "csv": "#F0C46A", "parquet": "#7F77DD", "best": "#5DCAA5"}
    plt.rcParams.update({"font.family": ["Inter", "DejaVu Sans"], "font.size": 12,
                         "text.color": fg, "axes.labelcolor": muted, "xtick.color": muted, "ytick.color": fg})
    paths = []

    # 1. bytes per reading, all formats
    fig, ax = plt.subplots(figsize=(12, 6.5), facecolor=bg)
    ax.set_facecolor(bg)
    names = [r["format"] for r in results][::-1]
    vals = [r["bytes_per_reading"] for r in results][::-1]
    best = min(vals)
    colors = [pal["best"] if v == best else pal["json"] if n.startswith("JSON") else
              pal["csv"] if n.startswith("CSV") else pal["parquet"] for n, v in zip(names, vals)]
    bars = ax.barh(names, vals, color=colors, height=0.62)
    for b_, v in zip(bars, vals):
        ax.text(b_.get_width() + max(vals) * 0.01, b_.get_y() + b_.get_height() / 2, f"{v:.1f} B",
                va="center", color=fg, fontsize=11)
    ax.set_xlabel("Bytes per published reading")
    ax.set_title("One day of factory data, stored eleven ways", color=fg, loc="left", fontsize=16, pad=14,
                 fontweight="bold")
    for s in ax.spines.values():
        s.set_visible(False)
    ax.xaxis.grid(True, color=grid); ax.set_axisbelow(True); ax.tick_params(length=0)
    fig.tight_layout()
    p = out_dir / "bytes_per_reading.png"; fig.savefig(p, dpi=150, facecolor=bg); plt.close(fig); paths.append(p)

    # 2. where the bytes go: stacked columns for the Parquet variants
    order = ["value", "event_ts", "delivered_ts", "delay_ms", "metric", "unit", "machine", "line", "area"]
    cpal = {"value": "#7F77DD", "event_ts": "#5DCAA5", "delivered_ts": "#9FE1CB", "delay_ms": "#9FE1CB",
            "metric": "#F0C46A", "unit": "#F6DFA6", "machine": "#F0997B", "line": "#F5C4B3", "area": "#D85A30"}
    labels = [c["format"].replace("Parquet, ", "").replace(" instead of delivered_ts", "") for c in columns]
    fig, ax = plt.subplots(figsize=(12, 6.5), facecolor=bg)
    ax.set_facecolor(bg)
    left = [0.0] * len(columns)
    for col in order:
        widths = [c["columns"].get(col, 0) / 1e6 for c in columns]
        if not any(w > 0.005 for w in widths):
            continue
        ax.barh(labels, widths, left=left, color=cpal[col], height=0.6, label=col)
        left = [l + w for l, w in zip(left, widths)]
    other = [c["total"] / 1e6 - l for c, l in zip(columns, left)]
    ax.barh(labels, other, left=left, color="#4A5675", height=0.6, label="other + metadata")
    ax.invert_yaxis()
    ax.set_xlabel("MB per day")
    ax.set_title("Where the bytes go: Parquet size by column", color=fg, loc="left", fontsize=16, pad=14,
                 fontweight="bold")
    leg = ax.legend(frameon=False, ncol=5, loc="lower center", bbox_to_anchor=(0.45, -0.32), fontsize=11)
    for txt in leg.get_texts():
        txt.set_color(muted)
    for s in ax.spines.values():
        s.set_visible(False)
    ax.xaxis.grid(True, color=grid); ax.set_axisbelow(True); ax.tick_params(length=0)
    fig.tight_layout()
    p = out_dir / "bytes_by_column.png"; fig.savefig(p, dpi=150, facecolor=bg); plt.close(fig); paths.append(p)

    # 3. report-by-exception: published vs every sample
    fig, ax = plt.subplots(figsize=(12, 4.5), facecolor=bg)
    ax.set_facecolor(bg)
    fmts = [r["format"] for r in rbe if r["mode"] == "every sample"]
    x = range(len(fmts))
    full = [next(r["size_mb"] for r in rbe if r["format"] == f and r["mode"] == "every sample") for f in fmts]
    pub = [next(r["size_mb"] for r in rbe if r["format"] == f and r["mode"] == "report-by-exception") for f in fmts]
    ax.barh([i + 0.2 for i in x], full, height=0.38, color="#4A5675", label="every sample")
    ax.barh([i - 0.2 for i in x], pub, height=0.38, color=pal["best"], label="report-by-exception")
    for i, (f_, p_) in enumerate(zip(full, pub)):
        ax.text(f_ + max(full) * 0.01, i + 0.2, f"{f_:.1f} MB", va="center", color=muted, fontsize=11)
        ax.text(p_ + max(full) * 0.01, i - 0.2, f"{p_:.1f} MB", va="center", color=fg, fontsize=11)
    ax.set_yticks(list(x)); ax.set_yticklabels(fmts)
    ax.set_xlabel("MB per day")
    ax.set_title("What report-by-exception saves", color=fg, loc="left", fontsize=16, pad=14, fontweight="bold")
    leg = ax.legend(frameon=False, loc="lower right", fontsize=11)
    for txt in leg.get_texts():
        txt.set_color(muted)
    for s in ax.spines.values():
        s.set_visible(False)
    ax.xaxis.grid(True, color=grid); ax.set_axisbelow(True); ax.tick_params(length=0)
    fig.tight_layout()
    p = out_dir / "report_by_exception.png"; fig.savefig(p, dpi=150, facecolor=bg); plt.close(fig); paths.append(p)
    return paths


# ------------------------------------------------------------------ main

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default=str(ROOT / "config" / "factory.yaml"))
    ap.add_argument("--duration", type=parse_duration, default=parse_duration("1d"))
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--out", default=str(ROOT / "benchmarks" / "results" / "01_storage_footprint"))
    ap.add_argument("--skip-rbe", action="store_true", help="skip the every-sample comparison (faster)")
    args = ap.parse_args()
    out_dir = Path(args.out)

    cfg = load_config(args.config)
    print(f"simulating {args.duration}s ...", file=sys.stderr)
    table, factory = simulate(cfg, args.duration)
    arrival = table.take(pc.sort_indices(table, sort_keys=[("delivered_ts", "ascending")]))
    sorted_ = table.sort_by(SORT_KEYS)
    n = table.num_rows
    samples = sum(args.duration // s.p.interval for s in factory.sensors if s.p.kind != "state")
    year_factor = 365 * 86400 / args.duration

    results, columns = [], []
    with tempfile.TemporaryDirectory() as tmp:
        for i, (name, ext, fn) in enumerate(variants(arrival, sorted_)):
            path = Path(tmp) / f"v{i}.{ext}"
            seconds, _ = timed(lambda: fn(path), args.repeats)
            size = path.stat().st_size
            results.append({
                "format": name,
                "size_mb": round(size / 1e6, 2),
                "bytes_per_reading": round(size / n, 2),
                "write_seconds": round(seconds, 2),
                "gb_per_year": round(size * year_factor / 1e9, 2),
            })
            if ext == "parquet":
                columns.append({"format": name, "total": size, "columns": column_sizes(path)})
            print(f"  {name:<62} {size / 1e6:9.2f} MB  {size / n:7.2f} B/reading", file=sys.stderr)

        base = results[0]["size_mb"]
        for r in results:
            r["vs_json"] = f"{base / r['size_mb']:.1f}x smaller" if r["size_mb"] < base else "baseline"

        # report-by-exception vs every sample
        rbe = []
        if not args.skip_rbe:
            print("simulating the same plant with every sample published ...", file=sys.stderr)
            full, _ = simulate(every_sample_config(cfg), args.duration)
            full_sorted = full.sort_by(SORT_KEYS)
            full_arrival = full.take(pc.sort_indices(full, sort_keys=[("delivered_ts", "ascending")]))
            pairs = [("JSON lines (raw MQTT messages)", "jsonl",
                      lambda t, p: write_jsonl(t, p, False), arrival, full_arrival),
                     ("Parquet, best variant", "parquet",
                      lambda t, p: write_parquet(with_delay(t), p, "zstd", delta=True), sorted_, full_sorted)]
            for name, ext, fn, pub_t, full_t in pairs:
                for mode, t_ in (("report-by-exception", pub_t), ("every sample", full_t)):
                    path = Path(tmp) / f"rbe.{ext}"
                    fn(t_, path)
                    rbe.append({"format": name, "mode": mode, "rows": t_.num_rows,
                                "size_mb": round(path.stat().st_size / 1e6, 2)})
                    print(f"  {name:<34} {mode:<20} {t_.num_rows:>10,} rows "
                          f"{path.stat().st_size / 1e6:9.2f} MB", file=sys.stderr)

    meta = {
        "experiment": "01_storage_footprint",
        "duration_s": args.duration,
        "seed": cfg.seed,
        "machines": len(cfg.machines),
        "signals": len(factory.sensors),
        "simulated_samples": samples,
        "published_readings": n,
        "report_by_exception_ratio": round(n / samples, 3),
        "repeats": args.repeats,
        "environment": environment(),
    }
    save(results, "storage_footprint", meta, out_dir)
    col_rows = [{"format": c["format"], "column": k, "bytes": v} for c in columns for k, v in c["columns"].items()]
    save(col_rows, "bytes_by_column", meta, out_dir)
    if rbe:
        save(rbe, "report_by_exception", meta, out_dir)
    pngs = charts(results, columns, rbe, out_dir) if rbe else []

    print(f"\n{meta['machines']} machines, {meta['signals']} signals, {args.duration}s simulated")
    print(f"{samples:,} samples taken, {n:,} published after report-by-exception "
          f"({100 * n / samples:.0f}%)\n")
    print(markdown(results, [("format", "Format"), ("size_mb", "Size (MB)"),
                             ("bytes_per_reading", "Bytes / reading"), ("vs_json", "vs. JSON"),
                             ("gb_per_year", "GB / year"), ("write_seconds", "Write (s)")]))
    print("\nWhere the bytes go (MB):")
    keys = ["value", "event_ts", "delivered_ts", "delay_ms", "metric", "unit", "machine", "line", "area"]
    for c in columns:
        parts = ", ".join(f"{k} {c['columns'][k] / 1e6:.2f}" for k in keys if c["columns"].get(k, 0) > 5000)
        print(f"  {c['format']:<62} {parts}")
    if rbe:
        print("\nReport-by-exception vs every sample:")
        print(markdown(rbe, [("format", "Format"), ("mode", "Mode"), ("rows", "Rows"), ("size_mb", "Size (MB)")]))
    print(f"\nresults: {out_dir}" + (f"  charts: {', '.join(p.name for p in pngs)}" if pngs else ""))


if __name__ == "__main__":
    main()
