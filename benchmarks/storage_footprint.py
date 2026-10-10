"""Experiment 01: how much space does sensor data really take?

Simulates the virtual factory and writes the same readings in several formats:
raw MQTT-style JSON lines, CSV, and Parquet with different compression, sort order
and encodings. Reports file size, bytes per reading, write time and a one-year
projection for the plant.

    python benchmarks/storage_footprint.py --duration 1d --repeats 3
"""

from __future__ import annotations

import argparse
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


def write_parquet(t: pa.Table, path: Path, compression: str, tuned: bool = False) -> None:
    if not tuned:
        pq.write_table(t, path, compression=compression)
        return
    # delta encoding for sorted timestamps, byte-stream-split for floats,
    # dictionary for the repetitive hierarchy strings
    encodings = {"event_ts": "DELTA_BINARY_PACKED", "delivered_ts": "DELTA_BINARY_PACKED",
                 "value": "BYTE_STREAM_SPLIT"}
    dict_cols = [c for c in t.column_names if c not in encodings]
    pq.write_table(t, path, compression=compression, use_dictionary=dict_cols, column_encoding=encodings)


def variants(t_arrival: pa.Table, t_sorted: pa.Table):
    return [
        ("JSON lines (raw MQTT messages)", "jsonl", lambda p: write_jsonl(t_arrival, p, False)),
        ("JSON lines + gzip", "jsonl.gz", lambda p: write_jsonl(t_arrival, p, True)),
        ("CSV", "csv", lambda p: write_csv(t_arrival, p, False)),
        ("CSV + gzip", "csv.gz", lambda p: write_csv(t_arrival, p, True)),
        ("Parquet, uncompressed", "parquet", lambda p: write_parquet(t_arrival, p, "none")),
        ("Parquet, snappy", "parquet", lambda p: write_parquet(t_arrival, p, "snappy")),
        ("Parquet, zstd", "parquet", lambda p: write_parquet(t_arrival, p, "zstd")),
        ("Parquet, zstd, sorted by sensor + time", "parquet", lambda p: write_parquet(t_sorted, p, "zstd")),
        ("Parquet, zstd, sorted + tuned encodings", "parquet", lambda p: write_parquet(t_sorted, p, "zstd", True)),
    ]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default=str(ROOT / "config" / "factory.yaml"))
    ap.add_argument("--duration", type=parse_duration, default=parse_duration("1d"))
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--out", default=str(ROOT / "benchmarks" / "results"))
    args = ap.parse_args()

    cfg = load_config(args.config)
    factory = VirtualFactory(cfg, args.duration)
    print(f"simulating {args.duration}s ...", file=sys.stderr)
    table = pa.concat_tables(c.table for c in factory.chunks())
    arrival = table.take(pc.sort_indices(table, sort_keys=[("delivered_ts", "ascending")]))
    sorted_ = table.sort_by(SORT_KEYS)
    n = table.num_rows
    samples = sum(args.duration // s.p.interval for s in factory.sensors if s.p.kind != "state")
    year_factor = 365 * 86400 / args.duration

    results = []
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
            print(f"  {name:<42} {size / 1e6:9.2f} MB  {size / n:7.2f} B/reading", file=sys.stderr)

    base = results[0]["size_mb"]
    for r in results:
        r["vs_json"] = f"{base / r['size_mb']:.1f}x smaller" if r["size_mb"] < base else "baseline"

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
    save(results, "01_storage_footprint", meta, Path(args.out))

    print(f"\n{meta['machines']} machines, {meta['signals']} signals, {args.duration}s simulated")
    print(f"{samples:,} samples taken, {n:,} published after report-by-exception "
          f"({100 * n / samples:.0f}%)\n")
    print(markdown(results, [("format", "Format"), ("size_mb", "Size (MB)"),
                             ("bytes_per_reading", "Bytes / reading"), ("vs_json", "vs. JSON"),
                             ("gb_per_year", "GB / year"), ("write_seconds", "Write (s)")]))


if __name__ == "__main__":
    main()
