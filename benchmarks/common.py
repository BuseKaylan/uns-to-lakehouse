"""Shared helpers for experiments: environment capture and result tables."""

from __future__ import annotations

import csv
import json
import os
import platform
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path


def environment() -> dict:
    import numpy
    import pyarrow

    env = {
        "date_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "python": platform.python_version(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpus": os.cpu_count(),
        "numpy": numpy.__version__,
        "pyarrow": pyarrow.__version__,
    }
    try:
        import duckdb
        env["duckdb"] = duckdb.__version__
    except ImportError:
        pass
    return env


def timed(fn, repeats: int) -> tuple[float, object]:
    """Run fn `repeats` times, return (median seconds, last result)."""
    times, result = [], None
    for _ in range(repeats):
        t = time.perf_counter()
        result = fn()
        times.append(time.perf_counter() - t)
    return statistics.median(times), result


def save(results: list[dict], name: str, meta: dict, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    with open(out_dir / f"{name}.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(results[0].keys()))
        w.writeheader()
        w.writerows(results)
    (out_dir / f"{name}.json").write_text(json.dumps({"meta": meta, "results": results}, indent=2))


def markdown(results: list[dict], columns: list[tuple[str, str]]) -> str:
    head = "| " + " | ".join(label for _, label in columns) + " |"
    sep = "|" + "|".join("---" for _ in columns) + "|"
    rows = ["| " + " | ".join(str(r[k]) for k, _ in columns) + " |" for r in results]
    return "\n".join([head, sep, *rows])
