"""Write a simulation run to Parquet: the dataset every benchmark starts from."""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow.parquet as pq

from ..sim import SCHEMA, Chunk, VirtualFactory
from .base import Sink


class ParquetSink(Sink):
    """Writes <out>/readings.parquet, <out>/ground_truth.parquet and <out>/run.json."""

    def __init__(self, out_dir: str | Path, compression: str = "zstd"):
        self.out = Path(out_dir)
        self.compression = compression
        self.rows = 0
        self.historical = 0

    def open(self, factory: VirtualFactory) -> None:
        self.factory = factory
        self.out.mkdir(parents=True, exist_ok=True)
        self.writer = pq.ParquetWriter(self.out / "readings.parquet", SCHEMA, compression=self.compression)

    def write(self, chunk: Chunk) -> None:
        if chunk.table.num_rows:
            self.writer.write_table(chunk.table)
            self.rows += chunk.table.num_rows
            self.historical += sum(chunk.table.column("historical").to_pylist())

    def close(self) -> None:
        self.writer.close()
        f = self.factory
        pq.write_table(f.ground_truth(), self.out / "ground_truth.parquet")
        meta = {
            "site": f.cfg.site,
            "seed": f.cfg.seed,
            "start": f.cfg.start.isoformat(),
            "duration_s": f.duration_s,
            "machines": len(f.cfg.machines),
            "sensors": len(f.sensors),
            "simulated_samples": sum(
                (f.duration_s // s.p.interval) for s in f.sensors if s.p.kind != "state"
            ),
            "published_readings": self.rows,
            "historical_readings": self.historical,
        }
        (self.out / "run.json").write_text(json.dumps(meta, indent=2))
        self.summary = meta
