# Experiments

Every experiment runs on data from the same virtual factory (`config/factory.yaml`, seed 42),
writes its raw results to `benchmarks/results/<experiment>/` as CSV, JSON and PNG charts, and
records the environment it ran on. Timings are the median of several runs.

Results are committed to the repository, so every number in the blog series links back to
the exact run that produced it.

| # | Question | Script | Blog post | Status |
|---|---|---|---|---|
| 01 | How much space does sensor data really take? | `storage_footprint.py` | Part 3 | ready |
| 02 | Narrow vs. wide vs. hybrid tables in Iceberg | | Part 4 | planned |
| 03 | Partitioning and sort order | | Part 5 | planned |
| 04 | Commit interval and the small files problem | | Part 6 | planned |
| 05 | Throughput limits of a Python ingestion pipeline | | Part 7 | planned |
| 06 | Ignoring Sparkplug state: how wrong does uptime get? | | Part 8 | planned |

## 01 Storage footprint

```bash
make bench-storage
# or with options
.venv/bin/python benchmarks/storage_footprint.py --duration 7d --repeats 5
```

Writes one simulated day in eleven formats (JSON lines, CSV, Parquet with different compression,
sort orders and encodings) and answers three questions:

| Output | Question |
|---|---|
| `storage_footprint.csv`, `bytes_per_reading.png` | How big is each format, per reading and per year? |
| `bytes_by_column.csv`, `bytes_by_column.png` | Which columns take the space in each Parquet variant? |
| `report_by_exception.csv`, `report_by_exception.png` | How much does report-by-exception save, compared with storing every sample? |

Takes about two minutes on a laptop. Add `--skip-rbe` to skip the every-sample comparison.

## Ground rules

- Same config and seed for every experiment unless the experiment is about changing them.
- Report the hardware and library versions (captured automatically in the JSON output).
- At least three runs per measurement; report the median.
- Publish the raw results next to the blog post, including the inconvenient ones.
- Write down the limitations: this is simulated data on one machine, not a real plant.
