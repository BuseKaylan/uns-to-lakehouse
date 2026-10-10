"""The virtual factory simulation engine.

The engine runs on a one-second simulation grid and produces data in chunks
(default: 60 simulated seconds). Each chunk is a pyarrow Table with one row per
*published* reading, so report-by-exception, faults, frozen sensors and network
outages are already applied. Every run is fully determined by the config seed.

Output columns
--------------
event_ts      int64   ms since epoch, when the value was measured
delivered_ts  int64   ms since epoch, when the value reached the broker
                      (later than event_ts for data buffered during an outage)
site, area, line, machine, metric, unit   strings
value         float64 numeric value (null for state metrics)
value_str     string  state value (null for numeric metrics)
historical    bool    true for data delivered late after an outage
"""

from __future__ import annotations

import math
import zlib
from dataclasses import dataclass, field
from typing import Iterator, Optional

import numpy as np
import pyarrow as pa

from .config import FactoryConfig, MachineDef, RandomIntervals, SensorProfile

RUNNING, FAULT = 0, 1
STATE_NAMES = {RUNNING: "RUNNING", FAULT: "FAULT"}

SCHEMA = pa.schema([
    ("event_ts", pa.int64()),
    ("delivered_ts", pa.int64()),
    ("site", pa.string()),
    ("area", pa.string()),
    ("line", pa.string()),
    ("machine", pa.string()),
    ("metric", pa.string()),
    ("unit", pa.string()),
    ("value", pa.float64()),
    ("value_str", pa.string()),
    ("historical", pa.bool_()),
])


def rng_for(seed: int, *parts: str) -> np.random.Generator:
    """A random generator that depends only on the seed and a stable name."""
    key = zlib.crc32("/".join(parts).encode())
    return np.random.default_rng([seed, key])


def schedule(rng: np.random.Generator, proc: RandomIntervals, duration_s: int) -> list[tuple[int, int]]:
    """Non-overlapping [start, end) intervals in seconds from a Poisson process."""
    out: list[tuple[int, int]] = []
    if proc.rate_per_second <= 0:
        return out
    t = 0.0
    while True:
        t += rng.exponential(1.0 / proc.rate_per_second)
        if t >= duration_s:
            return out
        length = max(1, int(round(proc.median_seconds * math.exp(proc.spread * rng.standard_normal()))))
        start = int(t)
        if out and start <= out[-1][1]:
            start = out[-1][1] + 1
        end = min(start + length, duration_s)
        if start < end:
            out.append((start, end))
        t = end


def mask(intervals: list[tuple[int, int]], t0: int, n: int) -> np.ndarray:
    m = np.zeros(n, dtype=bool)
    for s, e in intervals:
        if e <= t0 or s >= t0 + n:
            continue
        m[max(s, t0) - t0:min(e, t0 + n) - t0] = True
    return m


@dataclass
class Event:
    """Edge node connectivity changes, published alongside the data."""

    ts: int           # ms since epoch
    kind: str         # "edge_offline" | "edge_online"
    area: str
    line: str


class SensorSim:
    """Simulates one sensor on one machine, including report-by-exception."""

    def __init__(self, machine: MachineDef, profile: SensorProfile, seed: int, frozen: list[tuple[int, int]]):
        self.p = profile
        self.machine = machine
        self.rng = rng_for(seed, machine.path, profile.name)
        self.frozen = frozen
        self.x = profile.mean
        self.count = 0
        self.last_value: Optional[float | str] = None
        self.last_t: Optional[int] = None

    def _should_emit(self, t: int, v) -> bool:
        if self.last_t is None:
            return True
        if t - self.last_t >= self.p.max_silence:
            return True
        if isinstance(v, str):
            return v != self.last_value
        return abs(v - self.last_value) >= max(self.p.deadband, 1e-12)

    def run(self, t0: int, states: np.ndarray) -> tuple[list[int], list]:
        """Simulate seconds [t0, t0+len(states)). Returns emitted (t, value) pairs."""
        p, n = self.p, len(states)
        ts: list[int] = []
        vals: list = []
        if p.kind == "state":
            for i in range(n):
                v = STATE_NAMES[int(states[i])]
                if self.last_value is None or v != self.last_value:
                    ts.append(t0 + i); vals.append(v)
                    self.last_value, self.last_t = v, t0 + i
            return ts, vals

        first = (-t0) % p.interval
        idx = range(first, n, p.interval)
        frozen = mask(self.frozen, t0, n)
        noise = self.rng.standard_normal(len(idx))
        dt = p.interval
        for k, i in enumerate(idx):
            t = t0 + i
            running = states[i] == RUNNING
            if p.kind == "gauge":
                if not frozen[i]:
                    if not running and p.on_fault is not None:
                        self.x += 0.3 * (p.on_fault - self.x) + 0.3 * p.sigma * noise[k]
                    else:
                        target = p.mean + p.trend_per_day * t / 86400.0
                        self.x += min(p.theta * dt, 1.0) * (target - self.x) + p.sigma * math.sqrt(dt) * noise[k]
                    if p.min is not None:
                        self.x = max(self.x, p.min)
                    if p.max is not None:
                        self.x = min(self.x, p.max)
                v = round(self.x, 3)
            else:  # counter
                if running and not frozen[i]:
                    self.count += int(self.rng.poisson(p.rate * dt))
                v = float(self.count)
            if self._should_emit(t, v):
                ts.append(t); vals.append(v)
                self.last_value, self.last_t = v, t
        return ts, vals


@dataclass
class Chunk:
    t0: int                      # first simulated second in this chunk
    n: int                       # number of simulated seconds
    table: pa.Table
    events: list[Event] = field(default_factory=list)


class VirtualFactory:
    def __init__(self, cfg: FactoryConfig, duration_s: int):
        self.cfg = cfg
        self.duration_s = int(duration_s)
        seed = cfg.seed
        self.fault_schedule = {
            m.path: schedule(rng_for(seed, "faults", m.path), cfg.faults, self.duration_s) for m in cfg.machines
        }
        self.outage_schedule = {
            (a, l): schedule(rng_for(seed, "outages", a, l), cfg.outages, self.duration_s) for a, l in cfg.lines
        }
        self.sensors: list[SensorSim] = []
        self.frozen_schedule: dict[tuple[str, str], list[tuple[int, int]]] = {}
        for m in cfg.machines:
            for prof in m.sensors:
                frozen = []
                if prof.kind != "state":
                    frozen = schedule(rng_for(seed, "frozen", m.path, prof.name), cfg.frozen_sensors, self.duration_s)
                self.frozen_schedule[(m.path, prof.name)] = frozen
                self.sensors.append(SensorSim(m, prof, seed, frozen))

    # ------------------------------------------------------------ helpers
    def ms(self, t: float) -> int:
        return self.cfg.start_ms + int(round(t * 1000))

    def ground_truth(self) -> pa.Table:
        """What really happened, for measuring the accuracy of later experiments."""
        rows = []
        for path, iv in self.fault_schedule.items():
            for s, e in iv:
                rows.append(("machine_fault", path, None, self.ms(s), self.ms(e)))
        for (path, metric), iv in self.frozen_schedule.items():
            for s, e in iv:
                rows.append(("frozen_sensor", path, metric, self.ms(s), self.ms(e)))
        for (a, l), iv in self.outage_schedule.items():
            for s, e in iv:
                rows.append(("edge_outage", f"{a}/{l}", None, self.ms(s), self.ms(e)))
        cols = list(zip(*rows)) if rows else [[], [], [], [], []]
        return pa.table({
            "kind": pa.array(cols[0], pa.string()),
            "path": pa.array(cols[1], pa.string()),
            "metric": pa.array(cols[2], pa.string()),
            "start_ts": pa.array(cols[3], pa.int64()),
            "end_ts": pa.array(cols[4], pa.int64()),
        })

    def _delivery(self, area: str, line: str, t: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        """Store-and-forward: readings taken during an outage arrive when it ends."""
        event_ms = self.cfg.start_ms + t * 1000
        delivered = event_ms.copy()
        historical = np.zeros(len(t), dtype=bool)
        for s, e in self.outage_schedule[(area, line)]:
            inside = (t >= s) & (t < e)
            if inside.any():
                historical |= inside
                # flushed right after reconnect, keeping the original order
                order = np.cumsum(inside) - 1
                delivered[inside] = self.ms(e) + 1 + order[inside]
        return delivered, historical

    # ------------------------------------------------------------ main loop
    def chunks(self, chunk_s: int = 60) -> Iterator[Chunk]:
        cfg = self.cfg
        for t0 in range(0, self.duration_s, chunk_s):
            n = min(chunk_s, self.duration_s - t0)
            states = {
                m.path: np.where(mask(self.fault_schedule[m.path], t0, n), FAULT, RUNNING).astype(np.int8)
                for m in cfg.machines
            }
            cols: dict[str, list] = {k: [] for k in SCHEMA.names}
            for s in self.sensors:
                m = s.machine
                ts, vals = s.run(t0, states[m.path])
                if not ts:
                    continue
                t = np.asarray(ts, dtype=np.int64)
                delivered, hist = self._delivery(m.area, m.line, t)
                k = len(ts)
                cols["event_ts"].append(cfg.start_ms + t * 1000)
                cols["delivered_ts"].append(delivered)
                cols["historical"].append(hist)
                for name, v in (("site", cfg.site), ("area", m.area), ("line", m.line),
                                ("machine", m.name), ("metric", s.p.name), ("unit", s.p.unit)):
                    cols[name].append(np.full(k, v, dtype=object))
                if s.p.kind == "state":
                    cols["value"].append(np.full(k, np.nan))
                    cols["value_str"].append(np.asarray(vals, dtype=object))
                else:
                    cols["value"].append(np.asarray(vals, dtype=np.float64))
                    cols["value_str"].append(np.full(k, None, dtype=object))
            if cols["event_ts"]:
                arrays = {k: np.concatenate(v) for k, v in cols.items()}
                order = np.argsort(arrays["event_ts"], kind="stable")
                # from_pandas=True turns the NaN placeholders of state rows into proper nulls
                table = pa.table({k: pa.array(v[order], type=SCHEMA.field(k).type, from_pandas=True)
                                  for k, v in arrays.items()}, schema=SCHEMA)
            else:
                table = SCHEMA.empty_table()
            events = []
            for (a, l), iv in self.outage_schedule.items():
                for s_, e_ in iv:
                    if t0 <= s_ < t0 + n:
                        events.append(Event(self.ms(s_), "edge_offline", a, l))
                    if t0 <= e_ < t0 + n:
                        events.append(Event(self.ms(e_), "edge_online", a, l))
            yield Chunk(t0, n, table, sorted(events, key=lambda ev: ev.ts))
