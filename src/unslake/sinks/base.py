"""Sink interface, the run loop, and delivery-time ordering for live MQTT sinks."""

from __future__ import annotations

import heapq
import itertools
import time
from typing import Iterator, Optional

from ..sim import Chunk, Event, VirtualFactory


class Sink:
    def open(self, factory: VirtualFactory) -> None: ...
    def write(self, chunk: Chunk) -> None: ...
    def close(self) -> None: ...


def run(factory: VirtualFactory, sink: Sink, chunk_s: int = 60, progress=None) -> None:
    sink.open(factory)
    try:
        for chunk in factory.chunks(chunk_s):
            sink.write(chunk)
            if progress:
                progress(chunk)
    finally:
        sink.close()


class Clock:
    """Maps simulated milliseconds to wall-clock time. speed=0 means as fast as possible."""

    def __init__(self, sim_start_ms: int, speed: float):
        self.sim_start_ms = sim_start_ms
        self.speed = speed
        self.wall_start = time.monotonic()

    def wait_until(self, sim_ms: int) -> None:
        if self.speed <= 0:
            return
        target = self.wall_start + (sim_ms - self.sim_start_ms) / 1000.0 / self.speed
        delay = target - time.monotonic()
        if delay > 0:
            time.sleep(delay)


class DeliveryQueue:
    """Releases readings and events in delivery order across chunk boundaries.

    Readings buffered during an outage are delivered after the outage ends, which can
    be in a later chunk. Items are released once the simulation has passed their
    delivery time. Events sort before readings with the same timestamp, so an edge
    node reconnects before its buffered data is flushed.
    """

    def __init__(self):
        self._heap: list = []
        self._seq = itertools.count()

    def push_chunk(self, chunk: Chunk) -> None:
        for ev in chunk.events:
            heapq.heappush(self._heap, (ev.ts, 0, next(self._seq), ev))
        for row in chunk.table.to_pylist():
            heapq.heappush(self._heap, (row["delivered_ts"], 1, next(self._seq), row))

    def pop_until(self, ts_ms: Optional[int]) -> Iterator[tuple[int, object]]:
        while self._heap and (ts_ms is None or self._heap[0][0] < ts_ms):
            t, _, _, item = heapq.heappop(self._heap)
            yield t, item


def is_event(item) -> bool:
    return isinstance(item, Event)
