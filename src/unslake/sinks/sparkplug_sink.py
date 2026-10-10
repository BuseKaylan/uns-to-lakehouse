"""Publish the virtual factory over MQTT using Sparkplug B.

Mapping
-------
group_id   = config sparkplug.group_id (default: the site)
edge node  = one per line, id "<area>.<line>"      e.g. filling-area.line-2
device     = one per machine                         e.g. filler

Behaviour that matters for the experiments in the series:
* NBIRTH carries bdSeq; the matching NDEATH is registered as the MQTT last will.
* DBIRTH announces every metric with its name, alias and datatype.
* DDATA messages carry aliases only, so consumers must remember the births.
* seq counts 0..255 per edge node and wraps.
* During a simulated network outage the edge node "dies" (NDEATH), buffers its data,
  then reconnects with bdSeq + 1, re-births and flushes the buffer as historical metrics.
"""

from __future__ import annotations

import paho.mqtt.client as mqtt

from .. import sparkplug as sp
from ..sim import Chunk, VirtualFactory
from .base import Clock, DeliveryQueue, Sink, is_event

DATATYPE = {"gauge": sp.DOUBLE, "counter": sp.UINT64, "state": sp.STRING}


class EdgeNode:
    def __init__(self, sink: "SparkplugSink", area: str, line: str):
        self.sink = sink
        self.area, self.line = area, line
        self.node_id = f"{area}.{line}"
        self.bd_seq = 0
        self.seq = 0
        self.client = None
        self.aliases: dict[tuple[str, str], int] = {}
        self.datatypes: dict[tuple[str, str], int] = {}
        self.last: dict[tuple[str, str], object] = {}
        self.devices: list[str] = []
        for m in sink.factory.cfg.machines:
            if (m.area, m.line) != (area, line):
                continue
            self.devices.append(m.name)
            for p in m.sensors:
                key = (m.name, p.name)
                self.aliases[key] = len(self.aliases) + 1
                self.datatypes[key] = DATATYPE[p.kind]
                self.last[key] = None

    def _next_seq(self) -> int:
        s = self.seq
        self.seq = (self.seq + 1) % 256
        return s

    def _death_payload(self) -> bytes:
        return sp.encode(sp.Payload(timestamp=self.sink.now_ms,
                                    metrics=[sp.Metric(name="bdSeq", datatype=sp.INT64, value=self.bd_seq)]))

    def connect(self) -> None:
        g = self.sink.group
        c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"sim-{self.node_id}")
        c.will_set(sp.topic(g, "NDEATH", self.node_id), self._death_payload(), qos=1, retain=False)
        c.connect(self.sink.host, self.sink.port, keepalive=30)
        c.loop_start()
        self.client = c
        self.seq = 0
        now = self.sink.now_ms
        nbirth = sp.Payload(timestamp=now, seq=self._next_seq(), metrics=[
            sp.Metric(name="bdSeq", datatype=sp.INT64, value=self.bd_seq, timestamp=now),
            sp.Metric(name="Node Control/Rebirth", datatype=sp.BOOLEAN, value=False, timestamp=now),
        ])
        self._pub("NBIRTH", None, nbirth, qos=0)
        for dev in self.devices:
            metrics = [
                sp.Metric(name=metric, alias=self.aliases[(d, metric)], datatype=self.datatypes[(d, metric)],
                          value=self.last[(d, metric)], timestamp=now)
                for (d, metric) in self.aliases if d == dev
            ]
            self._pub("DBIRTH", dev, sp.Payload(timestamp=now, seq=self._next_seq(), metrics=metrics), qos=0)

    def die(self) -> None:
        """Simulate a lost connection: the broker would publish our last will (NDEATH)."""
        info = self._pub("NDEATH", None, sp.Payload(timestamp=self.sink.now_ms, metrics=[
            sp.Metric(name="bdSeq", datatype=sp.INT64, value=self.bd_seq)]), qos=1)
        info.wait_for_publish(timeout=5)     # make sure the death certificate leaves before we drop
        self.client.loop_stop()
        self.client.disconnect()
        self.client = None
        self.bd_seq = (self.bd_seq + 1) % 256

    def ddata(self, device: str, rows: list[dict], ts: int) -> None:
        metrics = []
        for r in rows:
            key = (device, r["metric"])
            value = r["value_str"] if r["value_str"] is not None else r["value"]
            if self.datatypes[key] == sp.UINT64:
                value = int(value)
            self.last[key] = value
            metrics.append(sp.Metric(alias=self.aliases[key], timestamp=r["event_ts"],
                                     datatype=self.datatypes[key], value=value, is_historical=r["historical"]))
        self._pub("DDATA", device, sp.Payload(timestamp=ts, seq=self._next_seq(), metrics=metrics),
                  qos=self.sink.qos)

    def _pub(self, msg_type: str, device, payload: sp.Payload, qos: int):
        info = self.client.publish(sp.topic(self.sink.group, msg_type, self.node_id, device),
                                   sp.encode(payload), qos=qos)
        self.sink.messages += 1
        return info


class SparkplugSink(Sink):
    def __init__(self, host: str, port: int, qos: int = 0, speed: float = 1.0):
        self.host, self.port, self.qos, self.speed = host, port, qos, speed
        self.queue = DeliveryQueue()
        self.messages = 0
        self.now_ms = 0

    def open(self, factory: VirtualFactory) -> None:
        self.factory = factory
        self.group = factory.cfg.sparkplug_group
        self.now_ms = factory.cfg.start_ms
        self.clock = Clock(factory.cfg.start_ms, self.speed)
        self.nodes = {(a, l): EdgeNode(self, a, l) for a, l in factory.cfg.lines}
        for node in self.nodes.values():
            node.connect()

    def _flush(self, pending: dict, ts: int) -> None:
        for (area, line, device), rows in pending.items():
            node = self.nodes[(area, line)]
            if node.client is None:          # outage ended after the run's last chunk
                node.connect()
            node.ddata(device, rows, ts)
        pending.clear()

    def _release(self, until_ms):
        pending: dict[tuple[str, str, str], list[dict]] = {}
        current = None
        for t, item in self.queue.pop_until(until_ms):
            if current is not None and t != current:
                self._flush(pending, current)
            current = t
            self.clock.wait_until(t)
            self.now_ms = t
            if is_event(item):
                self._flush(pending, t)
                node = self.nodes[(item.area, item.line)]
                node.die() if item.kind == "edge_offline" else node.connect()
                continue
            pending.setdefault((item["area"], item["line"], item["machine"]), []).append(item)
        if pending:
            self._flush(pending, current)

    def write(self, chunk: Chunk) -> None:
        self.queue.push_chunk(chunk)
        self._release(self.factory.ms(chunk.t0 + chunk.n))

    def close(self) -> None:
        self._release(None)
        for node in self.nodes.values():
            if node.client is not None:
                node.die()                   # graceful shutdown: publish NDEATH, then disconnect
