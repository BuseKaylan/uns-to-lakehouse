"""Publish the virtual factory as a plain-MQTT Unified Namespace with JSON payloads.

Topic:   <site>/<area>/<line>/<machine>/<metric>
Payload: {"value": 812.4, "unit": "mbar", "timestamp": 1767592800000}
         "historical": true is added for data delivered late after an outage.

Each line also has a retained status topic, mimicking an edge gateway's last will:
         <site>/<area>/<line>/_edge/status  ->  "online" | "offline"
"""

from __future__ import annotations

import json

import paho.mqtt.client as mqtt

from ..sim import Chunk, VirtualFactory
from .base import Clock, DeliveryQueue, Sink, is_event


class UnsJsonSink(Sink):
    def __init__(self, host: str, port: int, qos: int = 0, speed: float = 1.0):
        self.host, self.port, self.qos, self.speed = host, port, qos, speed
        self.queue = DeliveryQueue()
        self.messages = 0

    def open(self, factory: VirtualFactory) -> None:
        self.factory = factory
        self.site = factory.cfg.site
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"{self.site}-uns-sim")
        self.client.connect(self.host, self.port, keepalive=30)
        self.client.loop_start()
        self.clock = Clock(factory.cfg.start_ms, self.speed)
        for area, line in factory.cfg.lines:
            self._status(area, line, "online")

    def _status(self, area: str, line: str, status: str) -> None:
        self.client.publish(f"{self.site}/{area}/{line}/_edge/status", status, qos=1, retain=True)

    def _release(self, until_ms):
        for t, item in self.queue.pop_until(until_ms):
            self.clock.wait_until(t)
            if is_event(item):
                self._status(item.area, item.line, "offline" if item.kind == "edge_offline" else "online")
                continue
            r = item
            body = {
                "value": r["value_str"] if r["value_str"] is not None else r["value"],
                "unit": r["unit"],
                "timestamp": r["event_ts"],
            }
            if r["historical"]:
                body["historical"] = True
            topic = f"{r['site']}/{r['area']}/{r['line']}/{r['machine']}/{r['metric']}"
            self.client.publish(topic, json.dumps(body), qos=self.qos)
            self.messages += 1

    def write(self, chunk: Chunk) -> None:
        self.queue.push_chunk(chunk)
        self._release(self.factory.ms(chunk.t0 + chunk.n))

    def close(self) -> None:
        self._release(None)
        self.client.loop_stop()
        self.client.disconnect()
