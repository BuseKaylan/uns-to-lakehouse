"""A small subscriber that prints what the virtual factory publishes.

For Sparkplug B it keeps the alias table from DBIRTH messages, which is exactly
the bookkeeping any consumer (including our future connector) has to do.
"""

from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

from . import sparkplug as sp


def _ts(ms) -> str:
    if ms is None:
        return "-"
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%H:%M:%S.%f")[:-3]


def listen(host: str, port: int, mode: str, topic: str | None, limit: int, seconds: float) -> int:
    aliases: dict[tuple[str, str, int], str] = {}   # (edge_node, device, alias) -> metric name
    seen = 0
    done = threading.Event()

    def on_message(_c, _u, msg):
        nonlocal seen
        if mode == "sparkplug":
            try:
                t = sp.parse_topic(msg.topic)
            except ValueError:
                return
            p = sp.decode(msg.payload)
            where = f"{t['edge_node']}/{t['device']}" if t["device"] else t["edge_node"]
            if t["type"] in ("NBIRTH", "NDEATH"):
                bd = next((m.value for m in p.metrics if m.name == "bdSeq"), None)
                print(f"{t['type']:<6} {where:<32} bdSeq={bd} seq={p.seq}")
            elif t["type"] == "DBIRTH":
                for m in p.metrics:
                    aliases[(t["edge_node"], t["device"], m.alias)] = m.name
                names = ", ".join(f"{m.name}={m.alias}" for m in p.metrics)
                print(f"DBIRTH {where:<32} seq={p.seq} aliases: {names}")
            elif t["type"] == "DDATA":
                parts = []
                for m in p.metrics:
                    name = aliases.get((t["edge_node"], t["device"], m.alias), f"alias#{m.alias}")
                    flag = " (historical)" if m.is_historical else ""
                    parts.append(f"{name}={m.value}@{_ts(m.timestamp)}{flag}")
                print(f"DDATA  {where:<32} seq={p.seq:<3} " + "  ".join(parts))
            else:
                print(f"{t['type']:<6} {where}")
        else:
            body = msg.payload.decode(errors="replace")
            try:
                d = json.loads(body)
                hist = " (historical)" if isinstance(d, dict) and d.get("historical") else ""
                if isinstance(d, dict) and "value" in d:
                    body = f"{d['value']} {d.get('unit', '')} @{_ts(d.get('timestamp'))}{hist}"
            except ValueError:
                pass
            print(f"{msg.topic:<55} {body}")
        seen += 1
        if limit and seen >= limit:
            done.set()

    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"unslake-listener-{int(time.time())}")
    c.on_message = on_message
    c.connect(host, port, keepalive=30)
    c.subscribe(topic or ("spBv1.0/#" if mode == "sparkplug" else "#"))
    c.loop_start()
    try:
        done.wait(seconds if seconds > 0 else None)
    except KeyboardInterrupt:
        pass
    finally:
        c.loop_stop()
        c.disconnect()
    return seen
