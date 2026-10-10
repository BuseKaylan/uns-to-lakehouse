"""Command line interface: `unslake info | simulate | listen`."""

from __future__ import annotations

import argparse
import re
import sys
import time

from .config import load_config
from .sim import VirtualFactory
from .sinks import ParquetSink, run

UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def parse_duration(text: str) -> int:
    m = re.fullmatch(r"(\d+(?:\.\d+)?)([smhd])", text.strip())
    if not m:
        raise argparse.ArgumentTypeError("use a number with a unit, e.g. 90s, 15m, 6h, 1d")
    return int(float(m.group(1)) * UNITS[m.group(2)])


def cmd_info(args) -> None:
    cfg = load_config(args.config)
    print(f"site: {cfg.site}   start: {cfg.start.isoformat()}   seed: {cfg.seed}")
    current = None
    for m in cfg.machines:
        if (m.area, m.line) != current:
            current = (m.area, m.line)
            print(f"  {m.area}/{m.line}")
        print(f"    {m.name:<12} {', '.join(s.name for s in m.sensors)}")
    n = sum(len(m.sensors) for m in cfg.machines)
    print(f"{len(cfg.machines)} machines, {n} signals")


def cmd_simulate(args) -> None:
    cfg = load_config(args.config)
    if args.seed is not None:
        cfg.seed = args.seed
    host = args.host or cfg.mqtt_host
    port = args.port or cfg.mqtt_port
    factory = VirtualFactory(cfg, args.duration)

    if args.mode == "file":
        sink = ParquetSink(args.out)
    elif args.mode == "mqtt":
        from .sinks.mqtt_json import UnsJsonSink
        sink = UnsJsonSink(host, port, cfg.mqtt_qos, args.speed)
    else:
        from .sinks.sparkplug_sink import SparkplugSink
        sink = SparkplugSink(host, port, cfg.mqtt_qos, args.speed)

    started = time.monotonic()
    last_report = [started]

    def progress(chunk):
        now = time.monotonic()
        if now - last_report[0] >= 5 or chunk.t0 + chunk.n >= factory.duration_s:
            last_report[0] = now
            pct = 100 * (chunk.t0 + chunk.n) / factory.duration_s
            print(f"  simulated {chunk.t0 + chunk.n:>7}s of {factory.duration_s}s ({pct:5.1f}%)", file=sys.stderr)

    print(f"simulating {args.duration}s of {cfg.site} -> {args.mode}", file=sys.stderr)
    run(factory, sink, progress=progress)
    took = time.monotonic() - started
    if args.mode == "file":
        s = sink.summary
        print(f"done in {took:.1f}s: {s['published_readings']:,} readings "
              f"({s['historical_readings']:,} delivered late) from {s['simulated_samples']:,} samples -> {args.out}")
    else:
        print(f"done in {took:.1f}s: {sink.messages:,} MQTT messages published to {host}:{port}")


def cmd_listen(args) -> None:
    from .listen import listen
    cfg = load_config(args.config)
    n = listen(args.host or cfg.mqtt_host, args.port or cfg.mqtt_port, args.mode, args.topic, args.limit, args.seconds)
    print(f"-- {n} messages", file=sys.stderr)


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(prog="unslake", description="Virtual factory for the UNS to Lakehouse series.")
    ap.add_argument("--config", default="config/factory.yaml")
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("info", help="show the plant hierarchy").set_defaults(func=cmd_info)

    s = sub.add_parser("simulate", help="run the virtual factory")
    s.add_argument("--mode", choices=["file", "mqtt", "sparkplug"], default="file",
                   help="file: Parquet dataset; mqtt: UNS with JSON payloads; sparkplug: Sparkplug B")
    s.add_argument("--duration", type=parse_duration, default=parse_duration("1h"))
    s.add_argument("--speed", type=float, default=1.0,
                   help="live modes: 1 = real time, 60 = one simulated minute per second, 0 = no pacing")
    s.add_argument("--out", default="data/run", help="file mode: output directory")
    s.add_argument("--seed", type=int, help="override the config seed")
    s.add_argument("--host")
    s.add_argument("--port", type=int)
    s.set_defaults(func=cmd_simulate)

    l = sub.add_parser("listen", help="print what the factory publishes")
    l.add_argument("--mode", choices=["mqtt", "sparkplug"], default="sparkplug")
    l.add_argument("--topic", help="MQTT topic filter (default: everything for the mode)")
    l.add_argument("--limit", type=int, default=0, help="stop after N messages")
    l.add_argument("--seconds", type=float, default=0, help="stop after N seconds")
    l.add_argument("--host")
    l.add_argument("--port", type=int)
    l.set_defaults(func=cmd_listen)

    args = ap.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
