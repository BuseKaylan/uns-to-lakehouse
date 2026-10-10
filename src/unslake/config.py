"""Load and validate the virtual factory definition (config/factory.yaml)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import yaml

KINDS = {"gauge", "counter", "state"}


@dataclass(frozen=True)
class SensorProfile:
    name: str
    kind: str
    unit: str = ""
    mean: float = 0.0
    sigma: float = 0.0
    theta: float = 0.1
    deadband: float = 0.0
    interval: int = 1
    max_silence: int = 60
    on_fault: Optional[float] = None
    trend_per_day: float = 0.0
    rate: float = 1.0
    min: Optional[float] = None
    max: Optional[float] = None


@dataclass(frozen=True)
class MachineDef:
    area: str
    line: str
    name: str
    type: str
    sensors: tuple[SensorProfile, ...]

    @property
    def path(self) -> str:
        return f"{self.area}/{self.line}/{self.name}"


@dataclass(frozen=True)
class RandomIntervals:
    """A Poisson process of events with log-normal durations."""

    rate_per_second: float
    median_seconds: float
    spread: float


@dataclass
class FactoryConfig:
    site: str
    start: datetime
    seed: int
    machines: list[MachineDef]
    faults: RandomIntervals
    frozen_sensors: RandomIntervals
    outages: RandomIntervals
    mqtt_host: str = "localhost"
    mqtt_port: int = 1883
    mqtt_qos: int = 0
    sparkplug_group: str = "acme-plant"
    profiles: dict[str, SensorProfile] = field(default_factory=dict)

    @property
    def lines(self) -> list[tuple[str, str]]:
        seen: list[tuple[str, str]] = []
        for m in self.machines:
            if (m.area, m.line) not in seen:
                seen.append((m.area, m.line))
        return seen

    @property
    def start_ms(self) -> int:
        return int(self.start.timestamp() * 1000)


def _intervals(d: dict, per: str) -> RandomIntervals:
    seconds = {"hour": 3600, "day": 86400}[per]
    return RandomIntervals(
        rate_per_second=float(d.get(f"rate_per_{per}", 0)) / seconds,
        median_seconds=float(d.get("median_seconds", 60)),
        spread=float(d.get("spread", 0.5)),
    )


def load_config(path: str | Path) -> FactoryConfig:
    raw = yaml.safe_load(Path(path).read_text())

    profiles: dict[str, SensorProfile] = {}
    for name, p in raw["sensor_profiles"].items():
        if p.get("kind") not in KINDS:
            raise ValueError(f"sensor profile {name!r}: kind must be one of {sorted(KINDS)}")
        profiles[name] = SensorProfile(name=name, **p)

    machines: list[MachineDef] = []
    for area, lines in raw["areas"].items():
        for line, machine_types in lines.items():
            counts: dict[str, int] = {}
            for mtype in machine_types:
                if mtype not in raw["machine_types"]:
                    raise ValueError(f"unknown machine type {mtype!r} on {area}/{line}")
                counts[mtype] = counts.get(mtype, 0) + 1
                # a second filler on the same line becomes filler-2
                name = mtype if counts[mtype] == 1 else f"{mtype}-{counts[mtype]}"
                sensors = tuple(profiles[s] for s in raw["machine_types"][mtype])
                machines.append(MachineDef(area, line, name, mtype, sensors))

    start = datetime.fromisoformat(str(raw["start"]).replace("Z", "+00:00"))
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)

    mqtt = raw.get("mqtt", {})
    return FactoryConfig(
        site=raw["site"],
        start=start,
        seed=int(raw.get("seed", 0)),
        machines=machines,
        faults=_intervals(raw.get("faults", {}), "hour"),
        frozen_sensors=_intervals(raw.get("frozen_sensors", {}), "day"),
        outages=_intervals(raw.get("outages", {}), "day"),
        mqtt_host=mqtt.get("host", "localhost"),
        mqtt_port=int(mqtt.get("port", 1883)),
        mqtt_qos=int(mqtt.get("qos", 0)),
        sparkplug_group=raw.get("sparkplug", {}).get("group_id", raw["site"]),
        profiles=profiles,
    )
