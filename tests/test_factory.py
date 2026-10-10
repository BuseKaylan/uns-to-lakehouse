from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pytest

from unslake import sparkplug as sp
from unslake.config import load_config
from unslake.sim import VirtualFactory

CONFIG = Path(__file__).parents[1] / "config" / "factory.yaml"


def simulate(seconds=3600, seed=None):
    cfg = load_config(CONFIG)
    if seed is not None:
        cfg.seed = seed
    f = VirtualFactory(cfg, seconds)
    return f, pa.concat_tables([c.table for c in f.chunks()])


def test_hierarchy_loads():
    cfg = load_config(CONFIG)
    assert cfg.site == "acme-plant"
    assert len(cfg.machines) == 8
    assert ("filling-area", "line-2") in cfg.lines


def test_same_seed_same_data():
    _, a = simulate(1800)
    _, b = simulate(1800)
    assert a.equals(b)


def test_different_seed_different_data():
    _, a = simulate(600, seed=1)
    _, b = simulate(600, seed=2)
    assert not a.equals(b)


def test_report_by_exception_publishes_less_than_sampled():
    f, t = simulate(3600)
    sampled = sum(3600 // s.p.interval for s in f.sensors if s.p.kind != "state")
    assert 0 < t.num_rows < sampled


def test_heartbeat_limits_silence():
    _, t = simulate(3600)
    d = t.filter(pc.and_(pc.equal(t["machine"], "filler"), pc.equal(t["line"], "line-1")))
    d = d.filter(pc.equal(d["metric"], "temperature"))
    ts = np.sort(d["event_ts"].to_numpy())
    assert np.diff(ts).max() <= 300 * 1000   # max_silence for temperature


def test_counters_never_decrease():
    _, t = simulate(3600)
    c = t.filter(pc.equal(t["metric"], "count")).to_pylist()
    last = {}
    for r in sorted(c, key=lambda r: r["event_ts"]):
        key = (r["line"], r["machine"])
        assert r["value"] >= last.get(key, 0)
        last[key] = r["value"]


def test_physical_limits_respected():
    _, t = simulate(6 * 3600)
    speed = t.filter(pc.equal(t["metric"], "speed"))["value"].to_numpy()
    assert speed.min() >= 0


def test_outage_data_is_delivered_late_and_flagged():
    f, t = simulate(86400)
    hist = t.filter(t["historical"])
    assert hist.num_rows > 0
    assert (hist["delivered_ts"].to_numpy() > hist["event_ts"].to_numpy()).all()
    live = t.filter(pc.invert(t["historical"]))
    assert (live["delivered_ts"].to_numpy() == live["event_ts"].to_numpy()).all()


def test_ground_truth_matches_state_metric():
    f, t = simulate(86400)
    gt = f.ground_truth().filter(pc.equal(f.ground_truth()["kind"], "machine_fault"))
    states = t.filter(pc.equal(t["metric"], "state"))
    faults = states.filter(pc.equal(states["value_str"], "FAULT"))
    assert faults.num_rows == gt.num_rows


@pytest.mark.parametrize("value,datatype", [
    (812.375, sp.DOUBLE), (15204, sp.UINT64), ("RUNNING", sp.STRING), (True, sp.BOOLEAN), (-5, sp.INT64),
])
def test_sparkplug_roundtrip(value, datatype):
    p = sp.Payload(timestamp=1767592800000, seq=255, metrics=[
        sp.Metric(name="pressure", alias=3, timestamp=1767592800123, datatype=datatype,
                  value=value, is_historical=True),
        sp.Metric(alias=4, datatype=sp.DOUBLE, value=None),
    ])
    q = sp.decode(sp.encode(p))
    assert q.timestamp == p.timestamp and q.seq == 255
    m = q.metrics[0]
    assert (m.name, m.alias, m.timestamp, m.datatype, m.is_historical) == ("pressure", 3, 1767592800123, datatype, True)
    assert m.value == value
    assert q.metrics[1].is_null


def test_sparkplug_topic():
    t = sp.topic("acme-plant", "DDATA", "filling-area.line-2", "filler")
    assert t == "spBv1.0/acme-plant/DDATA/filling-area.line-2/filler"
    assert sp.parse_topic(t)["device"] == "filler"
