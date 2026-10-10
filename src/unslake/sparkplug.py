"""A small, dependency-free Sparkplug B payload codec.

Sparkplug B payloads are Protocol Buffers messages. Instead of pulling in generated
protobuf code, this module encodes and decodes the subset of the Eclipse Sparkplug B
schema the series needs: Payload(timestamp, metrics, seq) and Metric(name, alias,
timestamp, datatype, is_historical, is_null and the scalar value fields).

Field numbers follow the official sparkplug_b.proto:

    message Payload { uint64 timestamp = 1; repeated Metric metrics = 2; uint64 seq = 3; ... }
    message Metric  { string name = 1; uint64 alias = 2; uint64 timestamp = 3;
                      uint32 datatype = 4; bool is_historical = 5; bool is_transient = 6;
                      bool is_null = 7; ...
                      oneof value { uint32 int_value = 10; uint64 long_value = 11;
                                    float float_value = 12; double double_value = 13;
                                    bool boolean_value = 14; string string_value = 15; ... } }
"""

from __future__ import annotations

import struct
from dataclasses import dataclass, field
from typing import Optional, Union

NAMESPACE = "spBv1.0"

# Sparkplug B data types (subset)
INT32, INT64, UINT32, UINT64 = 3, 4, 7, 8
FLOAT, DOUBLE, BOOLEAN, STRING = 9, 10, 11, 12

Value = Union[int, float, bool, str, None]


@dataclass
class Metric:
    name: Optional[str] = None
    alias: Optional[int] = None
    timestamp: Optional[int] = None
    datatype: Optional[int] = None
    value: Value = None
    is_historical: bool = False
    is_null: bool = False


@dataclass
class Payload:
    timestamp: Optional[int] = None
    metrics: list[Metric] = field(default_factory=list)
    seq: Optional[int] = None


def topic(group: str, msg_type: str, edge_node: str, device: Optional[str] = None) -> str:
    parts = [NAMESPACE, group, msg_type, edge_node] + ([device] if device else [])
    return "/".join(parts)


def parse_topic(t: str) -> dict:
    parts = t.split("/")
    if len(parts) < 4 or parts[0] != NAMESPACE:
        raise ValueError(f"not a Sparkplug B topic: {t}")
    return {
        "group": parts[1],
        "type": parts[2],
        "edge_node": parts[3],
        "device": parts[4] if len(parts) > 4 else None,
    }


# ---------------------------------------------------------------- encoding

def _varint(n: int) -> bytes:
    if n < 0:
        n += 1 << 64
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def _key(num: int, wire: int) -> bytes:
    return _varint((num << 3) | wire)


def _len_field(num: int, data: bytes) -> bytes:
    return _key(num, 2) + _varint(len(data)) + data


def encode_metric(m: Metric) -> bytes:
    out = bytearray()
    if m.name is not None:
        out += _len_field(1, m.name.encode())
    if m.alias is not None:
        out += _key(2, 0) + _varint(m.alias)
    if m.timestamp is not None:
        out += _key(3, 0) + _varint(m.timestamp)
    if m.datatype is not None:
        out += _key(4, 0) + _varint(m.datatype)
    if m.is_historical:
        out += _key(5, 0) + _varint(1)
    if m.is_null or m.value is None:
        out += _key(7, 0) + _varint(1)
        return bytes(out)
    dt = m.datatype
    if dt in (INT32, UINT32):
        out += _key(10, 0) + _varint(int(m.value) & 0xFFFFFFFF)
    elif dt in (INT64, UINT64):
        out += _key(11, 0) + _varint(int(m.value))
    elif dt == FLOAT:
        out += _key(12, 5) + struct.pack("<f", float(m.value))
    elif dt == DOUBLE:
        out += _key(13, 1) + struct.pack("<d", float(m.value))
    elif dt == BOOLEAN:
        out += _key(14, 0) + _varint(1 if m.value else 0)
    elif dt == STRING:
        out += _len_field(15, str(m.value).encode())
    else:
        raise ValueError(f"unsupported datatype {dt}")
    return bytes(out)


def encode(p: Payload) -> bytes:
    out = bytearray()
    if p.timestamp is not None:
        out += _key(1, 0) + _varint(p.timestamp)
    for m in p.metrics:
        out += _len_field(2, encode_metric(m))
    if p.seq is not None:
        out += _key(3, 0) + _varint(p.seq)
    return bytes(out)


# ---------------------------------------------------------------- decoding

def _read_varint(buf: bytes, i: int) -> tuple[int, int]:
    shift = result = 0
    while True:
        b = buf[i]
        i += 1
        result |= (b & 0x7F) << shift
        if not b & 0x80:
            return result, i
        shift += 7


def _fields(buf: bytes):
    i = 0
    while i < len(buf):
        key, i = _read_varint(buf, i)
        num, wire = key >> 3, key & 7
        if wire == 0:
            val, i = _read_varint(buf, i)
        elif wire == 1:
            val, i = buf[i:i + 8], i + 8
        elif wire == 5:
            val, i = buf[i:i + 4], i + 4
        elif wire == 2:
            n, i = _read_varint(buf, i)
            val, i = buf[i:i + n], i + n
        else:
            raise ValueError(f"unsupported wire type {wire}")
        yield num, wire, val


def decode_metric(buf: bytes) -> Metric:
    m = Metric()
    for num, _wire, val in _fields(buf):
        if num == 1:
            m.name = val.decode()
        elif num == 2:
            m.alias = val
        elif num == 3:
            m.timestamp = val
        elif num == 4:
            m.datatype = val
        elif num == 5:
            m.is_historical = bool(val)
        elif num == 7:
            m.is_null = bool(val)
        elif num == 10:
            m.value = val - (1 << 32) if m.datatype == INT32 and val >= 1 << 31 else val
        elif num == 11:
            m.value = val - (1 << 64) if m.datatype == INT64 and val >= 1 << 63 else val
        elif num == 12:
            m.value = struct.unpack("<f", val)[0]
        elif num == 13:
            m.value = struct.unpack("<d", val)[0]
        elif num == 14:
            m.value = bool(val)
        elif num == 15:
            m.value = val.decode()
        # other fields (metadata, properties, datasets, ...) are skipped
    return m


def decode(buf: bytes) -> Payload:
    p = Payload()
    for num, _wire, val in _fields(buf):
        if num == 1:
            p.timestamp = val
        elif num == 2:
            p.metrics.append(decode_metric(val))
        elif num == 3:
            p.seq = val
    return p
