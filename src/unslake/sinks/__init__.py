"""Where the virtual factory sends its data: Parquet files, plain MQTT (UNS JSON) or Sparkplug B."""

from .base import Sink, run
from .parquet import ParquetSink

__all__ = ["Sink", "run", "ParquetSink"]
