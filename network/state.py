from __future__ import annotations

import threading
from copy import deepcopy
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class AggregateEntry:
    """Último valor conocido para una variable de un tópico/canal."""

    value: Any
    timestamp: float
    source_id: str
    msg_id: str
    metadata: dict[str, Any]


class LocalAggregateState:
    """Estado agregado local por tópico, canal y variable."""

    def __init__(self) -> None:
        self._state: dict[str, dict[str, dict[str, AggregateEntry]]] = {}
        self._lock = threading.Lock()

    @staticmethod
    def _resolve_key(channel: str, metadata: dict[str, Any]) -> str:
        pollutant = metadata.get("pollutant")
        if pollutant:
            return str(pollutant)

        crime_type = metadata.get("crime_type")
        if crime_type:
            return str(crime_type)

        if channel == "subjective":
            return "perception"

        return "value"

    def update(
        self,
        topic: str,
        channel: str,
        value: Any,
        timestamp: float,
        source_id: str,
        msg_id: str,
        metadata: dict[str, Any] | None = None,
    ) -> AggregateEntry:
        metadata = dict(metadata or {})
        key = self._resolve_key(channel=channel, metadata=metadata)

        entry = AggregateEntry(
            value=value,
            timestamp=float(timestamp),
            source_id=str(source_id),
            msg_id=str(msg_id),
            metadata=metadata,
        )

        with self._lock:
            topic_state = self._state.setdefault(topic, {})
            channel_state = topic_state.setdefault(channel, {})
            channel_state[key] = entry

        return entry

    def get(
        self,
        topic: str,
        channel: str,
        key: str | None = None,
    ) -> AggregateEntry | None:
        with self._lock:
            topic_state = self._state.get(topic)
            if topic_state is None:
                return None

            channel_state = topic_state.get(channel)
            if channel_state is None:
                return None

            if key is not None:
                return channel_state.get(key)

            if len(channel_state) == 1:
                return next(iter(channel_state.values()))

            return None

    def topic_state(self, topic: str) -> dict[str, dict[str, AggregateEntry]]:
        with self._lock:
            return deepcopy(self._state.get(topic, {}))

    def channel_state(self, topic: str, channel: str) -> dict[str, AggregateEntry]:
        with self._lock:
            return deepcopy(self._state.get(topic, {}).get(channel, {}))

    def snapshot(self) -> dict[str, dict[str, dict[str, AggregateEntry]]]:
        with self._lock:
            return deepcopy(self._state)
