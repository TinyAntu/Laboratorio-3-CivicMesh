from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any
import json


@dataclass
class PeerInfo:
    node_id: str
    host: str
    port: int
    status: str = "alive"
    incarnation: int = 0
    last_seen: float = 0.0
    topics: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PeerInfo":
        return cls(
            node_id=str(data["node_id"]),
            host=str(data["host"]),
            port=int(data["port"]),
            status=str(data.get("status", "alive")),
            incarnation=int(data.get("incarnation", 0)),
            last_seen=float(data.get("last_seen", 0.0)),
            topics=list(data.get("topics", [])),
        )


@dataclass
class Message:
    type: str
    sender_id: str
    msg_id: str
    payload: dict[str, Any] = field(default_factory=dict)
    ttl: int = 0
    priority: int = 0
    hop_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def encode(self) -> bytes:
        return (json.dumps(self.to_dict(), separators=(",", ":")) + "\n").encode()

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Message":
        return cls(
            type=str(data["type"]),
            sender_id=str(data["sender_id"]),
            msg_id=str(data["msg_id"]),
            payload=dict(data.get("payload", {})),
            ttl=int(data.get("ttl", 0)),
            priority=int(data.get("priority", 0)),
            hop_count=int(data.get("hop_count", 0)),
        )
