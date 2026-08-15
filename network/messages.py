from __future__ import annotations
from dataclasses import asdict, dataclass, field
from typing import Any
import json
@dataclass
class PeerInfo:
    # Representa la información de un nodo de la red.

    node_id: str
    host: str
    port: int
    status: str = "alive"
    incarnation: int = 0
    last_seen: float = 0.0
    topics: list[str] = field(default_factory=list)

    # Convierte el objeto PeerInfo en un diccionario.
    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    # Crea un PeerInfo a partir de un diccionario.
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


# Representa un mensaje que se intercambia entre nodos.
@dataclass
class Message:

    type: str
    sender_id: str
    msg_id: str
    payload: dict[str, Any] = field(default_factory=dict)
    ttl: int = 0
    priority: int = 0
    hop_count: int = 0

    # Convierte el mensaje en un diccionario.
    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    # Convierte el mensaje a un formato que puede transmitirse por la red.
    def encode(self) -> bytes:
        return (json.dumps(self.to_dict(), separators=(",", ":")) + "\n").encode()

    # Crea un Message a partir de un diccionario.
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