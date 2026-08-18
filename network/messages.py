from __future__ import annotations
from dataclasses import asdict, dataclass, field
from typing import Any
import json

# Constantes de tipos de mensaje
MSG_JOIN = "JOIN"
MSG_JOIN_ACK = "JOIN_ACK"
MSG_PING = "PING"
MSG_PONG = "PONG"
MSG_MEMBERSHIP_GOSSIP = "MEMBERSHIP_GOSSIP"
MSG_GOSSIP_ACK = "GOSSIP_ACK"
MSG_SUBSCRIBE = "SUBSCRIBE"
MSG_SUBSCRIBE_ACK = "SUBSCRIBE_ACK"
MSG_UNSUBSCRIBE = "UNSUBSCRIBE"
MSG_UNSUBSCRIBE_ACK = "UNSUBSCRIBE_ACK"
MSG_PUBLISH = "PUBLISH"
MSG_PUBLISH_ACK = "PUBLISH_ACK"

# Constantes de canales
CHANNEL_OBJECTIVE = "objective"
CHANNEL_SUBJECTIVE = "subjective"

# Niveles de prioridad sugeridos
PRIORITY_CRITICAL = 100
PRIORITY_HIGH = 75
PRIORITY_NORMAL = 50
PRIORITY_LOW = 25


@dataclass
class PeerInfo:
    """Representa la información de un nodo de la red."""

    node_id: str
    host: str
    port: int
    status: str = "alive"
    incarnation: int = 0
    last_seen: float = 0.0
    topics: list[str] = field(default_factory=list)

    def add_topic(self, topic: str) -> bool:
        if topic not in self.topics:
            self.topics.append(topic)
            return True
        return False

    def remove_topic(self, topic: str) -> bool:
        if topic in self.topics:
            self.topics.remove(topic)
            return True
        return False

    def is_subscribed_to(self, topic: str) -> bool:
        return topic in self.topics

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
    """Representa un mensaje que se intercambia entre nodos."""

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