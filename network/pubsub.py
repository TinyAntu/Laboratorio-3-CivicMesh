from __future__ import annotations
from collections import OrderedDict
from dataclasses import dataclass, field
import heapq
import itertools
import random
import time
import uuid
from typing import Any, Callable

from .messages import (
    Message,
    PeerInfo,
    MSG_PUBLISH,
    MSG_SUBSCRIBE,
    MSG_UNSUBSCRIBE,
    CHANNEL_OBJECTIVE,
    CHANNEL_SUBJECTIVE,
    PRIORITY_HIGH,
    PRIORITY_NORMAL,
)
from .topology import GeoTopology, DEFAULT_COMMUNE_ADJACENCY


@dataclass
class PubSubConfig:
    """Configuración de la capa Pub/Sub."""
    pubsub_fanout: int = 3
    default_ttl_objective: int = 3
    default_priority_objective: int = 80
    default_ttl_subjective: int = 5
    default_priority_subjective: int = 50
    min_forward_priority: int = 0
    max_dedup_cache_size: int = 5000
    cache_ttl_seconds: float = 300.0


class Deduplicator:
    """Control de duplicados con tamaño acotado (LRU) y tiempo de expiración."""

    def __init__(self, max_size: int = 5000, ttl_seconds: float = 300.0):
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self._seen: OrderedDict[str, float] = OrderedDict()

    def is_seen(self, msg_id: str, now: float | None = None) -> bool:
        now = time.monotonic() if now is None else now
        if msg_id not in self._seen:
            return False

        # Verificar si expiró
        seen_time = self._seen[msg_id]
        if now - seen_time > self.ttl_seconds:
            del self._seen[msg_id]
            return False

        # Mover al final (más reciente)
        self._seen.move_to_end(msg_id)
        return True

    def mark_seen(self, msg_id: str, now: float | None = None) -> bool:
        """Marca un mensaje como visto. Retorna True si era nuevo, False si ya fue visto."""
        now = time.monotonic() if now is None else now
        if self.is_seen(msg_id, now=now):
            return False

        if len(self._seen) >= self.max_size:
            self._seen.popitem(last=False)

        self._seen[msg_id] = now
        return True

    def clear(self) -> None:
        self._seen.clear()


class SubscriptionManager:
    """Gestiona las suscripciones locales y el conocimiento de suscripciones de peers remotos."""

    def __init__(self, self_node_id: str):
        self.self_node_id = self_node_id
        self.local_subscriptions: set[str] = set()
        self.peer_subscriptions: dict[str, set[str]] = {}

    def subscribe_local(self, topic: str) -> bool:
        if topic not in self.local_subscriptions:
            self.local_subscriptions.add(topic)
            return True
        return False

    def unsubscribe_local(self, topic: str) -> bool:
        if topic in self.local_subscriptions:
            self.local_subscriptions.remove(topic)
            return True
        return False

    def is_locally_subscribed(self, topic: str) -> bool:
        return topic in self.local_subscriptions

    def update_peer_topics(self, node_id: str, topics: list[str] | set[str]) -> None:
        if node_id != self.self_node_id:
            self.peer_subscriptions[node_id] = set(topics)

    def add_peer_topic(self, node_id: str, topic: str) -> None:
        if node_id != self.self_node_id:
            if node_id not in self.peer_subscriptions:
                self.peer_subscriptions[node_id] = set()
            self.peer_subscriptions[node_id].add(topic)

    def remove_peer_topic(self, node_id: str, topic: str) -> None:
        if node_id in self.peer_subscriptions:
            self.peer_subscriptions[node_id].discard(topic)

    def remove_peer(self, node_id: str) -> None:
        self.peer_subscriptions.pop(node_id, None)

    def get_subscribers_for_topic(self, topic: str) -> set[str]:
        """Retorna los node_id de peers remotos suscritos a un tópico."""
        return {
            node_id
            for node_id, topics in self.peer_subscriptions.items()
            if topic in topics
        }


def should_forward(
    msg: Message,
    topic: str,
    local_view: dict[str, PeerInfo] | list[PeerInfo],
    deduplicator: Deduplicator | None = None,
    min_priority: int = 0,
) -> bool:
    """Decide si un mensaje debe ser reenviado según TTL, duplicados, prioridad y vista local.

    Evita el flooding ciego verificando:
    1. TTL restante > 0.
    2. Que el mensaje no haya sido visto/procesado anteriormente.
    3. Que la prioridad supere el umbral mínimo de reenvío.
    4. Que existan peers candidatos disponibles en la vista local distintos del emisor.
    """
    # 1. Validación de TTL
    if msg.ttl <= 0:
        return False

    # 2. Control de duplicados
    if deduplicator is not None and deduplicator.is_seen(msg.msg_id):
        return False

    # 3. Validación de prioridad
    if msg.priority < min_priority:
        return False

    # 4. Verificar existencia de candidatos válidos en la vista local
    peers_list = (
        list(local_view.values()) if isinstance(local_view, dict) else list(local_view)
    )
    available_targets = [
        p for p in peers_list
        if p.node_id != msg.sender_id and p.status != "failed"
    ]
    if not available_targets:
        return False

    return True


def select_forward_targets(
    msg: Message,
    topic: str,
    candidates: list[PeerInfo],
    fanout: int,
    subscription_manager: SubscriptionManager | None = None,
    topology: GeoTopology | None = None,
    rng: random.Random | None = None,
) -> list[PeerInfo]:
    """Selecciona hasta `fanout` peers de destino utilizando una política informada por geografía y suscripción.

    Prioridad de selección:
    1. Peers explícitamente suscritos al tópico.
    2. Peers suscritos a comunas vecinas/adyacentes en la topología geográfica.
    3. Otros peers activos (exploración / cobertura de malla).
    """
    if rng is None:
        rng = random.Random()

    # Filtrar emisor y nodos caídos
    valid_candidates = [
        p for p in candidates
        if p.node_id != msg.sender_id and p.status != "failed"
    ]
    if not valid_candidates or fanout <= 0:
        return []

    if len(valid_candidates) <= fanout:
        return valid_candidates

    direct_subscribers: list[PeerInfo] = []
    neighbor_subscribers: list[PeerInfo] = []
    other_peers: list[PeerInfo] = []

    neighbor_topics = set()
    if topology and topic:
        neighbor_topics = set(topology.get_neighbors(topic))

    for peer in valid_candidates:
        peer_topics = set(peer.topics)
        if subscription_manager and peer.node_id in subscription_manager.peer_subscriptions:
            peer_topics.update(subscription_manager.peer_subscriptions[peer.node_id])

        if topic in peer_topics:
            direct_subscribers.append(peer)
        elif bool(peer_topics.intersection(neighbor_topics)):
            neighbor_subscribers.append(peer)
        else:
            other_peers.append(peer)

    selected: list[PeerInfo] = []

    # 1. Agregar suscriptores directos
    rng.shuffle(direct_subscribers)
    for p in direct_subscribers:
        if len(selected) < fanout:
            selected.append(p)

    # 2. Agregar suscriptores de comunas vecinas
    if len(selected) < fanout:
        rng.shuffle(neighbor_subscribers)
        for p in neighbor_subscribers:
            if len(selected) < fanout:
                selected.append(p)

    # 3. Completar con otros peers para exploración
    if len(selected) < fanout:
        rng.shuffle(other_peers)
        for p in other_peers:
            if len(selected) < fanout:
                selected.append(p)

    return selected


class ForwardPriorityQueue:
    """Cola de mensajes pendientes de reenvío ordenada por prioridad (mayor prioridad primero)."""

    def __init__(self):
        self._queue: list[tuple[int, int, Message, list[PeerInfo]]] = []
        self._counter = itertools.count()

    def push(self, message: Message, targets: list[PeerInfo]) -> None:
        # heapq es min-heap, invertimos la prioridad con -message.priority
        count = next(self._counter)
        heapq.heappush(self._queue, (-message.priority, count, message, targets))

    def pop(self) -> tuple[Message, list[PeerInfo]] | None:
        if not self._queue:
            return None
        _, _, message, targets = heapq.heappop(self._queue)
        return message, targets

    def __len__(self) -> int:
        return len(self._queue)

    def is_empty(self) -> bool:
        return len(self._queue) == 0


class PubSubEngine:
    """Motor Pub/Sub que gestiona suscripciones, deduplicación, reenvío explícito y despacho local."""

    def __init__(
        self,
        self_peer: PeerInfo,
        send_fn: Callable[[PeerInfo, Message], None],
        config: PubSubConfig | None = None,
        topology: GeoTopology | None = None,
        seed: int | None = None,
        metrics_collector: Any | None = None,
    ):
        self.self_peer = self_peer
        self.send_fn = send_fn
        self.config = config or PubSubConfig()
        self.topology = topology or GeoTopology()
        self.rng = random.Random(seed)
        self.metrics = metrics_collector

        self.subscriptions = SubscriptionManager(self_peer.node_id)
        self.deduplicator = Deduplicator(
            max_size=self.config.max_dedup_cache_size,
            ttl_seconds=self.config.cache_ttl_seconds,
        )
        self.forward_queue = ForwardPriorityQueue()
        self.message_handlers: list[Callable[[Message], None]] = []

    def register_handler(self, handler: Callable[[Message], None]) -> None:
        """Registra un callback invocado cuando se recibe un mensaje de un tópico suscrito."""
        self.message_handlers.append(handler)

    def subscribe(self, topic: str, include_neighbors: bool = False) -> list[str]:
        """Suscribe el nodo a un tópico (comuna) y opcionalmente a sus comunas vecinas."""
        subscribed = []
        if self.subscriptions.subscribe_local(topic):
            self.self_peer.add_topic(topic)
            subscribed.append(topic)

        if include_neighbors:
            for neighbor in self.topology.get_neighbors(topic):
                if self.subscriptions.subscribe_local(neighbor):
                    self.self_peer.add_topic(neighbor)
                    subscribed.append(neighbor)

        return subscribed

    def unsubscribe(self, topic: str) -> bool:
        """Cancela la suscripción local a un tópico."""
        removed = self.subscriptions.unsubscribe_local(topic)
        self.self_peer.remove_topic(topic)
        return removed

    def create_publish_message(
        self,
        topic: str,
        channel: str,
        value: Any,
        timestamp: float | None = None,
        ttl: int | None = None,
        priority: int | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Message:
        """Construye un mensaje PUBLISH configurando TTL y prioridad según el canal."""
        timestamp = time.time() if timestamp is None else timestamp
        meta = metadata or {}

        if channel == CHANNEL_OBJECTIVE:
            msg_ttl = self.config.default_ttl_objective if ttl is None else ttl
            msg_prio = self.config.default_priority_objective if priority is None else priority
        elif channel == CHANNEL_SUBJECTIVE:
            msg_ttl = self.config.default_ttl_subjective if ttl is None else ttl
            msg_prio = self.config.default_priority_subjective if priority is None else priority
        else:
            msg_ttl = 3 if ttl is None else ttl
            msg_prio = PRIORITY_NORMAL if priority is None else priority

        msg_id = f"pub-{self.self_peer.node_id}-{uuid.uuid4().hex[:10]}"
        payload = {
            "topic": topic,
            "channel": channel,
            "value": value,
            "timestamp": timestamp,
            "source_id": self.self_peer.node_id,
            "metadata": meta,
        }

        return Message(
            type=MSG_PUBLISH,
            sender_id=self.self_peer.node_id,
            msg_id=msg_id,
            payload=payload,
            ttl=msg_ttl,
            priority=msg_prio,
            hop_count=0,
        )

    def handle_incoming_message(
        self,
        msg: Message,
        local_peers: dict[str, PeerInfo] | list[PeerInfo],
    ) -> tuple[bool, int]:
        """Procesa un mensaje recibido en la red.

        Retorna una tupla: (delivered_locally: bool, forwarded_to_count: int)
        """
        if msg.type != MSG_PUBLISH:
            if msg.type == MSG_SUBSCRIBE:
                topic = msg.payload.get("topic")
                if topic:
                    self.subscriptions.add_peer_topic(msg.sender_id, topic)
            elif msg.type == MSG_UNSUBSCRIBE:
                topic = msg.payload.get("topic")
                if topic:
                    self.subscriptions.remove_peer_topic(msg.sender_id, topic)
            return (False, 0)

        topic = msg.payload.get("topic", "")

        # 1. Deduplicación: si ya se vio, no se procesa ni reenvía
        is_new = self.deduplicator.mark_seen(msg.msg_id)
        if not is_new:
            if self.metrics:
                self.metrics.record_drop(
                    reason="duplicate",
                    msg_id=msg.msg_id,
                    topic=topic,
                    channel=msg.payload.get("channel", ""),
                )
            return (False, 0)

        # 2. Despacho local: Si el peer está suscrito al tópico, entregar a handlers
        delivered_locally = False
        if self.subscriptions.is_locally_subscribed(topic):
            delivered_locally = True
            if self.metrics:
                self.metrics.record_delivery(
                    topic=topic,
                    channel=msg.payload.get("channel", ""),
                    value=msg.payload.get("value"),
                    msg_id=msg.msg_id,
                    sender_id=msg.sender_id,
                    source_id=msg.payload.get("source_id"),
                    hop_count=msg.hop_count,
                    metadata=msg.payload.get("metadata", {}),
                )
            for handler in self.message_handlers:
                try:
                    handler(msg)
                except Exception:
                    pass

        # 3. Reenvío explícito (should_forward)
        peers_list = (
            list(local_peers.values())
            if isinstance(local_peers, dict)
            else list(local_peers)
        )

        can_forward = should_forward(
            msg=msg,
            topic=topic,
            local_view=peers_list,
            deduplicator=None,  # Ya validado arriba
            min_priority=self.config.min_forward_priority,
        )

        forwarded_count = 0
        if can_forward and msg.ttl > 1:
            targets = select_forward_targets(
                msg=msg,
                topic=topic,
                candidates=peers_list,
                fanout=self.config.pubsub_fanout,
                subscription_manager=self.subscriptions,
                topology=self.topology,
                rng=self.rng,
            )

            if targets:
                forward_msg = Message(
                    type=MSG_PUBLISH,
                    sender_id=self.self_peer.node_id,
                    msg_id=msg.msg_id,  # Mantener msg_id original para control de duplicados
                    payload=msg.payload,
                    ttl=msg.ttl - 1,
                    priority=msg.priority,
                    hop_count=msg.hop_count + 1,
                )
                self.forward_queue.push(forward_msg, targets)
                forwarded_count = self.flush_forward_queue()
                if self.metrics:
                    self.metrics.record_forward(
                        topic=topic,
                        channel=msg.payload.get("channel", ""),
                        msg_id=msg.msg_id,
                        targets_count=len(targets),
                        remaining_ttl=forward_msg.ttl,
                        hop_count=forward_msg.hop_count,
                    )
        elif not can_forward:
            if self.metrics:
                reason = "ttl_expired" if msg.ttl <= 0 else "filtered"
                self.metrics.record_drop(
                    reason=reason,
                    msg_id=msg.msg_id,
                    topic=topic,
                    channel=msg.payload.get("channel", ""),
                )

        return (delivered_locally, forwarded_count)

    def flush_forward_queue(self) -> int:
        """Despacha los mensajes encolados para reenvío según prioridad."""
        total_sent = 0
        while not self.forward_queue.is_empty():
            item = self.forward_queue.pop()
            if not item:
                break
            msg, targets = item
            for target in targets:
                try:
                    self.send_fn(target, msg)
                    total_sent += 1
                except OSError:
                    pass
        return total_sent
