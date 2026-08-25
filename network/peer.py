from __future__ import annotations
import argparse
import json
import socket
import threading
import time
from typing import Any, Callable

from .gossip import Gossip
from .membership import Membership, MembershipConfig
from .messages import (
    Message,
    PeerInfo,
    MSG_JOIN,
    MSG_JOIN_ACK,
    MSG_PING,
    MSG_PONG,
    MSG_MEMBERSHIP_GOSSIP,
    MSG_GOSSIP_ACK,
    MSG_SUBSCRIBE,
    MSG_SUBSCRIBE_ACK,
    MSG_UNSUBSCRIBE,
    MSG_UNSUBSCRIBE_ACK,
    MSG_PUBLISH,
    MSG_PUBLISH_ACK,
    CHANNEL_OBJECTIVE,
    CHANNEL_SUBJECTIVE,
)
from .metrics import MetricsCollector
from .pubsub import PubSubEngine, PubSubConfig
from .state import LocalAggregateState
from .topology import GeoTopology


# Abre una conexion TCP con el nodo destino y envia un mensaje.
def send_json(host: str, port: int, message: Message, timeout: float = 2.0) -> None:
    with socket.create_connection((host, port), timeout=timeout) as conn:
        # Codifica el mensaje y lo envia por la conexion.
        conn.sendall(message.encode())


class Peer:
    """Peer TCP que integra membresía (Gossip) y capa Publish/Subscribe."""

    def __init__(
        self,
        node_id: str,
        host: str,
        port: int,
        fanout: int = 2,
        max_view_size: int = 8,
        pubsub_fanout: int | None = None,
        pubsub_fanout_objective: int | None = None,
        pubsub_fanout_subjective: int | None = None,
        ttl_objective: int = 3,
        ttl_subjective: int = 5,
        priority_objective: int = 80,
        priority_subjective: int = 50,
        failure_timeout: float = 5.0,
        suspect_timeout: float = 5.0,
        seed: int | None = None,
        topology: GeoTopology | None = None,
        runs_dir: str | None = None,
        run_id: str | None = None,
        metrics_collector: MetricsCollector | None = None,
    ):
        # Inicializa un nodo de la red.
        self.info = PeerInfo(node_id=node_id, host=host, port=port)
        self.metrics = (
            metrics_collector
            if metrics_collector is not None
            else MetricsCollector(node_id=node_id, run_id=run_id, runs_dir=runs_dir)
        )
        self.membership = Membership(
            self.info,
            MembershipConfig(
                gossip_fanout=fanout,
                max_view_size=max_view_size,
                failure_timeout=failure_timeout,
                suspect_timeout=suspect_timeout,
            ),
            seed=seed,
        )
        self.gossip = Gossip(self.membership, self._send_peer)

        # Compatibilidad: --pubsub-fanout sigue pudiendo fijar un valor común,
        # pero los valores por canal tienen precedencia si se entregan.
        legacy_pubsub_fanout = 3 if pubsub_fanout is None else int(pubsub_fanout)
        resolved_fanout_objective = (
            legacy_pubsub_fanout
            if pubsub_fanout_objective is None
            else int(pubsub_fanout_objective)
        )
        resolved_fanout_subjective = (
            legacy_pubsub_fanout
            if pubsub_fanout_subjective is None
            else int(pubsub_fanout_subjective)
        )

        self.pubsub = PubSubEngine(
            self_peer=self.info,
            send_fn=self._send_peer,
            config=PubSubConfig(
                fanout_objective=resolved_fanout_objective,
                fanout_subjective=resolved_fanout_subjective,
                default_ttl_objective=ttl_objective,
                default_ttl_subjective=ttl_subjective,
                default_priority_objective=priority_objective,
                default_priority_subjective=priority_subjective,
            ),
            topology=topology or GeoTopology(),
            seed=seed,
            metrics_collector=self.metrics,
        )

        # Estado agregado local por tópico y canal. Se registra como primer
        # handler interno para actualizarlo antes de ejecutar callbacks externos.
        self.state = LocalAggregateState()
        self.pubsub.register_handler(self._update_local_state)

        self.running = False
        self.server: socket.socket | None = None

    def _update_local_state(self, message: Message) -> None:
        """Actualiza el estado agregado cuando Pub/Sub entrega un PUBLISH local."""
        if message.type != MSG_PUBLISH:
            return

        payload = message.payload
        topic = str(payload.get("topic", ""))
        channel = str(payload.get("channel", ""))

        if not topic or not channel:
            return

        try:
            timestamp = float(payload.get("timestamp", time.time()))
        except (TypeError, ValueError):
            timestamp = time.time()

        self.state.update(
            topic=topic,
            channel=channel,
            value=payload.get("value"),
            timestamp=timestamp,
            source_id=str(payload.get("source_id", message.sender_id)),
            msg_id=message.msg_id,
            metadata=payload.get("metadata", {}),
        )

    def get_local_state(self) -> dict[str, Any]:
        """Retorna una copia del estado agregado local completo del peer."""
        return self.state.snapshot()

    def get_topic_state(self, topic: str) -> dict[str, Any]:
        """Retorna una copia del estado agregado local de un tópico."""
        return self.state.topic_state(topic)

    # Envia un mensaje a un nodo especifico.
    def _send_peer(self, peer: PeerInfo, message: Message) -> None:
        send_json(peer.host, peer.port, message)

    # Enviar un mensaje a otro nodo.
    def send(self, peer: PeerInfo, message: Message) -> None:
        self._send_peer(peer, message)

    # --- Métodos de la Capa Pub/Sub ---

    def subscribe(self, topic: str, include_neighbors: bool = False) -> list[str]:
        """Suscribe este peer a un tópico (comuna) y opcionalmente a comunas vecinas."""
        subscribed = self.pubsub.subscribe(topic, include_neighbors=include_neighbors)
        return subscribed

    def unsubscribe(self, topic: str) -> bool:
        """Cancela la suscripción a un tópico."""
        return self.pubsub.unsubscribe(topic)

    def on_message(self, handler: Callable[[Message], None]) -> None:
        """Registra un callback invocado al recibir un evento de un tópico suscrito."""
        self.pubsub.register_handler(handler)

    def publish(
        self,
        topic: str,
        channel: str,
        value: Any,
        timestamp: float | None = None,
        ttl: int | None = None,
        priority: int | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Message:
        """Publica un mensaje en un tópico y canal específico (objective/subjective)."""
        msg = self.pubsub.create_publish_message(
            topic=topic,
            channel=channel,
            value=value,
            timestamp=timestamp,
            ttl=ttl,
            priority=priority,
            metadata=metadata,
        )

        # Si el propio peer está suscrito, despacharlo localmente
        if self.pubsub.subscriptions.is_locally_subscribed(topic):
            for handler in self.pubsub.message_handlers:
                try:
                    handler(msg)
                except Exception:
                    pass

        # Registrar en deduplicador propio
        self.pubsub.deduplicator.mark_seen(msg.msg_id)

        # Registrar métrica de publicación
        if self.metrics:
            self.metrics.record_publish(
                topic=topic,
                channel=channel,
                value=value,
                msg_id=msg.msg_id,
                timestamp=timestamp,
                metadata=metadata,
            )

        # Reenviar a peers según la política de fanout de pubsub
        candidates = list(self.membership.peers.values())
        targets = self.pubsub.config.fanout_for_channel(channel)
        from .pubsub import select_forward_targets
        chosen = select_forward_targets(
            msg=msg,
            topic=topic,
            candidates=candidates,
            fanout=targets,
            subscription_manager=self.pubsub.subscriptions,
            topology=self.pubsub.topology,
            rng=self.pubsub.rng,
        )

        for target in chosen:
            try:
                self.send(target, msg)
            except OSError:
                pass

        return msg

    # --- Red y Membresía ---

    def join(self, seeds: list[PeerInfo]) -> None:
        for seed in seeds:
            if seed.node_id == self.info.node_id:
                continue

            self.membership.add_peer(seed)
            if seed.topics:
                self.pubsub.subscriptions.update_peer_topics(seed.node_id, seed.topics)

            msg = Message(
                type=MSG_JOIN,
                sender_id=self.info.node_id,
                msg_id=f"join-{self.info.node_id}-{time.time_ns()}",
                payload={"peer": self.info.to_dict()},
                ttl=1,
                priority=100,
            )

            try:
                self.send(seed, msg)
            except OSError:
                pass

    # Inicia el servidor TCP y los procesos de gossip.
    def start(self) -> None:
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind((self.info.host, self.info.port))
        self.server.listen(64)
        self.running = True
        threading.Thread(target=self._serve_loop, daemon=True).start()
        threading.Thread(target=self._gossip_loop, daemon=True).start()

        print(
            f"[peer] node={self.info.node_id} "
            f"listen={self.info.host}:{self.info.port} "
            f"gossip_fanout={self.membership.config.gossip_fanout} "
            f"max_view_size={self.membership.config.max_view_size} "
            f"pubsub_fanout_objective={self.pubsub.config.fanout_objective} "
            f"pubsub_fanout_subjective={self.pubsub.config.fanout_subjective} "
            f"topics={self.info.topics}",
            flush=True,
        )

    # Detiene el nodo.
    def stop(self) -> None:
        self.running = False
        if self.server:
            try:
                self.server.close()
            except OSError:
                pass

    # Mantiene el servidor esperando conexiones.
    def _serve_loop(self) -> None:
        assert self.server is not None
        while self.running:
            try:
                conn, _ = self.server.accept()
            except OSError:
                break

            threading.Thread(
                target=self._handle_connection,
                args=(conn,),
                daemon=True,
            ).start()

    # Procesa un mensaje recibido desde otro nodo.
    def _handle_connection(self, conn: socket.socket) -> None:
        try:
            conn.settimeout(3)
            buffer = b""
            while b"\n" not in buffer:
                part = conn.recv(65536)
                if not part:
                    return
                buffer += part

            line, _, _ = buffer.partition(b"\n")
            message = Message.from_dict(json.loads(line.decode()))

            if message.type == MSG_JOIN:
                peer = PeerInfo.from_dict(message.payload["peer"])
                self.membership.add_peer(peer)
                if peer.topics:
                    self.pubsub.subscriptions.update_peer_topics(peer.node_id, peer.topics)
                self._reply(conn, MSG_JOIN_ACK, {"members": self.membership.gossip_view()})

            elif message.type == MSG_PING:
                self.membership.mark_seen(message.sender_id)
                self._reply(conn, MSG_PONG, {"node_id": self.info.node_id})

            elif message.type == MSG_MEMBERSHIP_GOSSIP:
                self.membership.mark_seen(message.sender_id)
                self.gossip.handle(message)
                # Sincronizar topics aprendidos
                for m in message.payload.get("members", []):
                    if "node_id" in m and "topics" in m:
                        self.pubsub.subscriptions.update_peer_topics(m["node_id"], m["topics"])
                self._reply(conn, MSG_GOSSIP_ACK, {"node_id": self.info.node_id})

            elif message.type == MSG_SUBSCRIBE:
                topic = message.payload.get("topic", "")
                if topic:
                    self.pubsub.subscriptions.add_peer_topic(message.sender_id, topic)
                    if message.sender_id in self.membership.peers:
                        self.membership.peers[message.sender_id].add_topic(topic)
                self._reply(conn, MSG_SUBSCRIBE_ACK, {"topic": topic, "status": "subscribed"})

            elif message.type == MSG_UNSUBSCRIBE:
                topic = message.payload.get("topic", "")
                if topic:
                    self.pubsub.subscriptions.remove_peer_topic(message.sender_id, topic)
                    if message.sender_id in self.membership.peers:
                        self.membership.peers[message.sender_id].remove_topic(topic)
                self._reply(conn, MSG_UNSUBSCRIBE_ACK, {"topic": topic, "status": "unsubscribed"})

            elif message.type == MSG_PUBLISH:
                delivered, forwarded = self.pubsub.handle_incoming_message(
                    message, self.membership.peers
                )
                self._reply(
                    conn,
                    MSG_PUBLISH_ACK,
                    {
                        "msg_id": message.msg_id,
                        "delivered": delivered,
                        "forwarded": forwarded,
                        "node_id": self.info.node_id,
                    },
                )

            else:
                self._reply(
                    conn,
                    "ERROR",
                    {"error": f"unsupported message type: {message.type}"},
                )

        except (OSError, ValueError, KeyError, json.JSONDecodeError):
            pass
        finally:
            try:
                conn.close()
            except OSError:
                pass

    def _reply(self, conn: socket.socket, message_type: str, payload: dict[str, Any]) -> None:
        msg = Message(
            type=message_type,
            sender_id=self.info.node_id,
            msg_id=f"reply-{time.time_ns()}",
            payload=payload,
            ttl=1,
            priority=100,
        )
        conn.sendall(msg.encode())

    def _gossip_loop(self) -> None:
        while self.running:
            time.sleep(self.gossip.interval)
            if self.running:
                sent = self.gossip.round()
                changes = self.membership.run_failure_check()
                if self.metrics:
                    active = [p.node_id for p in self.membership.peers.values() if p.status == "alive"]
                    suspect = [p.node_id for p in self.membership.peers.values() if p.status == "suspect"]
                    failed = [p.node_id for p in self.membership.peers.values() if p.status == "failed"]
                    self.metrics.record_gossip(active, suspect, failed, sent)
                if sent or changes:
                    print(
                        f"[gossip] sent={sent} changes={changes} "
                        f"view={list(self.membership.peers)}",
                        flush=True,
                    )


def load_peers(path: str) -> list[PeerInfo]:
    peers = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            node_id, host, port = parts[0], parts[1], int(parts[2])
            topics = parts[3].split(",") if len(parts) > 3 else []
            p = PeerInfo(node_id=node_id, host=host, port=port, topics=topics)
            peers.append(p)
    return peers


def main() -> int:
    parser = argparse.ArgumentParser(description="CivicMesh network peer with Pub/Sub")
    parser.add_argument("--node-id", required=True)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--seeds-file")
    parser.add_argument("--fanout", type=int, default=2, help="Gossip membership fanout")
    parser.add_argument("--max-view-size", type=int, default=8, help="Maximum local membership view size")
    parser.add_argument(
        "--pubsub-fanout",
        type=int,
        default=None,
        help="Legacy: aplica el mismo fanout PubSub a ambos canales",
    )
    parser.add_argument("--pubsub-fanout-objective", type=int, default=None)
    parser.add_argument("--pubsub-fanout-subjective", type=int, default=None)
    parser.add_argument("--ttl-objective", type=int, default=3)
    parser.add_argument("--ttl-subjective", type=int, default=5)
    parser.add_argument("--priority-objective", type=int, default=80)
    parser.add_argument("--priority-subjective", type=int, default=50)
    parser.add_argument("--failure-timeout", type=float, default=5.0)
    parser.add_argument("--suspect-timeout", type=float, default=5.0)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--topics", default="", help="Comma separated list of initial topics/communes")
    parser.add_argument("--runs-dir", default=None, help="Base directory for runs")
    parser.add_argument("--run-id", default=None, help="Identifier for current run")
    args = parser.parse_args()

    peer = Peer(
        node_id=args.node_id,
        host=args.host,
        port=args.port,
        fanout=args.fanout,
        max_view_size=args.max_view_size,
        pubsub_fanout=args.pubsub_fanout,
        pubsub_fanout_objective=args.pubsub_fanout_objective,
        pubsub_fanout_subjective=args.pubsub_fanout_subjective,
        ttl_objective=args.ttl_objective,
        ttl_subjective=args.ttl_subjective,
        priority_objective=args.priority_objective,
        priority_subjective=args.priority_subjective,
        failure_timeout=args.failure_timeout,
        suspect_timeout=args.suspect_timeout,
        seed=args.seed,
        runs_dir=args.runs_dir,
        run_id=args.run_id,
    )

    if args.topics:
        for topic in args.topics.split(","):
            topic = topic.strip()
            if topic:
                peer.subscribe(topic)

    peer.start()

    if args.seeds_file:
        peer.join(load_peers(args.seeds_file))

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        peer.stop()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())