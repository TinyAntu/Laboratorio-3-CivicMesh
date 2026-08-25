from __future__ import annotations

from dataclasses import dataclass
import random
import time

from .messages import PeerInfo
from .failure_detector import FailureDetector


@dataclass
class MembershipConfig:
    """Configuración del sistema de membresía."""

    # Cantidad máxima de peers contactados en cada ronda Gossip.
    gossip_fanout: int = 2

    # Cantidad máxima de peers almacenados en la vista local.
    max_view_size: int = 8

    # Tiempo sin señales antes de considerar un peer sospechoso.
    failure_timeout: float = 5.0

    # Tiempo adicional antes de considerarlo fallido.
    suspect_timeout: float = 5.0


class Membership:
    """
    Vista parcial de membresía con Gossip periódico y detección de fallos.

    La vista local está limitada por ``max_view_size``. El estado de liveness
    se conserva aunque un peer salga temporalmente de la vista parcial, para
    evitar que Gossip indirecto reinicie su timeout al reaprenderlo.
    """

    def __init__(
        self,
        self_peer: PeerInfo,
        config: MembershipConfig | None = None,
        seed: int | None = None,
    ):
        self.self_peer = self_peer
        self.config = config or MembershipConfig()

        if self.config.gossip_fanout <= 0:
            raise ValueError("gossip_fanout debe ser mayor que 0")

        if self.config.max_view_size <= 0:
            raise ValueError("max_view_size debe ser mayor que 0")

        # Vista parcial activa usada para seleccionar destinos Gossip/PubSub.
        self.peers: dict[str, PeerInfo] = {}

        # Historial ligero separado de la vista parcial. Conserva la última
        # incarnation y metadatos conocidos aunque un peer sea expulsado de la
        # vista por max_view_size.
        self._known_peers: dict[str, PeerInfo] = {}

        self.failure_detector = FailureDetector(
            timeout=self.config.failure_timeout,
            suspect_timeout=self.config.suspect_timeout,
        )

        self.rng = random.Random(seed)

    def _remember_peer(self, peer: PeerInfo) -> None:
        """Conserva la metadata/incarnation más reciente conocida."""
        current = self._known_peers.get(peer.node_id)
        if current is None or peer.incarnation >= current.incarnation:
            self._known_peers[peer.node_id] = PeerInfo.from_dict(peer.to_dict())

    def _remove_peer(self, node_id: str) -> None:
        """
        Elimina un peer de la vista parcial, pero conserva su historial de
        liveness/incarnation.

        Esto evita que una mención Gossip stale lo vuelva a introducir como si
        fuera completamente nuevo y reinicie el detector de fallos.
        """
        peer = self.peers.pop(node_id, None)
        if peer is not None:
            self._remember_peer(peer)

    def _select_eviction_candidate(self) -> str | None:
        """Selecciona qué peer expulsar cuando la vista está llena."""
        if not self.peers:
            return None

        non_alive = [
            peer.node_id
            for peer in self.peers.values()
            if peer.status != "alive"
        ]

        if non_alive:
            return self.rng.choice(non_alive)

        return self.rng.choice(list(self.peers.keys()))

    def _make_room_for_new_peer(self) -> None:
        if len(self.peers) < self.config.max_view_size:
            return

        candidate = self._select_eviction_candidate()
        if candidate is not None:
            self._remove_peer(candidate)

    def add_peer(
        self,
        peer: PeerInfo,
        now: float | None = None,
    ) -> bool:
        """
        Agrega o actualiza un peer usando evidencia DIRECTA.

        Este método se usa para JOIN/seeds. Una comunicación directa puede
        recuperar un peer con la misma incarnation después de una partición
        temporal; una incarnation menor siempre se rechaza.
        """
        if peer.node_id == self.self_peer.node_id:
            return False

        now = time.monotonic() if now is None else now
        current = self.peers.get(peer.node_id)
        known = self._known_peers.get(peer.node_id)
        reference = current or known

        if reference is not None and peer.incarnation < reference.incarnation:
            return False

        if current is None:
            self._make_room_for_new_peer()

        peer.last_seen = now
        peer.status = "alive"
        self.peers[peer.node_id] = peer
        self._remember_peer(peer)
        self.failure_detector.observe(peer.node_id, now)
        return True

    def mark_seen(
        self,
        node_id: str,
        now: float | None = None,
    ) -> None:
        """
        Registra una señal de vida DIRECTA.

        Si el peer había sido expulsado de la vista parcial, puede restaurarse
        desde el historial conocido sin perder su metadata.
        """
        now = time.monotonic() if now is None else now
        peer = self.peers.get(node_id)

        if peer is None:
            known = self._known_peers.get(node_id)
            if known is None:
                return

            self._make_room_for_new_peer()
            peer = PeerInfo.from_dict(known.to_dict())
            self.peers[node_id] = peer

        peer.last_seen = now
        peer.status = "alive"
        self._remember_peer(peer)
        self.failure_detector.observe(node_id, now)

    def remove_failed(self) -> list[str]:
        """
        Quita peers failed de la vista activa pero conserva su tombstone local.

        Una misma incarnation no puede reaparecer solo por Gossip indirecto;
        una señal directa o una incarnation mayor sí pueden recuperarla.
        """
        removed: list[str] = []

        for node_id, peer in list(self.peers.items()):
            if peer.status == "failed":
                removed.append(node_id)
                self._remove_peer(node_id)

        return removed

    def select_gossip_targets(
        self,
        fanout: int | None = None,
    ) -> list[PeerInfo]:
        """Selecciona hasta ``fanout`` peers alive de la vista parcial."""
        fanout = self.config.gossip_fanout if fanout is None else fanout

        candidates = [
            peer
            for peer in self.peers.values()
            if peer.status == "alive"
        ]

        if not candidates:
            return []

        return self.rng.sample(candidates, min(fanout, len(candidates)))

    def _merge_indirect_peer(
        self,
        peer: PeerInfo,
        now: float,
    ) -> bool:
        """
        Integra información aprendida INDIRECTAMENTE por Gossip.

        Regla central: una mención de ``pX`` enviada por ``pY`` NO es un
        heartbeat de ``pX``. Para la misma incarnation se actualizan solo
        metadatos; ``last_seen`` y status permanecen bajo control local.
        """
        if peer.node_id == self.self_peer.node_id:
            return False

        current = self.peers.get(peer.node_id)
        known = self._known_peers.get(peer.node_id)
        reference = current or known

        # Primer descubrimiento indirecto: comienza una observación local. Si
        # nunca hay contacto directo, expirará normalmente por timeout.
        if reference is None:
            self._make_room_for_new_peer()
            peer.last_seen = now
            peer.status = "alive"
            self.peers[peer.node_id] = peer
            self._remember_peer(peer)
            self.failure_detector.observe(peer.node_id, now)
            return True

        if peer.incarnation < reference.incarnation:
            return False

        # Una incarnation mayor representa un reinicio/nueva versión del peer.
        if peer.incarnation > reference.incarnation:
            if current is None:
                self._make_room_for_new_peer()

            peer.last_seen = now
            peer.status = "alive"
            self.peers[peer.node_id] = peer
            self._remember_peer(peer)
            self.failure_detector.observe(peer.node_id, now)
            return True

        # Misma incarnation: actualizar metadatos sin tocar liveness.
        updated = PeerInfo.from_dict(reference.to_dict())
        changed = False

        if updated.host != peer.host:
            updated.host = peer.host
            changed = True
        if updated.port != peer.port:
            updated.port = peer.port
            changed = True
        if updated.topics != peer.topics:
            updated.topics = list(peer.topics)
            changed = True

        self._remember_peer(updated)

        # Si había salido de la vista, puede reingresar conservando el reloj
        # previo. Un failed no puede volver por información stale.
        if current is None:
            failure_state = self.failure_detector.get_state(peer.node_id)

            if failure_state is not None and failure_state.status == "failed":
                return False

            self._make_room_for_new_peer()

            if failure_state is not None:
                updated.last_seen = failure_state.last_seen
                updated.status = failure_state.status

            self.peers[peer.node_id] = updated
            return True

        # Si sigue en la vista, conservar status/last_seen locales.
        current.host = updated.host
        current.port = updated.port
        current.topics = list(updated.topics)
        return changed

    def merge(
        self,
        remote_peers: list[dict],
        now: float | None = None,
    ) -> int:
        """Integra una vista Gossip sin convertir terceros en heartbeats."""
        now = time.monotonic() if now is None else now
        changed = 0

        for raw in remote_peers:
            peer = PeerInfo.from_dict(raw)
            if self._merge_indirect_peer(peer, now):
                changed += 1

        return changed

    def gossip_view(self) -> list[dict]:
        """Construye la vista parcial propagada por Gossip."""
        return [
            peer.to_dict()
            for peer in self.peers.values()
            if peer.status != "failed"
        ]

    def run_failure_check(
        self,
        now: float | None = None,
    ) -> dict[str, str]:
        """Ejecuta el detector y sincroniza cambios con la vista activa."""
        changes = self.failure_detector.check(now=now)

        for node_id, status in changes.items():
            if node_id in self.peers:
                self.peers[node_id].status = status
                self._remember_peer(self.peers[node_id])
            elif node_id in self._known_peers:
                self._known_peers[node_id].status = status

        return changes

    def snapshot(self) -> dict:
        """Retorna una representación del estado actual de membresía."""
        return {
            "self": self.self_peer.to_dict(),
            "max_view_size": self.config.max_view_size,
            "peers": {
                node_id: peer.to_dict()
                for node_id, peer in self.peers.items()
            },
            "failure_detector": self.failure_detector.snapshot(),
        }
