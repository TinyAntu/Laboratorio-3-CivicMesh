from __future__ import annotations

from dataclasses import dataclass
import random
import time

from .messages import PeerInfo
from .failure_detector import FailureDetector


@dataclass
class MembershipConfig:
    """
    Configuración del sistema de membresía.
    """

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
    Vista parcial de membresía con Gossip periódico
    y detección de fallos por timeout.

    La vista local está limitada por max_view_size.

    Si llega un peer nuevo cuando la vista está llena:
      1. se prioriza reemplazar peers suspect/failed;
      2. si todos están alive, se reemplaza uno al azar.

    El gossip_fanout es independiente del tamaño de la vista:
    determina cuántos peers activos son contactados en cada ronda.
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
            raise ValueError(
                "gossip_fanout debe ser mayor que 0"
            )

        if self.config.max_view_size <= 0:
            raise ValueError(
                "max_view_size debe ser mayor que 0"
            )

        self.peers: dict[str, PeerInfo] = {}

        self.failure_detector = FailureDetector(
            timeout=self.config.failure_timeout,
            suspect_timeout=self.config.suspect_timeout,
        )

        self.rng = random.Random(seed)

    def _remove_peer(self, node_id: str) -> None:
        """
        Elimina un peer tanto de la vista local como
        del detector de fallos.
        """

        self.peers.pop(node_id, None)
        self.failure_detector.remove(node_id)

    def _select_eviction_candidate(self) -> str | None:
        """
        Selecciona un peer para expulsar cuando la vista
        parcial está llena.

        Se priorizan peers que no estén alive.
        Si todos están activos, se escoge uno al azar.
        """

        if not self.peers:
            return None

        non_alive = [
            peer.node_id
            for peer in self.peers.values()
            if peer.status != "alive"
        ]

        if non_alive:
            return self.rng.choice(non_alive)

        return self.rng.choice(
            list(self.peers.keys())
        )

    def _make_room_for_new_peer(self) -> None:
        """
        Libera una posición cuando la vista alcanzó
        max_view_size.
        """

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
        Agrega un peer nuevo o actualiza uno existente.

        Un peer conocido solamente puede actualizarse si:

          - posee una incarnation mayor, o
          - posee la misma incarnation y actualmente está alive.

        Esto evita que información Gossip atrasada con la misma
        incarnation reactive accidentalmente un peer que ya fue
        marcado suspect o failed.

        La vista nunca supera max_view_size.
        """

        if peer.node_id == self.self_peer.node_id:
            return False

        now = (
            time.monotonic()
            if now is None
            else now
        )

        current = self.peers.get(peer.node_id)

        # ---------------------------------------------------------
        # Peer ya conocido.
        #
        # IMPORTANTE:
        # Esta es la lógica proveniente del fix de tus compañeros.
        #
        # Una misma incarnation solamente puede refrescar a un peer
        # que aún está alive. Si está suspect/failed, un dato Gossip
        # antiguo no debe resucitarlo.
        #
        # Una incarnation mayor sí representa información nueva.
        # ---------------------------------------------------------
        if current is not None:
            valid_update = (
                peer.incarnation > current.incarnation
                or (
                    peer.incarnation == current.incarnation
                    and current.status == "alive"
                )
            )

            if not valid_update:
                return False

            peer.last_seen = now
            peer.status = "alive"

            self.peers[peer.node_id] = peer

            self.failure_detector.observe(
                peer.node_id,
                now,
            )

            return True

        # ---------------------------------------------------------
        # Peer nuevo.
        #
        # Si la vista parcial está llena, se libera primero una
        # posición según la política definida.
        # ---------------------------------------------------------
        self._make_room_for_new_peer()

        peer.last_seen = now
        peer.status = "alive"

        self.peers[peer.node_id] = peer

        self.failure_detector.observe(
            peer.node_id,
            now,
        )

        return True

    def mark_seen(
        self,
        node_id: str,
        now: float | None = None,
    ) -> None:
        """
        Registra una señal de vida directa de un peer conocido.
        """

        now = (
            time.monotonic()
            if now is None
            else now
        )

        peer = self.peers.get(node_id)

        if peer is None:
            return

        peer.last_seen = now
        peer.status = "alive"

        self.failure_detector.observe(
            node_id,
            now,
        )

    def remove_failed(self) -> list[str]:
        """
        Elimina de la vista los peers actualmente marcados
        como failed.
        """

        removed: list[str] = []

        for node_id, peer in list(
            self.peers.items()
        ):
            if peer.status == "failed":
                removed.append(node_id)
                self._remove_peer(node_id)

        return removed

    def select_gossip_targets(
        self,
        fanout: int | None = None,
    ) -> list[PeerInfo]:
        """
        Selecciona aleatoriamente hasta fanout peers activos
        desde la vista parcial local.
        """

        fanout = (
            self.config.gossip_fanout
            if fanout is None
            else fanout
        )

        candidates = [
            peer
            for peer in self.peers.values()
            if peer.status == "alive"
        ]

        if not candidates:
            return []

        return self.rng.sample(
            candidates,
            min(fanout, len(candidates)),
        )

    def merge(
        self,
        remote_peers: list[dict],
        now: float | None = None,
    ) -> int:
        """
        Integra una vista recibida mediante Gossip.

        add_peer() aplica tanto las reglas de incarnation/status
        como el límite max_view_size.
        """

        changed = 0

        for raw in remote_peers:
            peer = PeerInfo.from_dict(raw)

            if self.add_peer(
                peer,
                now=now,
            ):
                changed += 1

        return changed

    def gossip_view(self) -> list[dict]:
        """
        Construye la vista que será compartida durante Gossip.

        Como self.peers ya está limitado por max_view_size,
        lo enviado sigue representando una vista parcial.
        """

        return [
            peer.to_dict()
            for peer in self.peers.values()
            if peer.status != "failed"
        ]

    def run_failure_check(
        self,
        now: float | None = None,
    ) -> dict[str, str]:
        """
        Ejecuta el detector de fallos y sincroniza los cambios
        de estado con la vista de Membership.
        """

        changes = self.failure_detector.check(
            now=now
        )

        for node_id, status in changes.items():
            if node_id in self.peers:
                self.peers[node_id].status = status

        return changes

    def snapshot(self) -> dict:
        """
        Retorna una representación del estado actual
        de membresía.
        """

        return {
            "self": self.self_peer.to_dict(),

            "max_view_size": (
                self.config.max_view_size
            ),

            "peers": {
                node_id: peer.to_dict()
                for node_id, peer
                in self.peers.items()
            },

            "failure_detector": (
                self.failure_detector.snapshot()
            ),
        }