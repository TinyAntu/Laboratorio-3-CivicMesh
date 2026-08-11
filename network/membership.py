from __future__ import annotations

from dataclasses import dataclass
import random
import time

from .messages import PeerInfo
from .failure_detector import FailureDetector


@dataclass
class MembershipConfig:
    gossip_fanout: int = 2
    failure_timeout: float = 5.0
    suspect_timeout: float = 5.0


class Membership:
    """Vista parcial de la membresia con gossip periodico y deteccion de timeout."""

    def __init__(
        self,
        self_peer: PeerInfo,
        config: MembershipConfig | None = None,
        seed: int | None = None,
    ):
        self.self_peer = self_peer
        self.config = config or MembershipConfig()
        self.peers: dict[str, PeerInfo] = {}
        self.failure_detector = FailureDetector(
            timeout=self.config.failure_timeout,
            suspect_timeout=self.config.suspect_timeout,
        )
        self.rng = random.Random(seed)

    def add_peer(self, peer: PeerInfo, now: float | None = None) -> bool:
        if peer.node_id == self.self_peer.node_id:
            return False

        now = time.monotonic() if now is None else now
        current = self.peers.get(peer.node_id)

        if current is None or peer.incarnation >= current.incarnation:
            peer.last_seen = now
            peer.status = "alive"
            self.peers[peer.node_id] = peer
            self.failure_detector.observe(peer.node_id, now)
            return True

        return False

    def mark_seen(self, node_id: str, now: float | None = None) -> None:
        now = time.monotonic() if now is None else now
        peer = self.peers.get(node_id)
        if peer:
            peer.last_seen = now
            peer.status = "alive"
            self.failure_detector.observe(node_id, now)

    def remove_failed(self) -> list[str]:
        removed = []
        for node_id, peer in list(self.peers.items()):
            if peer.status == "failed":
                removed.append(node_id)
                del self.peers[node_id]
                self.failure_detector.remove(node_id)
        return removed

    def select_gossip_targets(self, fanout: int | None = None) -> list[PeerInfo]:
        """Selecciona uniformemente hasta un numero determinado de nodos activos de la vista parcial para la propagacion (fanout)."""
        fanout = self.config.gossip_fanout if fanout is None else fanout
        candidates = [
            peer for peer in self.peers.values()
            if peer.status == "alive"
        ]
        if not candidates:
            return []
        return self.rng.sample(candidates, min(fanout, len(candidates)))

    def merge(self, remote_peers: list[dict], now: float | None = None) -> int:
        changed = 0
        for raw in remote_peers:
            peer = PeerInfo.from_dict(raw)
            if self.add_peer(peer, now=now):
                changed += 1
        return changed

    def gossip_view(self) -> list[dict]:
        return [
            peer.to_dict()
            for peer in self.peers.values()
            if peer.status != "failed"
        ]

    def run_failure_check(self, now: float | None = None) -> dict[str, str]:
        changes = self.failure_detector.check(now=now)
        for node_id, status in changes.items():
            if node_id in self.peers:
                self.peers[node_id].status = status
        return changes

    def snapshot(self) -> dict:
        return {
            "self": self.self_peer.to_dict(),
            "peers": {
                node_id: peer.to_dict()
                for node_id, peer in self.peers.items()
            },
            "failure_detector": self.failure_detector.snapshot(),
        }
