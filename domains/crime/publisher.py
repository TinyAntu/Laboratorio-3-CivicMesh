from __future__ import annotations

import argparse
import time

from domains.config import ConfigLoader
from network.messages import (
    CHANNEL_OBJECTIVE,
    CHANNEL_SUBJECTIVE,
    MSG_PUBLISH,
    Message,
)
from network.peer import Peer, load_peers

from .generator import CrimeGenerator
from .perception import CrimePerceptionModel


class CrimePublisher:
    def __init__(
        self,
        peer: Peer,
        commune: str,
        generator: CrimeGenerator,
        perception_model: CrimePerceptionModel,
        delta_t: float,
        subjective_only: bool = False,
    ) -> None:
        self.peer = peer
        self.commune = commune
        self.generator = generator
        self.perception_model = perception_model
        self.delta_t = float(delta_t)
        self.subjective_only = bool(subjective_only)
        self.step = 0

        # Rumores subjetivos recibidos desde otros peers/publicadores
        # durante el paso anterior.
        self.received_rumors: list[float] = []

        self.peer.on_message(self._handle_message)

    def _handle_message(self, message: Message) -> None:
        """Guarda rumores subjetivos recibidos desde la malla."""
        if message.type != MSG_PUBLISH:
            return

        payload = message.payload

        if payload.get("channel") != CHANNEL_SUBJECTIVE:
            return

        # P_gossip_c(t) solo considera rumores del mismo tópico/comuna c.
        if payload.get("topic") != self.commune:
            return

        # Un publisher nunca debe usar su propia percepción como rumor.
        if payload.get("source_id") == self.peer.info.node_id:
            return

        metadata = payload.get("metadata", {})
        if metadata.get("domain") != "crime":
            return

        try:
            value = float(payload["value"])
            self.received_rumors.append(value)
        except (KeyError, TypeError, ValueError):
            return

    def _consume_gossip_value(self) -> float:
        """Promedia los rumores recibidos y limpia el buffer."""
        if not self.received_rumors:
            return 0.0

        gossip_value = sum(self.received_rumors) / len(self.received_rumors)
        self.received_rumors.clear()

        return gossip_value

    def run_step(self) -> tuple[int, float]:
        logical_time = self.step * self.delta_t
        events = self.generator.generate(self.commune, logical_time)
        total_crimes = self.generator.total(events)

        # El publisher secundario calcula el mismo ground truth local
        # para alimentar su percepción, pero no vuelve a publicarlo.
        if not self.subjective_only:
            for event in events:
                self.peer.publish(
                    topic=self.commune,
                    channel=CHANNEL_OBJECTIVE,
                    value=event.count,
                    timestamp=event.timestamp,
                    metadata={
                        "domain": "crime",
                        "crime_type": event.crime_type,
                        "step": self.step,
                        "delta_t": self.delta_t,
                        "source_role": "primary",
                    },
                )

        gossip_value = self._consume_gossip_value()

        perception = self.perception_model.update(
            commune=self.commune,
            crime_count=total_crimes,
            gossip_value=gossip_value,
        )

        self.peer.publish(
            topic=self.commune,
            channel=CHANNEL_SUBJECTIVE,
            value=perception,
            timestamp=logical_time,
            metadata={
                "domain": "crime",
                "step": self.step,
                "total_crimes": total_crimes,
                "memory": self.perception_model.memory(self.commune),
                "gossip_value": gossip_value,
                "source_role": (
                    "subjective_only"
                    if self.subjective_only
                    else "primary"
                ),
            },
        )

        if self.peer.metrics and not self.subjective_only:
            self.peer.metrics.record_step(
                domain="crime",
                commune=self.commune,
                step=self.step,
                objective_value=float(total_crimes),
                subjective_value=float(perception),
                memory=self.perception_model.memory(self.commune),
                gossip_value=gossip_value,
                timestamp=logical_time,
            )

        self.step += 1
        return total_crimes, perception


def main() -> int:
    parser = argparse.ArgumentParser(
        description="CivicMesh crime data publisher"
    )
    parser.add_argument("--node-id", required=True)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--commune", required=True)
    parser.add_argument("--config", default="config/civicmesh.yaml")
    parser.add_argument("--seeds-file")
    parser.add_argument("--fanout", type=int, default=2)
    parser.add_argument("--pubsub-fanout", type=int, default=3)
    parser.add_argument("--runs-dir", default=None, help="Base directory for runs")
    parser.add_argument("--run-id", default=None, help="Identifier for current run")
    parser.add_argument("--topics", default="", help="Comma-separated topics to subscribe for rumors")
    parser.add_argument(
        "--subjective-only",
        action="store_true",
        help="Publica solo el canal subjetivo; el ground truth se calcula localmente pero no se reenvía.",
    )
    args = parser.parse_args()

    config = ConfigLoader.load(args.config)

    if args.commune not in config.crime.rates:
        raise SystemExit(
            f"No hay tasas configuradas para {args.commune}"
        )

    peer = Peer(
        node_id=args.node_id,
        host=args.host,
        port=args.port,
        fanout=args.fanout,
        pubsub_fanout=args.pubsub_fanout,
        pubsub_fanout_objective=config.pubsub.objective.fanout,
        pubsub_fanout_subjective=config.pubsub.subjective.fanout,
        ttl_objective=config.pubsub.objective.ttl,
        ttl_subjective=config.pubsub.subjective.ttl,
        priority_objective=config.pubsub.objective.priority,
        priority_subjective=config.pubsub.subjective.priority,
        seed=config.seed,
        runs_dir=args.runs_dir,
        run_id=args.run_id,
    )

    if args.topics:
        for t in args.topics.split(","):
            t = t.strip()
            if t:
                peer.subscribe(t)
    else:
        peer.subscribe(args.commune, include_neighbors=True)

    peer.start()

    if args.seeds_file:
        peer.join(load_peers(args.seeds_file))

    publisher = CrimePublisher(
        peer=peer,
        commune=args.commune,
        generator=CrimeGenerator(
            rates=config.crime.rates,
            delta_t=config.simulation.delta_t,
            seed=config.seed,
        ),
        perception_model=CrimePerceptionModel(
            config.crime_perception,
            config.seed,
        ),
        delta_t=config.simulation.delta_t,
        subjective_only=args.subjective_only,
    )

    try:
        while True:
            total, perception = publisher.run_step()

            print(
                f"[crime] commune={args.commune} "
                f"step={publisher.step - 1} "
                f"total={total} "
                f"perception={perception:.4f} "
                f"mode={'subjective-only' if publisher.subjective_only else 'primary'}",
                flush=True,
            )

            time.sleep(config.simulation.interval_seconds)

    except KeyboardInterrupt:
        pass

    finally:
        peer.stop()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
