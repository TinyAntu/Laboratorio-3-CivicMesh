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
    ) -> None:
        self.peer = peer
        self.commune = commune
        self.generator = generator
        self.perception_model = perception_model
        self.delta_t = float(delta_t)
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

        # Descartar únicamente el despacho local inmediato del propio mensaje recién emitido
        if payload.get("source_id") == self.peer.info.node_id and message.hop_count == 0:
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
            },
        )

        if self.peer.metrics:
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
    )

    try:
        while True:
            total, perception = publisher.run_step()

            print(
                f"[crime] commune={args.commune} "
                f"step={publisher.step - 1} "
                f"total={total} "
                f"perception={perception:.4f}",
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
