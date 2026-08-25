from __future__ import annotations

import argparse
import threading
import time
from pathlib import Path

from domains.config import ConfigLoader
from network.messages import (
    CHANNEL_OBJECTIVE,
    CHANNEL_SUBJECTIVE,
    MSG_PUBLISH,
    Message,
)
from network.peer import Peer, load_peers

from .perception import AirPerceptionModel
from .replay import AirQualityDataset, AirQualityReplay


class AirQualityPublisher:
    def __init__(
        self,
        peer: Peer,
        commune: str,
        replay: AirQualityReplay,
        perception_model: AirPerceptionModel,
        pollutant: str = "pm2_5",
        subjective_only: bool = False,
    ) -> None:
        if pollutant not in {"pm2_5", "pm10"}:
            raise ValueError(
                "pollutant debe ser 'pm2_5' o 'pm10'"
            )

        self.peer = peer
        self.commune = commune
        self.replay = replay
        self.perception_model = perception_model
        self.pollutant = pollutant
        self.subjective_only = bool(subjective_only)
        self.step = 0

        # Rumores subjetivos agrupados por paso lógico. El paso t consume
        # exclusivamente los rumores publicados en t-1.
        self.received_rumors: dict[int, list[float]] = {}
        self._rumor_lock = threading.Lock()

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
        if metadata.get("domain") != "air":
            return

        # Si hay más de un contaminante subjetivo, evitar mezclarlos.
        rumor_pollutant = metadata.get("pollutant")
        if rumor_pollutant and rumor_pollutant != self.pollutant:
            return

        try:
            value = float(payload["value"])
            rumor_step = int(metadata["step"])
        except (KeyError, TypeError, ValueError):
            return

        if rumor_step < self.step - 1:
            return

        with self._rumor_lock:
            self.received_rumors.setdefault(rumor_step, []).append(value)

    def _consume_gossip_value(self) -> float:
        """Consume únicamente rumores del paso lógico anterior."""
        target_step = self.step - 1

        if target_step < 0:
            return 0.0

        with self._rumor_lock:
            values = self.received_rumors.pop(target_step, [])
            stale_steps = [
                step
                for step in self.received_rumors
                if step < target_step
            ]
            for step in stale_steps:
                self.received_rumors.pop(step, None)

        if not values:
            return 0.0

        return sum(values) / len(values)

    def run_step(self) -> tuple[float, float]:
        sample = self.replay.next_sample()

        objective_value = (
            sample.pm2_5
            if self.pollutant == "pm2_5"
            else sample.pm10
        )

        timestamp = sample.epoch_timestamp(
            self.replay.dataset.metadata.utc_offset_seconds
        )

        # El publisher secundario reproduce la misma muestra real para
        # calcular su percepción, pero no duplica el canal objetivo.
        if not self.subjective_only:
            for pollutant, value in (
                ("pm2_5", sample.pm2_5),
                ("pm10", sample.pm10),
            ):
                self.peer.publish(
                    topic=self.commune,
                    channel=CHANNEL_OBJECTIVE,
                    value=value,
                    timestamp=timestamp,
                    metadata={
                        "domain": "air",
                        "step": self.step,
                        "source": "Open-Meteo",
                        "pollutant": pollutant,
                        "sample_time": sample.time,
                        "latitude": (
                            self.replay.dataset.metadata.latitude
                        ),
                        "longitude": (
                            self.replay.dataset.metadata.longitude
                        ),
                        "source_role": "primary",
                    },
                )

        gossip_value = self._consume_gossip_value()

        perception = self.perception_model.update(
            commune=self.commune,
            objective_value=objective_value,
            gossip_value=gossip_value,
        )

        self.peer.publish(
            topic=self.commune,
            channel=CHANNEL_SUBJECTIVE,
            value=perception,
            timestamp=timestamp,
            metadata={
                "domain": "air",
                "step": self.step,
                "pollutant": self.pollutant,
                "sample_time": sample.time,
                "memory": self.perception_model.memory(
                    self.commune
                ),
                "gossip_value": gossip_value,
                "objective_value": objective_value,
                "source_role": (
                    "subjective_only"
                    if self.subjective_only
                    else "primary"
                ),
            },
        )

        if self.peer.metrics and not self.subjective_only:
            self.peer.metrics.record_step(
                domain="air",
                commune=self.commune,
                step=self.step,
                objective_value=float(objective_value),
                subjective_value=float(perception),
                memory=self.perception_model.memory(self.commune),
                gossip_value=gossip_value,
                timestamp=timestamp,
                metadata={"pollutant": self.pollutant, "sample_time": sample.time},
            )

        self.step += 1
        return objective_value, perception


def main() -> int:
    parser = argparse.ArgumentParser(
        description="CivicMesh air-quality replay publisher"
    )
    parser.add_argument("--node-id", required=True)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--commune", required=True)
    parser.add_argument("--config", default="config/civicmesh.yaml")
    parser.add_argument("--seeds-file")
    parser.add_argument("--fanout", type=int, default=2)
    parser.add_argument("--pubsub-fanout", type=int, default=3)
    parser.add_argument("--control-timeout", type=float, default=0.75)
    parser.add_argument("--listen-backlog", type=int, default=512)
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--runs-dir", default=None, help="Base directory for runs")
    parser.add_argument("--run-id", default=None, help="Identifier for current run")
    parser.add_argument("--topics", default="", help="Comma-separated topics to subscribe for rumors")
    parser.add_argument(
        "--subjective-only",
        action="store_true",
        help="Publica solo el canal subjetivo; la muestra real se reproduce localmente pero no se reenvía.",
    )
    args = parser.parse_args()

    config = ConfigLoader.load(args.config)

    if args.commune not in config.air.datasets:
        raise SystemExit(
            f"No hay dataset de aire configurado para "
            f"{args.commune}"
        )

    dataset_path = Path(
        config.air.datasets[args.commune]
    )

    dataset = AirQualityDataset.from_csv(
        dataset_path
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
        control_timeout=args.control_timeout,
        listen_backlog=args.listen_backlog,
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

    publisher = AirQualityPublisher(
        peer=peer,
        commune=args.commune,
        replay=AirQualityReplay(
            dataset,
            loop=args.loop,
        ),
        perception_model=AirPerceptionModel(
            config.air_perception,
            config.seed,
        ),
        pollutant=config.air.pollutant,
        subjective_only=args.subjective_only,
    )

    try:
        while publisher.replay.has_next():
            objective, perception = publisher.run_step()

            print(
                f"[air] commune={args.commune} "
                f"step={publisher.step - 1} "
                f"{publisher.pollutant}={objective:.2f} "
                f"perception={perception:.2f} "
                f"mode={'subjective-only' if publisher.subjective_only else 'primary'}",
                flush=True,
            )

            time.sleep(
                config.simulation.interval_seconds
            )

    except KeyboardInterrupt:
        pass

    finally:
        peer.stop()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
