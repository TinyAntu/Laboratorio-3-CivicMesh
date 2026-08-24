from __future__ import annotations

import argparse
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
        if metadata.get("domain") != "air":
            return

        # Si hay más de un contaminante subjetivo, evitar mezclarlos.
        rumor_pollutant = metadata.get("pollutant")
        if rumor_pollutant and rumor_pollutant != self.pollutant:
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

        # El canal objetivo publica las dos mediciones reales
        # disponibles en cada muestra del CSV.
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
            },
        )

        if self.peer.metrics:
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
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--runs-dir", default=None, help="Base directory for runs")
    parser.add_argument("--run-id", default=None, help="Identifier for current run")
    parser.add_argument("--topics", default="", help="Comma-separated topics to subscribe for rumors")
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
    )

    try:
        while publisher.replay.has_next():
            objective, perception = publisher.run_step()

            print(
                f"[air] commune={args.commune} "
                f"step={publisher.step - 1} "
                f"{publisher.pollutant}={objective:.2f} "
                f"perception={perception:.2f}",
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
