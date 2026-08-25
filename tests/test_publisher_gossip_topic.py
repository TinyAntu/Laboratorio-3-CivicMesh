import pytest
from types import SimpleNamespace

from domains.air.publisher import AirQualityPublisher
from domains.crime.publisher import CrimePublisher
from network.messages import CHANNEL_SUBJECTIVE, MSG_PUBLISH, Message


class FakePeer:
    def __init__(self, node_id: str) -> None:
        self.info = SimpleNamespace(node_id=node_id)
        self.handler = None

    def on_message(self, handler) -> None:
        self.handler = handler


def subjective_message(*, topic: str, domain: str, value: float, pollutant: str | None = None) -> Message:
    metadata = {"domain": domain}
    if pollutant is not None:
        metadata["pollutant"] = pollutant

    return Message(
        type=MSG_PUBLISH,
        sender_id="peer-x",
        msg_id=f"msg-{domain}-{topic}-{value}",
        payload={
            "topic": topic,
            "channel": CHANNEL_SUBJECTIVE,
            "value": value,
            "source_id": "other-publisher",
            "metadata": metadata,
        },
        ttl=3,
        priority=50,
        hop_count=1,
    )


def test_crime_publisher_uses_only_rumors_from_same_topic():
    peer = FakePeer("crime-santiago")
    publisher = CrimePublisher(
        peer=peer,
        commune="Santiago",
        generator=None,
        perception_model=None,
        delta_t=1.0,
    )

    publisher._handle_message(
        subjective_message(topic="Las Condes", domain="crime", value=0.9)
    )
    publisher._handle_message(
        subjective_message(topic="Santiago", domain="crime", value=0.4)
    )
    publisher._handle_message(
        subjective_message(topic="Santiago", domain="crime", value=0.8)
    )

    assert publisher._consume_gossip_value() == pytest.approx(0.6)


def test_air_publisher_uses_only_rumors_from_same_topic():
    peer = FakePeer("air-santiago")
    publisher = AirQualityPublisher(
        peer=peer,
        commune="Santiago",
        replay=None,
        perception_model=None,
        pollutant="pm2_5",
    )

    publisher._handle_message(
        subjective_message(
            topic="Quilicura",
            domain="air",
            value=90.0,
            pollutant="pm2_5",
        )
    )
    publisher._handle_message(
        subjective_message(
            topic="Santiago",
            domain="air",
            value=40.0,
            pollutant="pm2_5",
        )
    )
    publisher._handle_message(
        subjective_message(
            topic="Santiago",
            domain="air",
            value=60.0,
            pollutant="pm2_5",
        )
    )

    assert publisher._consume_gossip_value() == 50.0
