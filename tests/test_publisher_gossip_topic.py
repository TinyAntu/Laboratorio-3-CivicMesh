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


def subjective_message(
    *,
    topic: str,
    domain: str,
    value: float,
    step: int = 0,
    pollutant: str | None = None,
    source_id: str = "other-publisher",
) -> Message:
    metadata = {"domain": domain, "step": step}
    if pollutant is not None:
        metadata["pollutant"] = pollutant

    return Message(
        type=MSG_PUBLISH,
        sender_id="peer-x",
        msg_id=f"msg-{domain}-{topic}-{step}-{value}",
        payload={
            "topic": topic,
            "channel": CHANNEL_SUBJECTIVE,
            "value": value,
            "source_id": source_id,
            "metadata": metadata,
        },
        ttl=3,
        priority=50,
        hop_count=1,
    )


def test_crime_publisher_uses_only_rumors_from_same_topic_and_previous_step():
    peer = FakePeer("crime-santiago")
    publisher = CrimePublisher(
        peer=peer,
        commune="Santiago",
        generator=None,
        perception_model=None,
        delta_t=1.0,
    )

    publisher._handle_message(
        subjective_message(topic="Las Condes", domain="crime", value=0.9, step=0)
    )
    publisher._handle_message(
        subjective_message(topic="Santiago", domain="crime", value=0.4, step=0)
    )
    publisher._handle_message(
        subjective_message(topic="Santiago", domain="crime", value=0.8, step=0)
    )
    # Un rumor del paso actual no debe consumirse todavía.
    publisher._handle_message(
        subjective_message(topic="Santiago", domain="crime", value=0.99, step=1)
    )

    # Condición inicial obligatoria.
    assert publisher._consume_gossip_value() == 0.0

    publisher.step = 1
    assert publisher._consume_gossip_value() == pytest.approx(0.6)

    publisher.step = 2
    assert publisher._consume_gossip_value() == pytest.approx(0.99)


def test_air_publisher_uses_only_rumors_from_same_topic_and_previous_step():
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
            step=0,
            pollutant="pm2_5",
        )
    )
    publisher._handle_message(
        subjective_message(
            topic="Santiago",
            domain="air",
            value=40.0,
            step=0,
            pollutant="pm2_5",
        )
    )
    publisher._handle_message(
        subjective_message(
            topic="Santiago",
            domain="air",
            value=60.0,
            step=0,
            pollutant="pm2_5",
        )
    )
    # Contaminante distinto: se ignora.
    publisher._handle_message(
        subjective_message(
            topic="Santiago",
            domain="air",
            value=200.0,
            step=0,
            pollutant="pm10",
        )
    )

    assert publisher._consume_gossip_value() == 0.0

    publisher.step = 1
    assert publisher._consume_gossip_value() == 50.0


def test_late_rumor_is_not_reused_in_a_later_step():
    peer = FakePeer("crime-santiago")
    publisher = CrimePublisher(
        peer=peer,
        commune="Santiago",
        generator=None,
        perception_model=None,
        delta_t=1.0,
    )

    # El publisher ya va a calcular el paso 3; solo corresponde step=2.
    publisher.step = 3

    publisher._handle_message(
        subjective_message(topic="Santiago", domain="crime", value=0.2, step=1)
    )
    publisher._handle_message(
        subjective_message(topic="Santiago", domain="crime", value=0.7, step=2)
    )

    assert publisher._consume_gossip_value() == pytest.approx(0.7)
