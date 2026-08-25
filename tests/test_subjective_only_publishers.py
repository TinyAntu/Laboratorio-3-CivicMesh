from types import SimpleNamespace

from domains.air.publisher import AirQualityPublisher
from domains.crime.publisher import CrimePublisher
from network.messages import CHANNEL_OBJECTIVE, CHANNEL_SUBJECTIVE


class FakePeer:
    def __init__(self, node_id: str) -> None:
        self.info = SimpleNamespace(node_id=node_id)
        self.metrics = None
        self.published = []
        self.handler = None

    def on_message(self, handler) -> None:
        self.handler = handler

    def publish(self, **kwargs):
        self.published.append(kwargs)


class FakeCrimeGenerator:
    def generate(self, commune: str, timestamp: float):
        return [
            SimpleNamespace(
                commune=commune,
                crime_type="robo",
                count=2,
                timestamp=timestamp,
            ),
            SimpleNamespace(
                commune=commune,
                crime_type="hurto",
                count=1,
                timestamp=timestamp,
            ),
        ]

    @staticmethod
    def total(events) -> int:
        return sum(event.count for event in events)


class FakePerception:
    def __init__(self, value: float) -> None:
        self.value = value

    def update(self, **kwargs) -> float:
        return self.value

    def memory(self, commune: str) -> float:
        return 1.0


class FakeAirSample:
    time = "2026-07-01T00:00"
    pm2_5 = 20.0
    pm10 = 30.0

    def epoch_timestamp(self, utc_offset_seconds: int) -> float:
        return 1000.0


class FakeAirReplay:
    def __init__(self) -> None:
        self.dataset = SimpleNamespace(
            metadata=SimpleNamespace(
                utc_offset_seconds=-14400,
                latitude=-33.5,
                longitude=-70.6,
            )
        )

    def next_sample(self):
        return FakeAirSample()


def test_crime_subjective_only_does_not_publish_objective():
    peer = FakePeer("rumor_santiago_1")
    publisher = CrimePublisher(
        peer=peer,
        commune="Santiago",
        generator=FakeCrimeGenerator(),
        perception_model=FakePerception(0.7),
        delta_t=1.0,
        subjective_only=True,
    )

    total, perception = publisher.run_step()

    assert total == 3
    assert perception == 0.7
    assert not any(
        item["channel"] == CHANNEL_OBJECTIVE
        for item in peer.published
    )
    subjective = [
        item for item in peer.published
        if item["channel"] == CHANNEL_SUBJECTIVE
    ]
    assert len(subjective) == 1
    assert subjective[0]["metadata"]["source_role"] == "subjective_only"


def test_air_subjective_only_does_not_publish_objective():
    peer = FakePeer("rumor_santiago_1")
    publisher = AirQualityPublisher(
        peer=peer,
        commune="Santiago",
        replay=FakeAirReplay(),
        perception_model=FakePerception(25.0),
        pollutant="pm2_5",
        subjective_only=True,
    )

    objective, perception = publisher.run_step()

    assert objective == 20.0
    assert perception == 25.0
    assert not any(
        item["channel"] == CHANNEL_OBJECTIVE
        for item in peer.published
    )
    subjective = [
        item for item in peer.published
        if item["channel"] == CHANNEL_SUBJECTIVE
    ]
    assert len(subjective) == 1
    assert subjective[0]["metadata"]["source_role"] == "subjective_only"
