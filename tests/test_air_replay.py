import pytest

from domains.air.replay import AirQualityDataset, AirQualityReplay
from domains.air.replay import AirQualityReplay


def test_air_replay_is_sequential_and_deterministic():
    dataset = AirQualityDataset.from_csv("data/air_quality/santiago.csv")
    replay = AirQualityReplay(dataset)

    first = replay.next_sample()
    second = replay.next_sample()

    assert first.time == "2026-07-01T00:00"
    assert second.time == "2026-07-01T01:00"


def test_air_replay_raises_after_last_sample():
    dataset = AirQualityDataset.from_csv("data/air_quality/santiago.csv")
    replay = AirQualityReplay(dataset)
    for _ in range(len(dataset)):
        replay.next_sample()
    with pytest.raises(StopIteration):
        replay.next_sample()
