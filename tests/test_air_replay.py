from pathlib import Path

import pytest

from domains.air.replay import (
    AirQualityDataset,
    AirQualityReplay,
)


FIXTURE_PATH = (
    Path(__file__).parent
    / "fixtures"
    / "air_sample.csv"
)


def test_air_dataset_parses_minimal_fixture():
    dataset = AirQualityDataset.from_csv(
        FIXTURE_PATH
    )

    assert len(dataset) == 3

    assert dataset.metadata.latitude == -33.5
    assert dataset.metadata.longitude == -70.6
    assert dataset.metadata.elevation == 549.0
    assert dataset.metadata.utc_offset_seconds == -14400
    assert dataset.metadata.timezone == "America/Santiago"

    first = dataset.get(0)

    assert first.time == "2026-07-01T00:00"
    assert first.pm10 == 96.2
    assert first.pm2_5 == 93.6


def test_air_replay_is_sequential_and_deterministic():
    dataset = AirQualityDataset.from_csv(
        FIXTURE_PATH
    )

    replay = AirQualityReplay(dataset)

    first = replay.next_sample()
    second = replay.next_sample()
    third = replay.next_sample()

    assert first.time == "2026-07-01T00:00"
    assert first.pm10 == 96.2
    assert first.pm2_5 == 93.6

    assert second.time == "2026-07-01T01:00"
    assert second.pm10 == 90.6
    assert second.pm2_5 == 88.2

    assert third.time == "2026-07-01T02:00"
    assert third.pm10 == 85.2
    assert third.pm2_5 == 83.1


def test_air_replay_raises_after_last_sample():
    dataset = AirQualityDataset.from_csv(
        FIXTURE_PATH
    )

    replay = AirQualityReplay(dataset)

    for _ in range(len(dataset)):
        replay.next_sample()

    with pytest.raises(StopIteration):
        replay.next_sample()


def test_air_replay_reset_starts_from_beginning():
    dataset = AirQualityDataset.from_csv(
        FIXTURE_PATH
    )

    replay = AirQualityReplay(dataset)

    replay.next_sample()
    replay.next_sample()

    replay.reset()

    first_again = replay.next_sample()

    assert first_again.time == "2026-07-01T00:00"
    assert first_again.pm10 == 96.2
    assert first_again.pm2_5 == 93.6