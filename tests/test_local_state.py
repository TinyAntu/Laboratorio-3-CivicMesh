from network.state import LocalAggregateState


def test_local_state_keeps_air_pollutants_separate():
    state = LocalAggregateState()

    state.update(
        topic="Santiago",
        channel="objective",
        value=20.5,
        timestamp=1.0,
        source_id="air-santiago",
        msg_id="m1",
        metadata={"domain": "air", "pollutant": "pm2_5"},
    )
    state.update(
        topic="Santiago",
        channel="objective",
        value=31.2,
        timestamp=1.0,
        source_id="air-santiago",
        msg_id="m2",
        metadata={"domain": "air", "pollutant": "pm10"},
    )

    assert state.get("Santiago", "objective", "pm2_5").value == 20.5
    assert state.get("Santiago", "objective", "pm10").value == 31.2


def test_local_state_keeps_channels_separate():
    state = LocalAggregateState()

    state.update(
        topic="Santiago",
        channel="objective",
        value=3,
        timestamp=2.0,
        source_id="crime-santiago",
        msg_id="m3",
        metadata={"domain": "crime", "crime_type": "robo"},
    )
    state.update(
        topic="Santiago",
        channel="subjective",
        value=0.72,
        timestamp=2.0,
        source_id="crime-santiago",
        msg_id="m4",
        metadata={"domain": "crime"},
    )

    assert state.get("Santiago", "objective", "robo").value == 3
    assert state.get("Santiago", "subjective", "perception").value == 0.72


def test_snapshot_is_a_copy():
    state = LocalAggregateState()
    state.update(
        topic="Quilicura",
        channel="objective",
        value=10.0,
        timestamp=3.0,
        source_id="air-quilicura",
        msg_id="m5",
        metadata={"domain": "air", "pollutant": "pm2_5"},
    )

    snapshot = state.snapshot()
    snapshot.clear()

    assert state.get("Quilicura", "objective", "pm2_5") is not None
