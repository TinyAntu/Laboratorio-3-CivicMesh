import pytest

from domains.air.perception import AirPerceptionModel
from domains.config import AirPerceptionConfig


def make_config() -> AirPerceptionConfig:
    return AirPerceptionConfig(
        alpha=0.85,
        gamma=0.6,
        delta=0.3,
        sigma_epsilon=0.0,
        clip_min=0.0,
        clip_max=500.0,
    )


def test_air_perception_retains_peak_memory():
    model = AirPerceptionModel(make_config(), seed=42)
    model.update("Santiago", objective_value=100.0, gossip_value=0.0)
    first_memory = model.memory("Santiago")
    model.update("Santiago", objective_value=10.0, gossip_value=0.0)
    second_memory = model.memory("Santiago")

    assert first_memory == pytest.approx(15.0)
    assert second_memory > 10.0


def test_air_perception_applies_gossip_and_clip():
    config = AirPerceptionConfig(
        alpha=0.85,
        gamma=0.6,
        delta=10.0,
        sigma_epsilon=0.0,
        clip_min=0.0,
        clip_max=50.0,
    )
    model = AirPerceptionModel(config, seed=42)
    value = model.update("Quilicura", objective_value=20.0, gossip_value=100.0)
    assert value == 50.0
