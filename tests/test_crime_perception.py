import pytest

from domains.config import CrimePerceptionConfig
from domains.crime.perception import CrimePerceptionModel


def make_config() -> CrimePerceptionConfig:
    return CrimePerceptionConfig(
        alpha=0.8,
        beta0=-1.0,
        beta1=0.4,
        beta2=0.8,
        sigma_epsilon=0.1,
    )


def test_crime_perception_is_reproducible():
    m1 = CrimePerceptionModel(make_config(), seed=42)
    m2 = CrimePerceptionModel(make_config(), seed=42)

    values1 = [m1.update("Santiago", 3, 0.4) for _ in range(10)]
    values2 = [m2.update("Santiago", 3, 0.4) for _ in range(10)]
    assert values1 == values2


def test_crime_perception_stays_in_unit_interval():
    model = CrimePerceptionModel(make_config(), seed=42)
    value = model.update("Santiago", crime_count=5, gossip_value=0.8)
    assert 0.0 <= value <= 1.0
    assert model.memory("Santiago") == pytest.approx(1.0)
