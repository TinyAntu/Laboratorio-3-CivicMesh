from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class SimulationConfig:
    delta_t: float
    interval_seconds: float


@dataclass(frozen=True)
class PubSubChannelConfig:
    fanout: int
    ttl: int
    priority: int


@dataclass(frozen=True)
class PubSubRuntimeConfig:
    objective: PubSubChannelConfig
    subjective: PubSubChannelConfig


@dataclass(frozen=True)
class CrimeConfig:
    rates: dict[str, dict[str, float]]


@dataclass(frozen=True)
class CrimePerceptionConfig:
    alpha: float
    beta0: float
    beta1: float
    beta2: float
    sigma_epsilon: float


@dataclass(frozen=True)
class AirConfig:
    pollutant: str
    datasets: dict[str, str]


@dataclass(frozen=True)
class AirPerceptionConfig:
    alpha: float
    gamma: float
    delta: float
    sigma_epsilon: float
    clip_min: float
    clip_max: float


@dataclass(frozen=True)
class DataConfig:
    seed: int
    simulation: SimulationConfig
    pubsub: PubSubRuntimeConfig
    crime: CrimeConfig
    crime_perception: CrimePerceptionConfig
    air: AirConfig
    air_perception: AirPerceptionConfig


class ConfigLoader:
    @staticmethod
    def load(path: str | Path) -> DataConfig:
        config_path = Path(path)
        with config_path.open("r", encoding="utf-8") as fh:
            raw: dict[str, Any] = yaml.safe_load(fh)

        simulation = raw["simulation"]
        pubsub = raw["pubsub"]
        crime_perception = raw["crime_perception"]
        air_perception = raw["air_perception"]

        config = DataConfig(
            seed=int(raw["seed"]),
            simulation=SimulationConfig(
                delta_t=float(simulation["delta_t"]),
                interval_seconds=float(simulation["interval_seconds"]),
            ),
            pubsub=PubSubRuntimeConfig(
                objective=PubSubChannelConfig(
                    fanout=int(pubsub["objective"]["fanout"]),
                    ttl=int(pubsub["objective"]["ttl"]),
                    priority=int(pubsub["objective"]["priority"]),
                ),
                subjective=PubSubChannelConfig(
                    fanout=int(pubsub["subjective"]["fanout"]),
                    ttl=int(pubsub["subjective"]["ttl"]),
                    priority=int(pubsub["subjective"]["priority"]),
                ),
            ),
            crime=CrimeConfig(
                rates={
                    str(commune): {
                        str(crime_type): float(rate)
                        for crime_type, rate in crime_rates.items()
                    }
                    for commune, crime_rates in raw["crime"]["rates"].items()
                }
            ),
            crime_perception=CrimePerceptionConfig(
                alpha=float(crime_perception["alpha"]),
                beta0=float(crime_perception["beta0"]),
                beta1=float(crime_perception["beta1"]),
                beta2=float(crime_perception["beta2"]),
                sigma_epsilon=float(crime_perception["sigma_epsilon"]),
            ),
            air=AirConfig(
                pollutant=str(raw["air"]["pollutant"]),
                datasets={
                    str(commune): str(dataset_path)
                    for commune, dataset_path in raw["air"]["datasets"].items()
                },
            ),
            air_perception=AirPerceptionConfig(
                alpha=float(air_perception["alpha"]),
                gamma=float(air_perception["gamma"]),
                delta=float(air_perception["delta"]),
                sigma_epsilon=float(air_perception["sigma_epsilon"]),
                clip_min=float(air_perception["clip_min"]),
                clip_max=float(air_perception["clip_max"]),
            ),
        )
        ConfigLoader._validate(config)
        return config

    @staticmethod
    def _validate(config: DataConfig) -> None:
        if config.simulation.delta_t <= 0:
            raise ValueError("simulation.delta_t debe ser mayor que 0")
        if config.simulation.interval_seconds < 0:
            raise ValueError("simulation.interval_seconds no puede ser negativo")

        for channel_name, channel in (
            ("objective", config.pubsub.objective),
            ("subjective", config.pubsub.subjective),
        ):
            if channel.fanout <= 0:
                raise ValueError(
                    f"pubsub.{channel_name}.fanout debe ser mayor que 0"
                )
            if channel.ttl <= 0:
                raise ValueError(
                    f"pubsub.{channel_name}.ttl debe ser mayor que 0"
                )
            if channel.priority < 0:
                raise ValueError(
                    f"pubsub.{channel_name}.priority no puede ser negativa"
                )

        if not 0 < config.crime_perception.alpha < 1:
            raise ValueError("crime_perception.alpha debe estar en (0, 1)")
        if config.crime_perception.sigma_epsilon < 0:
            raise ValueError("crime_perception.sigma_epsilon no puede ser negativo")

        if not 0 < config.air_perception.alpha < 1:
            raise ValueError("air_perception.alpha debe estar en (0, 1)")
        if config.air_perception.sigma_epsilon < 0:
            raise ValueError("air_perception.sigma_epsilon no puede ser negativo")
        if config.air_perception.clip_min >= config.air_perception.clip_max:
            raise ValueError("air_perception.clip_min debe ser menor que clip_max")

        for commune, crime_rates in config.crime.rates.items():
            if not crime_rates:
                raise ValueError(f"No hay tasas de delito configuradas para {commune}")
            for crime_type, rate in crime_rates.items():
                if rate < 0:
                    raise ValueError(
                        f"La tasa de {crime_type} en {commune} no puede ser negativa"
                    )
