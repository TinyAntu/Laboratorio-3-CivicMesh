from __future__ import annotations

import random

from domains.config import AirPerceptionConfig


class AirPerceptionModel:
    """Modelo subjetivo de contaminación con memoria de picos y rumor."""

    def __init__(self, config: AirPerceptionConfig, seed: int) -> None:
        self.config = config
        self.seed = int(seed)
        self._memory: dict[str, float] = {}
        self._rngs: dict[str, random.Random] = {}

    def memory(self, commune: str) -> float:
        return self._memory.get(commune, 0.0)

    def _rng(self, commune: str) -> random.Random:
        if commune not in self._rngs:
            self._rngs[commune] = random.Random(
                f"{self.seed}:air-perception:{commune}"
            )
        return self._rngs[commune]

    def update(self, commune: str, objective_value: float, gossip_value: float) -> float:
        objective = float(objective_value)
        previous_memory = self.memory(commune)

        stimulus = max(objective, previous_memory)
        memory = (
            self.config.alpha * previous_memory
            + (1.0 - self.config.alpha) * stimulus
        )
        self._memory[commune] = memory

        epsilon = self._rng(commune).gauss(0.0, self.config.sigma_epsilon)
        perception = (
            objective
            + self.config.gamma * (memory - objective)
            + self.config.delta * float(gossip_value)
            + epsilon
        )
        return max(self.config.clip_min, min(self.config.clip_max, perception))
