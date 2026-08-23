from __future__ import annotations

import math
import random

from domains.config import CrimePerceptionConfig


class CrimePerceptionModel:
    """Modelo subjetivo de sensación de inseguridad del Dominio A."""

    def __init__(self, config: CrimePerceptionConfig, seed: int) -> None:
        self.config = config
        self.seed = int(seed)
        self._memory: dict[str, float] = {}
        self._rngs: dict[str, random.Random] = {}

    def memory(self, commune: str) -> float:
        return self._memory.get(commune, 0.0)

    def _rng(self, commune: str) -> random.Random:
        if commune not in self._rngs:
            self._rngs[commune] = random.Random(
                f"{self.seed}:crime-perception:{commune}"
            )
        return self._rngs[commune]

    @staticmethod
    def _sigmoid(z: float) -> float:
        if z >= 0:
            e = math.exp(-z)
            return 1.0 / (1.0 + e)
        e = math.exp(z)
        return e / (1.0 + e)

    def update(self, commune: str, crime_count: float, gossip_value: float) -> float:
        previous_memory = self.memory(commune)
        memory = (
            self.config.alpha * previous_memory
            + (1.0 - self.config.alpha) * float(crime_count)
        )
        self._memory[commune] = memory

        epsilon = self._rng(commune).gauss(0.0, self.config.sigma_epsilon)
        z = (
            self.config.beta0
            + self.config.beta1 * memory
            + self.config.beta2 * float(gossip_value)
            + epsilon
        )
        return self._sigmoid(z)
