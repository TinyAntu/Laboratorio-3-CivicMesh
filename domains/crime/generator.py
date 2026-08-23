from __future__ import annotations

import math
import random
from dataclasses import dataclass


@dataclass(frozen=True)
class CrimeEvent:
    commune: str
    crime_type: str
    count: int
    timestamp: float


class CrimeGenerator:
    """Genera X_c,k(t) ~ Poisson(lambda_c,k * delta_t)."""

    def __init__(
        self,
        rates: dict[str, dict[str, float]],
        delta_t: float,
        seed: int,
    ) -> None:
        if delta_t <= 0:
            raise ValueError("delta_t debe ser mayor que 0")

        self.rates = rates
        self.delta_t = float(delta_t)
        self.seed = int(seed)
        self._rngs: dict[tuple[str, str], random.Random] = {}

        for commune, crime_rates in rates.items():
            for crime_type, rate in crime_rates.items():
                if rate < 0:
                    raise ValueError("Las tasas Poisson no pueden ser negativas")
                self._rngs[(commune, crime_type)] = random.Random(
                    f"{self.seed}:crime:{commune}:{crime_type}"
                )

    @staticmethod
    def _poisson(rng: random.Random, mean: float) -> int:
        """Muestreo Poisson de Knuth; adecuado para las tasas pequeñas del laboratorio."""
        if mean < 0:
            raise ValueError("La media Poisson no puede ser negativa")
        if mean == 0:
            return 0

        limit = math.exp(-mean)
        k = 0
        product = 1.0
        while product > limit:
            k += 1
            product *= rng.random()
        return k - 1

    def generate(self, commune: str, timestamp: float) -> list[CrimeEvent]:
        if commune not in self.rates:
            raise KeyError(f"Comuna sin tasas configuradas: {commune}")

        events: list[CrimeEvent] = []
        for crime_type, rate in self.rates[commune].items():
            mean = rate * self.delta_t
            count = self._poisson(self._rngs[(commune, crime_type)], mean)
            events.append(
                CrimeEvent(
                    commune=commune,
                    crime_type=crime_type,
                    count=count,
                    timestamp=float(timestamp),
                )
            )
        return events

    @staticmethod
    def total(events: list[CrimeEvent]) -> int:
        return sum(event.count for event in events)
