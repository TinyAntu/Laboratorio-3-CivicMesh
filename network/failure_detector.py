from __future__ import annotations

from dataclasses import dataclass
import time


@dataclass
class FailureState:
    last_seen: float
    status: str = "alive"
    missed: int = 0


class FailureDetector:
    """Detector de fallos basado en tiempo de espera.

    Un nodo se marca como sospechoso tras transcurrir *timeout* segundos sin recibir una señal de vida (*heartbeat*).
    Tras otros *suspect_timeout* segundos adicionales, se marca como fallido.
    """

    def __init__(self, timeout: float = 5.0, suspect_timeout: float | None = None):
        if timeout <= 0:
            raise ValueError("timeout debe ser > 0")
        self.timeout = timeout
        self.suspect_timeout = suspect_timeout if suspect_timeout is not None else timeout
        self._states: dict[str, FailureState] = {}

    def observe(self, node_id: str, now: float | None = None) -> None:
        now = time.monotonic() if now is None else now
        self._states[node_id] = FailureState(last_seen=now, status="alive", missed=0)

    def status(self, node_id: str) -> str:
        state = self._states.get(node_id)
        return state.status if state else "unknown"

    def check(self, now: float | None = None) -> dict[str, str]:
        now = time.monotonic() if now is None else now
        changed: dict[str, str] = {}

        for node_id, state in self._states.items():
            age = now - state.last_seen

            if state.status == "alive" and age > self.timeout:
                state.status = "suspect"
                state.missed += 1
                changed[node_id] = "suspect"

            elif state.status == "suspect" and age > self.timeout + self.suspect_timeout:
                state.status = "failed"
                state.missed += 1
                changed[node_id] = "failed"

        return changed

    def remove(self, node_id: str) -> None:
        self._states.pop(node_id, None)

    def snapshot(self) -> dict[str, dict]:
        return {
            node_id: {
                "last_seen": state.last_seen,
                "status": state.status,
                "missed": state.missed,
            }
            for node_id, state in self._states.items()
        }
