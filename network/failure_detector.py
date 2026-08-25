from __future__ import annotations
from dataclasses import dataclass
import time
@dataclass
class FailureState:

    # Guarda el instante en que se recibió por última vez una señal
    # de vida (heartbeat) del nodo.
    last_seen: float

    # Indica el estado actual del nodo, que por defecto todo nodo observado comienza como alive.
    # Los posibles estados utilizados por el detector son alive, suspect y failed.
    status: str = "alive"

    # Cuenta la cantidad de veces que el detector ha considerado que el nodo perdio una señal.
    # Comienza en 0 y aumenta cuando el nodo pasa a suspect o posteriormente a failed.
    missed: int = 0


class FailureDetector:
    """Detector de fallos basado en tiempo de espera.

    Un nodo se marca como sospechoso tras transcurrir cierta cantidad de segundos sin recibir una señal de vida (heartbeat).
    Tras otros segundos adicionales, se marca como fallido.
    """

    # Constructor de la clase FailureDetector.
    #
    # Timeout: Tiempo maximo que puede pasar sin recibir un heartbeat antes de considerar al nodo como sospechoso.
    # Suspect_timeout: Tiempo adicional que debe pasar desde que el nodo fue marcado como sospechoso antes de considerarlo fallido.
    def __init__(self, timeout: float = 5.0, suspect_timeout: float | None = None):

        if timeout <= 0:
            raise ValueError("timeout debe ser > 0")

        self.timeout = timeout
        self.suspect_timeout = suspect_timeout if suspect_timeout is not None else timeout

        # Diccionario que almacena el estado de cada nodo.
        self._states: dict[str, FailureState] = {}


    # Registra que un nodo esta activo porque se recibio una señal de vida (heartbeat).
    #
    # Node_id: Identificador unico del nodo observado.
    # Now: Tiempo en el que se recibio el heartbeat.
    def observe(self, node_id: str, now: float | None = None) -> None:

        now = time.monotonic() if now is None else now

        # Registra o actualiza el estado del nodo.
        self._states[node_id] = FailureState(last_seen=now, status="alive", missed=0)


    # Obtiene el estado actual de un nodo.
    def status(self, node_id: str) -> str:

        state = self._states.get(node_id)
        return state.status if state else "unknown"


    # Revisa todos los nodos registrados para determinar si alguno ha dejado de enviar heartbeats durante demasiado tiempo.
    def check(self, now: float | None = None) -> dict[str, str]:

        now = time.monotonic() if now is None else now

        # Diccionario donde se almacenarán los cambios de estado
        changed: dict[str, str] = {}

        for node_id, state in self._states.items():

            age = now - state.last_seen

            # Se evalua primero el umbral mayor para permitir que un nodo inactivo por mucho tiempo pase directamente a "failed".
            if state.status in ("alive", "suspect") and age > self.timeout + self.suspect_timeout:

                state.status = "failed"

                state.missed += 1

                changed[node_id] = "failed"

            elif state.status == "alive" and age > self.timeout:

                state.status = "suspect"

                state.missed += 1

                changed[node_id] = "suspect"

        return changed


    def get_state(self, node_id: str) -> FailureState | None:
        """Retorna una copia del estado conocido de un nodo, si existe."""
        state = self._states.get(node_id)
        if state is None:
            return None
        return FailureState(
            last_seen=state.last_seen,
            status=state.status,
            missed=state.missed,
        )


    # Elimina completamente un nodo del detector.
    def remove(self, node_id: str) -> None:
        self._states.pop(node_id, None)


    # Devuelve una copia simplificada de la información almacenada sobre todos los nodos.
    def snapshot(self) -> dict[str, dict]:

        return {
            node_id: {
                "last_seen": state.last_seen,
                "status": state.status,
                "missed": state.missed,
            }
            for node_id, state in self._states.items()
        }