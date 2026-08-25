from __future__ import annotations
import uuid
import random
from typing import Callable
from .membership import Membership
from .messages import Message
class Gossip:
    """Coordinador de membresia gossip.

    El transporte se inyecta mediante `send(peer, message)`, 
    lo que mantiene el protocolo independiente de los sockets y facilita las pruebas.
    """

    def __init__(
        self,
        membership: Membership,
        send: Callable,
        interval: float = 2.0,
    ):

        self.membership = membership
        self.send = send
        self.interval = interval
        self.running = False

    # Construye un mensaje con la informacion de membresia local.
    def build_message(self) -> Message:

        return Message(
            type="MEMBERSHIP_GOSSIP",
            sender_id=self.membership.self_peer.node_id,
            # Se utiliza random.getrandbits(128) en lugar de uuid.uuid4() para respetar el determinismo estocastico del modulo random.
            msg_id=str(uuid.UUID(int=random.getrandbits(128), version=4)),
            payload={"members": self.membership.gossip_view()},
            ttl=1,
            priority=100,
        )

    # Ejecuta una ronda de gossip.
    def round(self) -> int:

        # Contador de mensajes enviados correctamente.
        sent = 0
        message = self.build_message()

        for peer in self.membership.select_gossip_targets():

            try:
                self.send(peer, message)
                sent += 1

            except OSError:
                pass

        # La detección de fallos se ejecuta en Peer._gossip_loop(), donde el
        # cambio suspect/failed puede registrarse en métricas antes de cualquier
        # limpieza. Gossip.round() se limita al intercambio de membresía.
        return sent

    # Procesa un mensaje recibido.
    def handle(self, message: Message) -> int:

        if message.type != "MEMBERSHIP_GOSSIP":
            return 0

        members = message.payload.get("members", [])


        return self.membership.merge(members)