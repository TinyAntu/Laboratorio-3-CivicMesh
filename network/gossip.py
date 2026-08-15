from __future__ import annotations

import time
import uuid
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

    def build_message(self) -> Message:
        return Message(
            type="MEMBERSHIP_GOSSIP",
            sender_id=self.membership.self_peer.node_id,
            msg_id=str(uuid.uuid4()),
            payload={"members": self.membership.gossip_view()},
            ttl=1,
            priority=100,
        )

    def round(self) -> int:
        sent = 0
        message = self.build_message()

        for peer in self.membership.select_gossip_targets():
            try:
                self.send(peer, message)
                sent += 1
            except OSError:
                # The failure detector will determine whether the peer is down.
                pass

        self.membership.run_failure_check()
        self.membership.remove_failed()
        return sent

    def handle(self, message: Message) -> int:
        if message.type != "MEMBERSHIP_GOSSIP":
            return 0

        members = message.payload.get("members", [])
        return self.membership.merge(members)
