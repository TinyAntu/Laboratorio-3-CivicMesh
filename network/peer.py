from __future__ import annotations
import argparse
import json
import socket
import threading
import time
from typing import Any
from .gossip import Gossip
from .membership import Membership, MembershipConfig
from .messages import Message, PeerInfo

# Abre una conexion TCP con el nodo destino.
def send_json(host: str, port: int, message: Message, timeout: float = 2.0) -> None:
    with socket.create_connection((host, port), timeout=timeout) as conn:
        # Codifica el mensaje y lo envia por la conexion.
        conn.sendall(message.encode())

class Peer:
    """Peer TCP que revisa las membresias, el gossip y la escucha de la red."""

    def __init__(
        self,
        node_id: str,
        host: str,
        port: int,
        fanout: int = 2,
        failure_timeout: float = 5.0,
        suspect_timeout: float = 5.0,
        seed: int | None = None,
    ):
        # Inicializa un nodo de la red.
        self.info = PeerInfo(node_id=node_id, host=host, port=port)
        self.membership = Membership(
            self.info,
            MembershipConfig(
                gossip_fanout=fanout,
                failure_timeout=failure_timeout,
                suspect_timeout=suspect_timeout,
            ),
            seed=seed,
        )
        self.gossip = Gossip(self.membership, self._send_peer)
        self.running = False
        self.server: socket.socket | None = None

    # Envia un mensaje a un nodo especifico.
    def _send_peer(self, peer: PeerInfo, message: Message) -> None:
        send_json(peer.host, peer.port, message)

    # Enviar un mensaje a otro nodo.
    def send(self, peer: PeerInfo, message: Message) -> None:
        self._send_peer(peer, message)

    # Conecta el nodo con los nodos iniciales de la red.
    def join(self, seeds: list[PeerInfo]) -> None:

        for seed in seeds:

            if seed.node_id == self.info.node_id:
                continue

            self.membership.add_peer(seed)

            msg = Message(
                type="JOIN",
                sender_id=self.info.node_id,
                msg_id=f"join-{self.info.node_id}-{time.time_ns()}",
                payload={"peer": self.info.to_dict()},
                ttl=1,
                priority=100,
            )

            try:
                self.send(seed, msg)

            except OSError:
                pass

    # Inicia el servidor TCP y los procesos de gossip.
    def start(self) -> None:

        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind((self.info.host, self.info.port))
        self.server.listen(64)
        self.running = True
        threading.Thread(target=self._serve_loop, daemon=True).start()
        threading.Thread(target=self._gossip_loop, daemon=True).start()

        print(
            f"[peer] node={self.info.node_id} "
            f"listen={self.info.host}:{self.info.port} "
            f"fanout={self.membership.config.gossip_fanout}",
            flush=True,
        )

    # Detiene el nodo.
    def stop(self) -> None:

        self.running = False

        if self.server:

            try:
                self.server.close()
 
            except OSError:
                pass

    # Mantiene el servidor esperando conexiones.
    def _serve_loop(self) -> None:

        assert self.server is not None
        while self.running:

            try:
                conn, _ = self.server.accept()
  
            except OSError:
                break

            threading.Thread(
                target=self._handle_connection,
                args=(conn,),
                daemon=True,
            ).start()

    # Procesa un mensaje recibido desde otro nodo.
    def _handle_connection(self, conn: socket.socket) -> None:

        try:
            conn.settimeout(3)

            buffer = b""

            while b"\n" not in buffer:

                part = conn.recv(65536)

                if not part:
                    return

                buffer += part

            line, _, _ = buffer.partition(b"\n")

            message = Message.from_dict(json.loads(line.decode()))

            if message.type == "JOIN":

                peer = PeerInfo.from_dict(message.payload["peer"])
                self.membership.add_peer(peer)
                self._reply(conn, "JOIN_ACK", {"members": self.membership.gossip_view()})

            elif message.type == "PING":

                self.membership.mark_seen(message.sender_id)
                self._reply(conn, "PONG", {"node_id": self.info.node_id})

            elif message.type == "MEMBERSHIP_GOSSIP":

                self.membership.mark_seen(message.sender_id)
                self.gossip.handle(message)
                self._reply(conn, "GOSSIP_ACK", {"node_id": self.info.node_id})

            else:
                self._reply(
                    conn,
                    "ERROR",
                    {"error": f"unsupported message type: {message.type}"},
                )

        except (OSError, ValueError, KeyError, json.JSONDecodeError):
            pass

        finally:
            try:
                conn.close()

            except OSError:
                pass

    # Construye y envia una respuesta al nodo que realizo la solicitud.
    def _reply(self, conn: socket.socket, message_type: str, payload: dict[str, Any]) -> None:

        msg = Message(
            type=message_type,
            sender_id=self.info.node_id,
            msg_id=f"reply-{time.time_ns()}",
            payload=payload,
            ttl=1,
            priority=100,
        )

        conn.sendall(msg.encode())

    # Ejecuta el gossip periodicamente.
    def _gossip_loop(self) -> None:

        while self.running:

            time.sleep(self.gossip.interval)

            if self.running:

                sent = self.gossip.round()
                changes = self.membership.run_failure_check()

                if sent or changes:

                    print(
                        f"[gossip] sent={sent} changes={changes} "
                        f"view={list(self.membership.peers)}",
                        flush=True,
                    )

# Carga los nodos iniciales desde un archivo.
def load_peers(path: str) -> list[PeerInfo]:

    peers = []

    with open(path, encoding="utf-8") as fh:

        for line in fh:

            line = line.strip()

            if not line or line.startswith("#"):
                continue

            node_id, host, port = line.split()
            peers.append(PeerInfo(node_id=node_id, host=host, port=int(port)))

    return peers

# Funcion principal encargada de configurar e iniciar el nodo.
def main() -> int:

    parser = argparse.ArgumentParser(description="CivicMesh network peer")
    parser.add_argument("--node-id", required=True)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--seeds-file")
    parser.add_argument("--fanout", type=int, default=2)
    parser.add_argument("--failure-timeout", type=float, default=5.0)
    parser.add_argument("--suspect-timeout", type=float, default=5.0)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    peer = Peer(
        args.node_id,
        args.host,
        args.port,
        fanout=args.fanout,
        failure_timeout=args.failure_timeout,
        suspect_timeout=args.suspect_timeout,
        seed=args.seed,
    )

    peer.start()

    if args.seeds_file:
        peer.join(load_peers(args.seeds_file))

    try:
        while True:

            time.sleep(1)

    except KeyboardInterrupt:

        peer.stop()

    return 0

if __name__ == "__main__":
    raise SystemExit(main())