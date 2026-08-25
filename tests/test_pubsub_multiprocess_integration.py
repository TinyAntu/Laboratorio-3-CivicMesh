from __future__ import annotations

import multiprocessing as mp
import os
from queue import Empty
import socket
import time
from typing import Any

from network.messages import CHANNEL_OBJECTIVE, PeerInfo
from network.peer import Peer


TOPIC = "Santiago"
EXPECTED_VALUE = {"delitos": 5}


def _free_tcp_ports(count: int) -> list[int]:
    """Obtiene puertos TCP distintos para aislar la prueba de otras ejecuciones."""
    ports: set[int] = set()

    while len(ports) < count:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            ports.add(int(sock.getsockname()[1]))

    return list(ports)


def _peer_worker(
    node_id: str,
    port: int,
    role: str,
    seed_peer: dict[str, Any] | None,
    command_queue,
    result_queue,
    runs_dir: str,
) -> None:
    """Ejecuta un Peer real dentro de un proceso independiente."""

    peer = Peer(
        node_id=node_id,
        host="127.0.0.1",
        port=port,
        fanout=1,
        max_view_size=4,
        pubsub_fanout_objective=1,
        pubsub_fanout_subjective=1,
        failure_timeout=30.0,
        suspect_timeout=30.0,
        seed=port,
        runs_dir=runs_dir,
        run_id=f"multiprocess-integration-{node_id}",
    )

    if role == "subscriber":
        peer.subscribe(TOPIC)

        def _capture_delivery(message) -> None:
            entry = peer.state.get(
                TOPIC,
                CHANNEL_OBJECTIVE,
                "value",
            )

            result_queue.put(
                {
                    "event": "delivery",
                    "node_id": node_id,
                    "pid": os.getpid(),
                    "sender_id": message.sender_id,
                    "source_id": message.payload.get("source_id"),
                    "hop_count": message.hop_count,
                    "topic": message.payload.get("topic"),
                    "channel": message.payload.get("channel"),
                    "value": message.payload.get("value"),
                    "state": None
                    if entry is None
                    else {
                        "value": entry.value,
                        "source_id": entry.source_id,
                        "msg_id": entry.msg_id,
                    },
                }
            )

        peer.on_message(_capture_delivery)

    try:
        peer.start()

        if seed_peer is not None:
            peer.join([PeerInfo.from_dict(seed_peer)])

        result_queue.put(
            {
                "event": "ready",
                "node_id": node_id,
                "pid": os.getpid(),
            }
        )

        while True:
            try:
                command = command_queue.get(timeout=0.1)
            except Empty:
                continue

            action = command.get("action")

            if action == "publish":
                peer.publish(
                    topic=TOPIC,
                    channel=CHANNEL_OBJECTIVE,
                    value=EXPECTED_VALUE,
                    ttl=3,
                    priority=80,
                )

            elif action == "membership":
                result_queue.put(
                    {
                        "event": "membership",
                        "node_id": node_id,
                        "pid": os.getpid(),
                        "peers": {
                            peer_id: info.to_dict()
                            for peer_id, info in peer.membership.peers.items()
                        },
                    }
                )

            elif action == "stop":
                break

    finally:
        peer.stop()


def _wait_for_event(
    result_queue,
    event_name: str,
    node_id: str | None = None,
    timeout: float = 8.0,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        remaining = max(0.01, deadline - time.monotonic())

        try:
            event = result_queue.get(timeout=remaining)
        except Empty:
            break

        if event.get("event") != event_name:
            continue

        if node_id is not None and event.get("node_id") != node_id:
            continue

        return event

    raise AssertionError(
        f"No se recibió event={event_name!r} "
        f"para node_id={node_id!r} dentro del timeout"
    )


def _wait_until_relay_knows_both_neighbors(
    command_queue,
    result_queue,
    timeout: float = 8.0,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        command_queue.put({"action": "membership"})

        snapshot = _wait_for_event(
            result_queue,
            "membership",
            node_id="p2",
            timeout=min(1.0, max(0.1, deadline - time.monotonic())),
        )

        if {"p1", "p3"}.issubset(snapshot["peers"]):
            return snapshot

        time.sleep(0.05)

    raise AssertionError(
        "p2 no llegó a conocer simultáneamente a p1 y p3"
    )


def test_pubsub_three_real_processes_end_to_end(tmp_path):
    """
    Integración multiproceso real:

        proceso p1 (publisher) -> proceso p2 (relay) -> proceso p3 (subscriber)

    Verifica recepción, hop intermedio y estado agregado del suscriptor.
    """

    ctx = mp.get_context("spawn")

    p1_port, p2_port, p3_port = _free_tcp_ports(3)

    result_queue = ctx.Queue()
    p1_commands = ctx.Queue()
    p2_commands = ctx.Queue()
    p3_commands = ctx.Queue()

    p2_info = PeerInfo(
        node_id="p2",
        host="127.0.0.1",
        port=p2_port,
    ).to_dict()

    processes = [
        ctx.Process(
            target=_peer_worker,
            args=(
                "p2",
                p2_port,
                "relay",
                None,
                p2_commands,
                result_queue,
                str(tmp_path),
            ),
            name="civicmesh-test-p2",
        ),
        ctx.Process(
            target=_peer_worker,
            args=(
                "p3",
                p3_port,
                "subscriber",
                p2_info,
                p3_commands,
                result_queue,
                str(tmp_path),
            ),
            name="civicmesh-test-p3",
        ),
        ctx.Process(
            target=_peer_worker,
            args=(
                "p1",
                p1_port,
                "publisher",
                p2_info,
                p1_commands,
                result_queue,
                str(tmp_path),
            ),
            name="civicmesh-test-p1",
        ),
    ]

    try:
        # Levantar primero el relay para que p1 y p3 puedan hacer JOIN contra él.
        processes[0].start()
        ready_p2 = _wait_for_event(result_queue, "ready", "p2")

        processes[1].start()
        ready_p3 = _wait_for_event(result_queue, "ready", "p3")

        processes[2].start()
        ready_p1 = _wait_for_event(result_queue, "ready", "p1")

        # Evidencia explícita de que son tres procesos del SO diferentes.
        child_pids = {
            ready_p1["pid"],
            ready_p2["pid"],
            ready_p3["pid"],
        }

        assert len(child_pids) == 3
        assert os.getpid() not in child_pids

        # Asegurar que p2 puede actuar realmente como nodo intermedio.
        relay_membership = _wait_until_relay_knows_both_neighbors(
            p2_commands,
            result_queue,
        )
        assert set(relay_membership["peers"]) >= {"p1", "p3"}

        # El publisher existe en un proceso separado y solo conoce al relay p2.
        p1_commands.put({"action": "publish"})

        delivery = _wait_for_event(
            result_queue,
            "delivery",
            node_id="p3",
            timeout=8.0,
        )

        # El suscriptor recibió el valor correcto.
        assert delivery["topic"] == TOPIC
        assert delivery["channel"] == CHANNEL_OBJECTIVE
        assert delivery["value"] == EXPECTED_VALUE

        # El mensaje nació en p1 pero p3 lo recibió desde p2:
        # esto prueba el reenvío real por un proceso intermedio.
        assert delivery["source_id"] == "p1"
        assert delivery["sender_id"] == "p2"
        assert delivery["hop_count"] == 1

        # El estado agregado local del proceso suscriptor refleja el dato esperado.
        assert delivery["state"] is not None
        assert delivery["state"]["value"] == EXPECTED_VALUE
        assert delivery["state"]["source_id"] == "p1"

    finally:
        for queue in (p1_commands, p2_commands, p3_commands):
            try:
                queue.put({"action": "stop"})
            except Exception:
                pass

        for process in processes:
            if process.pid is None:
                continue

            process.join(timeout=3.0)

            if process.is_alive():
                process.terminate()
                process.join(timeout=2.0)
