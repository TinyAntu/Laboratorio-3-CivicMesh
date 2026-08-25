import time
from network.peer import Peer
from network.messages import PeerInfo, CHANNEL_OBJECTIVE, CHANNEL_SUBJECTIVE


def test_pubsub_three_peers_end_to_end():
    # peer1 (publicador/emisor en 9101)
    # peer2 (nodo intermedio/reenviador en 9102)
    # peer3 (suscriptor destino en 9103, suscrito a "Santiago")

    p1 = Peer(node_id="p1", host="127.0.0.1", port=9101, pubsub_fanout=2, failure_timeout=10.0, seed=1)
    p2 = Peer(node_id="p2", host="127.0.0.1", port=9102, pubsub_fanout=2, failure_timeout=10.0, seed=2)
    p3 = Peer(node_id="p3", host="127.0.0.1", port=9103, pubsub_fanout=2, failure_timeout=10.0, seed=3)

    p3_received = []
    p3.on_message(lambda msg: p3_received.append(msg))
    p3.subscribe("Santiago")

    try:
        p1.start()
        p2.start()
        p3.start()

        # p2 se conecta a p1 y p3
        p2.join([p1.info, p3.info])
        # p1 se conecta a p2
        p1.join([p2.info])
        # p3 se conecta a p2
        p3.join([p2.info])

        time.sleep(0.3)

        # p1 publica un evento en el canal objetivo para la comuna "Santiago" con TTL=3
        p1.publish(
            topic="Santiago",
            channel=CHANNEL_OBJECTIVE,
            value={"delitos": 5},
            ttl=3,
            priority=80,
        )

        # Esperar propagación en la red TCP
        time.sleep(0.5)

        # Verificar que p3 recibió el mensaje
        assert len(p3_received) == 1
        msg = p3_received[0]
        assert msg.payload["topic"] == "Santiago"
        assert msg.payload["channel"] == CHANNEL_OBJECTIVE
        assert msg.payload["value"] == {"delitos": 5}

        # El suscriptor también debe reflejar el valor en su estado agregado local.
        state_entry = p3.state.get("Santiago", CHANNEL_OBJECTIVE, "value")
        assert state_entry is not None
        assert state_entry.value == {"delitos": 5}
        assert state_entry.source_id == "p1"

    finally:
        p1.stop()
        p2.stop()
        p3.stop()
