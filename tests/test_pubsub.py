import time
from network.messages import (
    Message,
    PeerInfo,
    MSG_PUBLISH,
    CHANNEL_OBJECTIVE,
    CHANNEL_SUBJECTIVE,
    PRIORITY_HIGH,
    PRIORITY_LOW,
)
from network.pubsub import (
    PubSubConfig,
    PubSubEngine,
    Deduplicator,
    SubscriptionManager,
    ForwardPriorityQueue,
    select_forward_targets,
)
from network.topology import GeoTopology


def test_deduplicator_lru_and_expiry():
    dedup = Deduplicator(max_size=3, ttl_seconds=10.0)

    # Añadir m1, m2, m3 en orden
    assert dedup.mark_seen("m1", now=0) is True
    assert dedup.mark_seen("m2", now=0) is True
    assert dedup.mark_seen("m3", now=0) is True

    # Acceder a m1 (se mueve al final de la cola LRU: orden queda [m2, m3, m1])
    assert dedup.mark_seen("m1", now=0) is False

    # Al agregar m4 con max_size=3, el más antiguo no accedido es m2, por lo que m2 se desaloja
    assert dedup.mark_seen("m4", now=0) is True
    assert dedup.is_seen("m2", now=0) is False
    assert dedup.is_seen("m1", now=0) is True
    assert dedup.is_seen("m3", now=0) is True
    assert dedup.is_seen("m4", now=0) is True

    # Expiración por tiempo (> 10s)
    assert dedup.is_seen("m4", now=15.0) is False


def test_subscription_manager():
    sm = SubscriptionManager(self_node_id="p0")

    # Local
    assert sm.subscribe_local("Santiago") is True
    assert sm.subscribe_local("Santiago") is False
    assert sm.is_locally_subscribed("Santiago") is True
    assert sm.is_locally_subscribed("Providencia") is False

    assert sm.unsubscribe_local("Santiago") is True
    assert sm.is_locally_subscribed("Santiago") is False

    # Peers remotos
    sm.update_peer_topics("p1", ["Santiago", "Las Condes"])
    sm.add_peer_topic("p2", "Santiago")

    assert sm.get_subscribers_for_topic("Santiago") == {"p1", "p2"}
    assert sm.get_subscribers_for_topic("Las Condes") == {"p1"}

    sm.remove_peer("p1")
    assert sm.get_subscribers_for_topic("Santiago") == {"p2"}


def test_select_forward_targets_ordering():
    topo = GeoTopology()
    # Candidatos:
    # p1: suscrito a Santiago (directo)
    # p2: suscrito a Providencia (vecino de Santiago)
    # p3: suscrito a Lo Barnechea (no vecino directo de Santiago)
    # p4: sin tópicos
    p1 = PeerInfo("p1", "127.0.0.1", 9001, topics=["Santiago"])
    p2 = PeerInfo("p2", "127.0.0.1", 9002, topics=["Providencia"])
    p3 = PeerInfo("p3", "127.0.0.1", 9003, topics=["Lo Barnechea"])
    p4 = PeerInfo("p4", "127.0.0.1", 9004, topics=[])

    candidates = [p1, p2, p3, p4]
    msg = Message(
        type=MSG_PUBLISH,
        sender_id="p0",
        msg_id="m1",
        payload={"topic": "Santiago"},
        ttl=3,
        priority=50,
    )

    # Fanout = 2: debe seleccionar p1 (directo) y p2 (vecino)
    selected = select_forward_targets(
        msg=msg,
        topic="Santiago",
        candidates=candidates,
        fanout=2,
        topology=topo,
    )
    selected_ids = [p.node_id for p in selected]
    assert len(selected) == 2
    assert "p1" in selected_ids
    assert "p2" in selected_ids


def test_forward_priority_queue():
    queue = ForwardPriorityQueue()
    msg_low = Message(type="PUBLISH", sender_id="p1", msg_id="m-low", priority=10)
    msg_high = Message(type="PUBLISH", sender_id="p1", msg_id="m-high", priority=90)
    msg_med = Message(type="PUBLISH", sender_id="p1", msg_id="m-med", priority=50)

    queue.push(msg_low, [])
    queue.push(msg_high, [])
    queue.push(msg_med, [])

    # Debe salir primero msg_high, luego msg_med, luego msg_low
    assert queue.pop()[0].msg_id == "m-high"
    assert queue.pop()[0].msg_id == "m-med"
    assert queue.pop()[0].msg_id == "m-low"
    assert queue.pop() is None


def test_pubsub_engine_dual_channels():
    self_peer = PeerInfo("p0", "127.0.0.1", 9000)
    engine = PubSubEngine(
        self_peer=self_peer,
        send_fn=lambda peer, msg: None,
        config=PubSubConfig(
            default_ttl_objective=2,
            default_priority_objective=80,
            default_ttl_subjective=6,
            default_priority_subjective=30,
        ),
    )

    msg_obj = engine.create_publish_message("Santiago", CHANNEL_OBJECTIVE, 42.0)
    assert msg_obj.ttl == 2
    assert msg_obj.priority == 80
    assert msg_obj.payload["channel"] == CHANNEL_OBJECTIVE

    msg_sub = engine.create_publish_message("Santiago", CHANNEL_SUBJECTIVE, 75.5)
    assert msg_sub.ttl == 6
    assert msg_sub.priority == 30
    assert msg_sub.payload["channel"] == CHANNEL_SUBJECTIVE


def test_pubsub_engine_incoming_delivery_and_forwarding():
    self_peer = PeerInfo("p0", "127.0.0.1", 9000)
    sent_messages = []

    def mock_send(peer, msg):
        sent_messages.append((peer.node_id, msg))

    engine = PubSubEngine(
        self_peer=self_peer,
        send_fn=mock_send,
        config=PubSubConfig(pubsub_fanout=2),
    )

    received_local = []
    engine.register_handler(lambda msg: received_local.append(msg))

    # Suscribir localmente a Santiago
    engine.subscribe("Santiago")

    target_peer = PeerInfo("p2", "127.0.0.1", 9002, topics=["Santiago"])
    local_peers = {"p2": target_peer}

    incoming_msg = Message(
        type=MSG_PUBLISH,
        sender_id="p1",
        msg_id="msg-incoming-1",
        payload={"topic": "Santiago", "channel": CHANNEL_OBJECTIVE, "value": 100},
        ttl=3,
        priority=50,
        hop_count=0,
    )

    # Procesar mensaje entrante
    delivered, forwarded = engine.handle_incoming_message(incoming_msg, local_peers)

    # Verificar que se entregó localmente
    assert delivered is True
    assert len(received_local) == 1
    assert received_local[0].payload["value"] == 100

    # Verificar que se reenvió con TTL decrementado y hop_count incrementado
    assert forwarded == 1
    assert len(sent_messages) == 1
    dest_id, fwd_msg = sent_messages[0]
    assert dest_id == "p2"
    assert fwd_msg.ttl == 2
    assert fwd_msg.hop_count == 1
    assert fwd_msg.msg_id == "msg-incoming-1"

    # Segundo envío del mismo mensaje no debe procesarse ni reenviarse (deduplicado)
    delivered2, forwarded2 = engine.handle_incoming_message(incoming_msg, local_peers)
    assert delivered2 is False
    assert forwarded2 == 0
    assert len(sent_messages) == 1
