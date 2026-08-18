from network.messages import Message, PeerInfo, MSG_PUBLISH, CHANNEL_OBJECTIVE
from network.pubsub import should_forward, Deduplicator


def create_sample_message(
    msg_id: str = "msg-1",
    sender_id: str = "p1",
    ttl: int = 3,
    priority: int = 50,
    topic: str = "Santiago",
) -> Message:
    return Message(
        type=MSG_PUBLISH,
        sender_id=sender_id,
        msg_id=msg_id,
        payload={"topic": topic, "channel": CHANNEL_OBJECTIVE, "value": 10},
        ttl=ttl,
        priority=priority,
        hop_count=0,
    )


def test_should_forward_ttl_positive():
    msg = create_sample_message(ttl=3)
    local_view = {
        "p2": PeerInfo("p2", "127.0.0.1", 9002),
        "p3": PeerInfo("p3", "127.0.0.1", 9003),
    }
    assert should_forward(msg, "Santiago", local_view) is True


def test_should_forward_ttl_zero_or_negative():
    msg_zero = create_sample_message(ttl=0)
    msg_neg = create_sample_message(ttl=-1)
    local_view = {"p2": PeerInfo("p2", "127.0.0.1", 9002)}

    assert should_forward(msg_zero, "Santiago", local_view) is False
    assert should_forward(msg_neg, "Santiago", local_view) is False


def test_should_forward_rejects_duplicate():
    dedup = Deduplicator()
    msg = create_sample_message(msg_id="msg-dup-1")
    local_view = {"p2": PeerInfo("p2", "127.0.0.1", 9002)}

    # Primer chequeo con mensaje nuevo
    assert should_forward(msg, "Santiago", local_view, deduplicator=dedup) is True

    # Marcar como visto
    dedup.mark_seen("msg-dup-1")

    # Segundo chequeo debe rechazarlo
    assert should_forward(msg, "Santiago", local_view, deduplicator=dedup) is False


def test_should_forward_priority_filtering():
    msg_low = create_sample_message(priority=20)
    msg_high = create_sample_message(priority=80)
    local_view = {"p2": PeerInfo("p2", "127.0.0.1", 9002)}

    # Con umbral mínimo de 50
    assert should_forward(msg_low, "Santiago", local_view, min_priority=50) is False
    assert should_forward(msg_high, "Santiago", local_view, min_priority=50) is True


def test_should_forward_no_available_targets():
    msg = create_sample_message(sender_id="p1")

    # Vista vacía
    assert should_forward(msg, "Santiago", {}) is False

    # Vista solo contiene al emisor
    assert should_forward(msg, "Santiago", {"p1": PeerInfo("p1", "127.0.0.1", 9001)}) is False

    # Vista solo contiene nodo fallido
    failed_peer = PeerInfo("p2", "127.0.0.1", 9002, status="failed")
    assert should_forward(msg, "Santiago", {"p2": failed_peer}) is False
