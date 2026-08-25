from network.messages import (
    CHANNEL_OBJECTIVE,
    CHANNEL_SUBJECTIVE,
    MSG_PUBLISH,
    Message,
    PeerInfo,
)
from network.pubsub import PubSubConfig, PubSubEngine


def _peers(count: int) -> dict[str, PeerInfo]:
    return {
        f"p{i}": PeerInfo(
            node_id=f"p{i}",
            host="127.0.0.1",
            port=9100 + i,
            topics=["Santiago"],
        )
        for i in range(1, count + 1)
    }


def _message(msg_id: str, channel: str) -> Message:
    return Message(
        type=MSG_PUBLISH,
        sender_id="source",
        msg_id=msg_id,
        payload={
            "topic": "Santiago",
            "channel": channel,
            "value": 1.0,
        },
        ttl=4,
        priority=50,
    )


def test_pubsub_uses_different_fanout_per_channel():
    sent: list[tuple[str, str]] = []

    engine = PubSubEngine(
        self_peer=PeerInfo("self", "127.0.0.1", 9000),
        send_fn=lambda peer, msg: sent.append((peer.node_id, msg.msg_id)),
        config=PubSubConfig(
            fanout_objective=1,
            fanout_subjective=3,
        ),
        seed=42,
    )

    peers = _peers(4)

    _, forwarded_objective = engine.handle_incoming_message(
        _message("objective-1", CHANNEL_OBJECTIVE),
        peers,
    )
    _, forwarded_subjective = engine.handle_incoming_message(
        _message("subjective-1", CHANNEL_SUBJECTIVE),
        peers,
    )

    assert forwarded_objective == 1
    assert forwarded_subjective == 3


def test_legacy_pubsub_fanout_still_applies_to_both_channels():
    config = PubSubConfig(pubsub_fanout=2)

    assert config.fanout_for_channel(CHANNEL_OBJECTIVE) == 2
    assert config.fanout_for_channel(CHANNEL_SUBJECTIVE) == 2
