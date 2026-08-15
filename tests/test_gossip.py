from network.gossip import Gossip
from network.membership import Membership, MembershipConfig
from network.messages import PeerInfo, Message


def test_gossip_selects_fanout_and_sends():
    me = PeerInfo("p0", "127.0.0.1", 9000)
    membership = Membership(me, MembershipConfig(gossip_fanout=2), seed=1)

    for i in range(1, 5):
        membership.add_peer(PeerInfo(f"p{i}", "127.0.0.1", 9000+i), now=0)

    sent = []

    def send(peer, message):
        sent.append((peer.node_id, message.type))

    gossip = Gossip(membership, send)
    assert gossip.round() == 2
    assert len(sent) == 2
    assert all(kind == "MEMBERSHIP_GOSSIP" for _, kind in sent)


def test_gossip_merges_received_view():
    me = PeerInfo("p0", "127.0.0.1", 9000)
    membership = Membership(me)
    gossip = Gossip(membership, lambda *_: None)

    message = Message(
        type="MEMBERSHIP_GOSSIP",
        sender_id="p1",
        msg_id="m1",
        payload={
            "members": [
                PeerInfo("p2", "127.0.0.1", 9002).to_dict()
            ]
        },
    )

    assert gossip.handle(message) == 1
    assert "p2" in membership.peers
