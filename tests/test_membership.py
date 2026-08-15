import time

from network.membership import Membership, MembershipConfig
from network.messages import PeerInfo


def peer(node_id, port):
    return PeerInfo(node_id=node_id, host="127.0.0.1", port=port)


def test_partial_view_and_fanout():
    me = peer("p0", 9000)
    membership = Membership(me, MembershipConfig(gossip_fanout=2), seed=10)

    for i in range(1, 6):
        membership.add_peer(peer(f"p{i}", 9000 + i), now=0)

    targets = membership.select_gossip_targets()
    assert len(targets) == 2
    assert all(p.node_id != "p0" for p in targets)


def test_merge_membership():
    membership = Membership(peer("p0", 9000))
    raw = [peer("p1", 9001).to_dict(), peer("p2", 9002).to_dict()]

    assert membership.merge(raw, now=1) == 2
    assert set(membership.peers) == {"p1", "p2"}
