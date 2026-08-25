from network.membership import Membership, MembershipConfig
from network.messages import PeerInfo


def peer(node_id, port):
    return PeerInfo(
        node_id=node_id,
        host="127.0.0.1",
        port=port,
    )


def test_partial_view_is_bounded_and_fanout_is_respected():
    me = peer("p0", 9000)

    membership = Membership(
        me,
        MembershipConfig(
            gossip_fanout=2,
            max_view_size=3,
        ),
        seed=10,
    )

    # Intentamos aprender cinco peers,
    # pero la vista solo admite tres.
    for i in range(1, 6):
        membership.add_peer(
            peer(
                f"p{i}",
                9000 + i,
            ),
            now=0,
        )

    assert len(membership.peers) == 3

    targets = membership.select_gossip_targets()

    assert len(targets) == 2

    assert all(
        p.node_id != "p0"
        for p in targets
    )


def test_merge_never_exceeds_partial_view_limit():
    membership = Membership(
        peer("p0", 9000),
        MembershipConfig(
            max_view_size=2,
        ),
        seed=42,
    )

    remote = [
        peer("p1", 9001).to_dict(),
        peer("p2", 9002).to_dict(),
        peer("p3", 9003).to_dict(),
        peer("p4", 9004).to_dict(),
    ]

    membership.merge(
        remote,
        now=1,
    )

    assert len(membership.peers) == 2


def test_known_peer_can_be_updated_without_growing_view():
    membership = Membership(
        peer("p0", 9000),
        MembershipConfig(
            max_view_size=2,
        ),
        seed=42,
    )

    membership.add_peer(
        peer("p1", 9001),
        now=1,
    )

    membership.add_peer(
        peer("p2", 9002),
        now=1,
    )

    updated = peer("p1", 9001)
    updated.incarnation = 1

    membership.add_peer(
        updated,
        now=2,
    )

    assert len(membership.peers) == 2
    assert "p1" in membership.peers
    assert (
        membership.peers["p1"].incarnation
        == 1
    )


def test_merge_membership():
    membership = Membership(
        peer("p0", 9000)
    )

    raw = [
        peer("p1", 9001).to_dict(),
        peer("p2", 9002).to_dict(),
    ]

    assert membership.merge(
        raw,
        now=1,
    ) == 2

    assert set(
        membership.peers
    ) == {
        "p1",
        "p2",
    }