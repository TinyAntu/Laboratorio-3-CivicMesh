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

def test_indirect_gossip_does_not_refresh_liveness_or_resurrect_peer():
    membership = Membership(
        peer("p0", 9000),
        MembershipConfig(
            failure_timeout=5,
            suspect_timeout=5,
        ),
        seed=42,
    )

    p1 = peer("p1", 9001)
    membership.add_peer(p1, now=0)

    # Otro peer sigue mencionando a p1 en su vista Gossip. Eso NO debe
    # equivaler a recibir un heartbeat directamente desde p1.
    stale_view = [peer("p1", 9001).to_dict()]
    membership.merge(stale_view, now=4)

    snapshot = membership.failure_detector.snapshot()
    assert snapshot["p1"]["last_seen"] == 0

    assert membership.run_failure_check(now=6) == {"p1": "suspect"}
    assert membership.peers["p1"].status == "suspect"

    # Más Gossip indirecto con la misma incarnation tampoco puede resucitarlo.
    membership.merge(stale_view, now=7)
    assert membership.peers["p1"].status == "suspect"
    assert membership.failure_detector.status("p1") == "suspect"

    assert membership.run_failure_check(now=11) == {"p1": "failed"}
    assert membership.peers["p1"].status == "failed"


def test_higher_incarnation_can_recover_peer_learned_by_gossip():
    membership = Membership(
        peer("p0", 9000),
        MembershipConfig(
            failure_timeout=5,
            suspect_timeout=5,
        ),
        seed=42,
    )

    membership.add_peer(peer("p1", 9001), now=0)
    membership.run_failure_check(now=6)
    assert membership.peers["p1"].status == "suspect"

    restarted = peer("p1", 9001)
    restarted.incarnation = 1

    assert membership.merge([restarted.to_dict()], now=7) == 1
    assert membership.peers["p1"].incarnation == 1
    assert membership.peers["p1"].status == "alive"
    assert membership.failure_detector.status("p1") == "alive"


def test_eviction_and_indirect_relearning_do_not_reset_failure_clock():
    membership = Membership(
        peer("p0", 9000),
        MembershipConfig(
            max_view_size=1,
            failure_timeout=5,
            suspect_timeout=5,
        ),
        seed=1,
    )

    membership.add_peer(peer("p1", 9001), now=0)

    # Forzamos la expulsión de p1 de la vista parcial agregando otro peer.
    membership.add_peer(peer("p2", 9002), now=1)
    assert "p1" not in membership.peers

    # p1 reaparece solo porque otro nodo lo menciona en Gossip. Debe conservar
    # el reloj original, no recibir last_seen=4.
    membership.merge([peer("p1", 9001).to_dict()], now=4)

    state = membership.failure_detector.get_state("p1")
    assert state is not None
    assert state.last_seen == 0

    assert membership.run_failure_check(now=6).get("p1") == "suspect"

    # Otra repetición indirecta tampoco lo revive.
    membership.merge([peer("p1", 9001).to_dict()], now=7)
    assert membership.failure_detector.status("p1") == "suspect"

    assert membership.run_failure_check(now=11).get("p1") == "failed"
