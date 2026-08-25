from network.messages import PeerInfo
from network.peer import Peer


def test_liveness_probe_refreshes_active_peer(monkeypatch, tmp_path):
    node = Peer(
        node_id="p0",
        host="127.0.0.1",
        port=9900,
        failure_timeout=5,
        suspect_timeout=5,
        runs_dir=tmp_path,
        run_id="probe-ok",
        seed=1,
    )
    node.membership.add_peer(
        PeerInfo("p1", "127.0.0.1", 9901),
        now=0,
    )

    # Simular una conexión TCP exitosa sin abrir sockets reales. _send_peer
    # debe convertir ese éxito en evidencia directa mediante mark_seen().
    monkeypatch.setattr("network.peer.send_json", lambda *_args, **_kwargs: None)

    assert node._probe_membership_liveness() == 1
    state = node.membership.failure_detector.get_state("p1")
    assert state is not None
    assert state.last_seen > 0
    assert state.status == "alive"


def test_liveness_probe_does_not_refresh_unreachable_peer(monkeypatch, tmp_path):
    node = Peer(
        node_id="p0",
        host="127.0.0.1",
        port=9910,
        failure_timeout=5,
        suspect_timeout=5,
        runs_dir=tmp_path,
        run_id="probe-fail",
        seed=1,
    )
    node.membership.add_peer(
        PeerInfo("p1", "127.0.0.1", 9911),
        now=0,
    )

    def fail_send(*_args, **_kwargs):
        raise OSError("unreachable")

    monkeypatch.setattr("network.peer.send_json", fail_send)

    assert node._probe_membership_liveness() == 0
    state = node.membership.failure_detector.get_state("p1")
    assert state is not None
    assert state.last_seen == 0

    assert node.membership.run_failure_check(now=6) == {"p1": "suspect"}
    assert node.membership.run_failure_check(now=11) == {"p1": "failed"}


def test_liveness_probe_parallelizes_slow_unreachable_peers(monkeypatch, tmp_path):
    """Una vista cargada no debe bloquear la ronda por N * timeout."""
    import time

    node = Peer(
        node_id="p0",
        host="127.0.0.1",
        port=9920,
        max_view_size=8,
        failure_timeout=5,
        suspect_timeout=5,
        control_timeout=0.20,
        runs_dir=tmp_path,
        run_id="probe-parallel",
        seed=1,
    )

    for index in range(8):
        node.membership.add_peer(
            PeerInfo(f"p{index + 1}", "127.0.0.1", 9930 + index),
            now=0,
        )

    def slow_failure(*_args, **_kwargs):
        time.sleep(0.20)
        raise OSError("slow unreachable peer")

    monkeypatch.setattr("network.peer.send_json", slow_failure)

    started = time.monotonic()
    assert node._probe_membership_liveness() == 0
    elapsed = time.monotonic() - started

    # Secuencialmente serían ~1.6 s. En paralelo la ronda debe quedar cerca
    # de un único timeout, con margen amplio para CI cargado.
    assert elapsed < 0.70
    node.stop()


def test_gossip_control_plane_uses_short_timeout(monkeypatch, tmp_path):
    node = Peer(
        node_id="p0",
        host="127.0.0.1",
        port=9940,
        fanout=1,
        control_timeout=0.35,
        runs_dir=tmp_path,
        run_id="gossip-control-timeout",
        seed=1,
    )
    node.membership.add_peer(PeerInfo("p1", "127.0.0.1", 9941))

    observed_timeouts = []

    def capture_timeout(_host, _port, _message, timeout=2.0):
        observed_timeouts.append(timeout)

    monkeypatch.setattr("network.peer.send_json", capture_timeout)

    assert node.gossip.round() == 1
    assert observed_timeouts == [0.35]
    node.stop()
