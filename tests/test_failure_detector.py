from network.failure_detector import FailureDetector


def test_peer_becomes_suspect_then_failed():
    fd = FailureDetector(timeout=5, suspect_timeout=5)
    fd.observe("p1", now=0)

    assert fd.check(now=4) == {}
    assert fd.status("p1") == "alive"

    assert fd.check(now=6) == {"p1": "suspect"}
    assert fd.status("p1") == "suspect"

    assert fd.check(now=11) == {"p1": "failed"}
    assert fd.status("p1") == "failed"


def test_observe_recovers_peer():
    fd = FailureDetector(timeout=5)
    fd.observe("p1", now=0)
    fd.check(now=6)

    fd.observe("p1", now=7)
    assert fd.status("p1") == "alive"
