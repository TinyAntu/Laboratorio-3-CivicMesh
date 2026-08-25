from domains.config import ConfigLoader
from network.peer import Peer


def test_pubsub_values_are_loaded_from_yaml():
    config = ConfigLoader.load("config/civicmesh.yaml")

    assert config.pubsub.objective.fanout == 3
    assert config.pubsub.objective.ttl == 3
    assert config.pubsub.objective.priority == 80
    assert config.pubsub.subjective.fanout == 2
    assert config.pubsub.subjective.ttl == 5
    assert config.pubsub.subjective.priority == 50


def test_peer_receives_external_pubsub_config(tmp_path):
    peer = Peer(
        node_id="p-config",
        host="127.0.0.1",
        port=0,
        pubsub_fanout_objective=4,
        pubsub_fanout_subjective=2,
        ttl_objective=7,
        ttl_subjective=9,
        priority_objective=90,
        priority_subjective=40,
        seed=42,
        runs_dir=str(tmp_path),
        run_id="test-config",
    )

    assert peer.pubsub.config.fanout_objective == 4
    assert peer.pubsub.config.fanout_subjective == 2
    assert peer.pubsub.config.default_ttl_objective == 7
    assert peer.pubsub.config.default_ttl_subjective == 9
    assert peer.pubsub.config.default_priority_objective == 90
    assert peer.pubsub.config.default_priority_subjective == 40
