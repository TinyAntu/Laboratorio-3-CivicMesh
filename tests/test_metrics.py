import json
import tempfile
from pathlib import Path
from network.metrics import MetricsCollector, load_metrics_from_run


def test_metrics_collector_records_and_loads():
    with tempfile.TemporaryDirectory() as tmpdir:
        run_id = "test-run-123"
        collector = MetricsCollector(node_id="peer0", run_id=run_id, runs_dir=tmpdir)

        collector.record_publish(
            topic="Santiago",
            channel="objective",
            value=10,
            msg_id="msg-1",
            timestamp=100.0,
        )

        collector.record_delivery(
            topic="Santiago",
            channel="objective",
            value=10,
            msg_id="msg-1",
            sender_id="peer1",
            hop_count=1,
            timestamp=100.1,
        )

        collector.record_step(
            domain="crime",
            commune="Santiago",
            step=0,
            objective_value=5.0,
            subjective_value=0.45,
            memory=1.2,
            gossip_value=0.3,
            timestamp=100.2,
        )

        run_path = Path(tmpdir) / run_id
        records = load_metrics_from_run(run_path)

        assert len(records) == 3
        assert records[0]["event"] == "publish"
        assert records[0]["topic"] == "Santiago"
        assert records[1]["event"] == "delivery"
        assert records[1]["hop_count"] == 1
        assert records[2]["event"] == "step"
        assert records[2]["objective_value"] == 5.0
        assert records[2]["gap"] == 0.45 - 5.0
