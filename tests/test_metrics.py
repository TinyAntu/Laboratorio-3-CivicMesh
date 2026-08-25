import json
import tempfile
from concurrent.futures import ThreadPoolExecutor
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


def test_loader_keeps_forward_drop_and_gossip_from_node_files():
    """Regresión: events.jsonl no debe ocultar eventos exclusivos por nodo."""
    with tempfile.TemporaryDirectory() as tmpdir:
        run_id = "node-events-run"
        collector = MetricsCollector(node_id="peer0", run_id=run_id, runs_dir=tmpdir)

        collector.record_forward(
            topic="Santiago",
            channel="objective",
            msg_id="msg-forward",
            targets_count=2,
            remaining_ttl=2,
            hop_count=1,
        )
        collector.record_drop(
            reason="duplicate",
            msg_id="msg-drop",
            topic="Santiago",
            channel="objective",
        )
        collector.record_gossip(
            active_peers=["peer1", "peer2"],
            suspect_peers=[],
            failed_peers=[],
            sent_count=2,
        )

        metrics_dir = Path(tmpdir) / run_id / "metrics"

        # Simula una corrida antigua donde events.jsonl existe pero contiene
        # solo un subconjunto de eventos. El loader debe preferir los archivos
        # por nodo para no perder forward/drop/gossip.
        (metrics_dir / "events.jsonl").write_text(
            json.dumps(
                {
                    "timestamp": 1.0,
                    "event": "publish",
                    "node_id": "legacy",
                    "topic": "Santiago",
                }
            )
            + "\n",
            encoding="utf-8",
        )

        records = load_metrics_from_run(Path(tmpdir) / run_id)
        event_types = {record["event"] for record in records}

        assert event_types == {"forward", "drop", "gossip"}


def test_events_jsonl_is_supported_as_legacy_fallback():
    with tempfile.TemporaryDirectory() as tmpdir:
        run_id = "legacy-run"
        metrics_dir = Path(tmpdir) / run_id / "metrics"
        metrics_dir.mkdir(parents=True)

        legacy_record = {
            "timestamp": 10.0,
            "event": "publish",
            "node_id": "peer-old",
            "topic": "Santiago",
        }
        (metrics_dir / "events.jsonl").write_text(
            json.dumps(legacy_record) + "\n",
            encoding="utf-8",
        )

        assert load_metrics_from_run(Path(tmpdir) / run_id) == [legacy_record]


def test_collectors_do_not_contend_on_shared_events_file():
    """Collectors distintos escriben archivos distintos, sin shared writer."""
    with tempfile.TemporaryDirectory() as tmpdir:
        run_id = "concurrent-run"
        peer_a = MetricsCollector(node_id="peer-a", run_id=run_id, runs_dir=tmpdir)
        peer_b = MetricsCollector(node_id="peer-b", run_id=run_id, runs_dir=tmpdir)

        def write_many(collector: MetricsCollector, prefix: str) -> None:
            for index in range(100):
                collector.record_drop(
                    reason="duplicate",
                    msg_id=f"{prefix}-{index}",
                    topic="Santiago",
                    channel="objective",
                )

        with ThreadPoolExecutor(max_workers=2) as executor:
            first = executor.submit(write_many, peer_a, "a")
            second = executor.submit(write_many, peer_b, "b")
            first.result()
            second.result()

        metrics_dir = Path(tmpdir) / run_id / "metrics"
        assert (metrics_dir / "peer-a.jsonl").exists()
        assert (metrics_dir / "peer-b.jsonl").exists()
        assert not (metrics_dir / "events.jsonl").exists()

        records = load_metrics_from_run(Path(tmpdir) / run_id)
        assert len(records) == 200
        assert all(record["event"] == "drop" for record in records)
