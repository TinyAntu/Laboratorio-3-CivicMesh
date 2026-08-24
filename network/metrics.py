from __future__ import annotations

import json
import os
from pathlib import Path
import threading
import time
from typing import Any


class MetricsCollector:
    """Recolector y exportador de métricas en formato JSONL para CivicMesh."""

    def __init__(
        self,
        node_id: str,
        run_id: str | None = None,
        runs_dir: str | Path | None = None,
    ) -> None:
        self.node_id = node_id
        
        # Determinar directorio base según $CIVICMESH_RUNS o valor por defecto
        base_dir = runs_dir or os.getenv("CIVICMESH_RUNS", "runs")
        self.runs_dir = Path(base_dir)
        
        if run_id is None:
            self.run_id = os.getenv("SLURM_JOB_ID", f"local-{int(time.time())}")
        else:
            self.run_id = run_id
            
        self.metrics_dir = self.runs_dir / self.run_id / "metrics"
        self.metrics_dir.mkdir(parents=True, exist_ok=True)
        
        self.node_file = self.metrics_dir / f"{self.node_id}.jsonl"
        self.summary_file = self.metrics_dir / "events.jsonl"
        self._lock = threading.Lock()
        
        # Contadores en memoria
        self.stats = {
            "published": 0,
            "delivered": 0,
            "forwarded": 0,
            "dropped_duplicate": 0,
            "dropped_ttl": 0,
            "dropped_other": 0,
            "hops_total": 0,
            "hops_count": 0,
        }

    def _write_record(self, record: dict[str, Any], also_summary: bool = True) -> None:
        line = json.dumps(record, separators=(",", ":")) + "\n"
        with self._lock:
            with open(self.node_file, "a", encoding="utf-8") as fh:
                fh.write(line)
            if also_summary:
                try:
                    with open(self.summary_file, "a", encoding="utf-8") as fh:
                        fh.write(line)
                except OSError:
                    pass

    def record_publish(
        self,
        topic: str,
        channel: str,
        value: Any,
        msg_id: str,
        timestamp: float | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        now = time.time() if timestamp is None else timestamp
        with self._lock:
            self.stats["published"] += 1

        record = {
            "timestamp": now,
            "event": "publish",
            "node_id": self.node_id,
            "topic": topic,
            "channel": channel,
            "value": value,
            "msg_id": msg_id,
            "metadata": metadata or {},
        }
        self._write_record(record)

    def record_delivery(
        self,
        topic: str,
        channel: str,
        value: Any,
        msg_id: str,
        sender_id: str,
        hop_count: int,
        source_id: str | None = None,
        timestamp: float | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        now = time.time() if timestamp is None else timestamp
        with self._lock:
            self.stats["delivered"] += 1
            self.stats["hops_total"] += hop_count
            self.stats["hops_count"] += 1

        record = {
            "timestamp": now,
            "event": "delivery",
            "node_id": self.node_id,
            "topic": topic,
            "channel": channel,
            "value": value,
            "msg_id": msg_id,
            "sender_id": sender_id,
            "source_id": source_id,
            "hop_count": hop_count,
            "metadata": metadata or {},
        }
        self._write_record(record)

    def record_forward(
        self,
        topic: str,
        channel: str,
        msg_id: str,
        targets_count: int,
        remaining_ttl: int,
        hop_count: int,
    ) -> None:
        with self._lock:
            self.stats["forwarded"] += targets_count

        record = {
            "timestamp": time.time(),
            "event": "forward",
            "node_id": self.node_id,
            "topic": topic,
            "channel": channel,
            "msg_id": msg_id,
            "targets_count": targets_count,
            "remaining_ttl": remaining_ttl,
            "hop_count": hop_count,
        }
        self._write_record(record, also_summary=False)

    def record_drop(self, reason: str, msg_id: str, topic: str = "", channel: str = "") -> None:
        with self._lock:
            if reason == "duplicate":
                self.stats["dropped_duplicate"] += 1
            elif reason == "ttl_expired":
                self.stats["dropped_ttl"] += 1
            else:
                self.stats["dropped_other"] += 1

        record = {
            "timestamp": time.time(),
            "event": "drop",
            "node_id": self.node_id,
            "reason": reason,
            "msg_id": msg_id,
            "topic": topic,
            "channel": channel,
        }
        self._write_record(record, also_summary=False)

    def record_gossip(
        self,
        active_peers: list[str],
        suspect_peers: list[str],
        failed_peers: list[str],
        sent_count: int,
    ) -> None:
        record = {
            "timestamp": time.time(),
            "event": "gossip",
            "node_id": self.node_id,
            "active_count": len(active_peers),
            "suspect_count": len(suspect_peers),
            "failed_count": len(failed_peers),
            "sent_count": sent_count,
            "active_peers": active_peers,
            "failed_peers": failed_peers,
        }
        self._write_record(record, also_summary=False)

    def record_step(
        self,
        domain: str,
        commune: str,
        step: int,
        objective_value: float,
        subjective_value: float,
        memory: float,
        gossip_value: float,
        timestamp: float | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        now = time.time() if timestamp is None else timestamp
        record = {
            "timestamp": now,
            "event": "step",
            "node_id": self.node_id,
            "domain": domain,
            "commune": commune,
            "step": step,
            "objective_value": objective_value,
            "subjective_value": subjective_value,
            "gap": subjective_value - objective_value,
            "memory": memory,
            "gossip_value": gossip_value,
            "metadata": metadata or {},
        }
        self._write_record(record, also_summary=True)


def load_metrics_from_run(run_dir: str | Path) -> list[dict[str, Any]]:
    """Carga todos los registros JSONL de una corrida específica."""
    metrics_path = Path(run_dir) / "metrics"
    if not metrics_path.exists():
        metrics_path = Path(run_dir)

    records: list[dict[str, Any]] = []
    events_file = metrics_path / "events.jsonl"

    if events_file.exists():
        with open(events_file, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
        return sorted(records, key=lambda r: float(r.get("timestamp", 0)))

    # Fallback: leer todos los .jsonl del directorio
    for f in metrics_path.glob("*.jsonl"):
        with open(f, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass

    return sorted(records, key=lambda r: float(r.get("timestamp", 0)))
