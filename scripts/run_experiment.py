#!/usr/bin/env python3
"""CivicMesh - Orquestador y Ejecutor de Experimentos Locales y Distribuidos."""

from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from typing import List

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from domains.config import ConfigLoader
from network.metrics import load_metrics_from_run


def create_experiment_run(
    run_id: str,
    runs_dir: Path,
    config_source: Path,
    fanout: int,
    pubsub_fanout: int,
    seed: int,
    communes: list[str],
) -> Path:
    run_dir = runs_dir / run_id
    metrics_dir = run_dir / "metrics"
    logs_dir = run_dir / "logs"
    
    run_dir.mkdir(parents=True, exist_ok=True)
    metrics_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    # Copiar o generar config.yaml en el run_dir
    config_target = run_dir / "config.yaml"
    with open(config_source, "r", encoding="utf-8") as src, open(config_target, "w", encoding="utf-8") as dst:
        dst.write(src.read())

    return run_dir


def main() -> int:
    parser = argparse.ArgumentParser(description="Ejecutor de experimentos CivicMesh")
    parser.add_argument(
        "--domain",
        choices=["crime", "air"],
        default="crime",
        help="Dominio de simulación (crime o air)",
    )
    parser.add_argument(
        "--communes",
        default="Santiago,Las Condes,Quilicura",
        help="Comunas separadas por coma",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=15.0,
        help="Duración del experimento en segundos",
    )
    parser.add_argument(
        "--step-interval",
        type=float,
        default=0.5,
        help="Intervalo de simulación entre pasos (s)",
    )
    parser.add_argument(
        "--num-peers",
        type=int,
        default=3,
        help="Cantidad de nodos Peer en la malla",
    )
    parser.add_argument("--fanout", type=int, default=2, help="Gossip fanout")
    parser.add_argument("--pubsub-fanout", type=int, default=2, help="PubSub fanout")
    parser.add_argument("--seed", type=int, default=42, help="Semilla RNG")
    parser.add_argument("--runs-dir", default="runs", help="Directorio base de corridas")
    parser.add_argument("--run-id", default=None, help="ID personalizado de corrida")
    parser.add_argument("--kill-peer", default=None, help="ID del peer a terminar para prueba de fallos")
    parser.add_argument("--kill-time", type=float, default=7.0, help="Segundo en el cual matar el peer")
    parser.add_argument("--config", default="config/civicmesh.yaml", help="Ruta de configuración")
    args = parser.parse_args()

    communes_list = [c.strip() for c in args.communes.split(",") if c.strip()]
    ts = int(time.time())
    run_id = args.run_id or f"local-{args.domain}-{ts}"
    runs_dir = Path(args.runs_dir)

    print("=" * 70)
    print(f"  CivicMesh - Ejecución de Experimento [{args.domain.upper()}]")
    print(f"  Run ID:      {run_id}")
    print(f"  Shared FS:   {runs_dir / run_id}")
    print(f"  Comunas:     {communes_list}")
    print(f"  Peers:       {args.num_peers} (Gossip fanout={args.fanout}, PubSub fanout={args.pubsub_fanout})")
    print(f"  Duración:    {args.duration} s")
    if args.kill_peer:
        print(f"  Fallo Simul: Matar '{args.kill_peer}' en t={args.kill_time}s")
    print("=" * 70)

    run_dir = create_experiment_run(
        run_id=run_id,
        runs_dir=runs_dir,
        config_source=Path(args.config),
        fanout=args.fanout,
        pubsub_fanout=args.pubsub_fanout,
        seed=args.seed,
        communes=communes_list,
    )

    # 1. Generar hostfile.txt
    hostfile_path = run_dir / "hostfile.txt"
    base_port = 9000
    peer_entries = []
    with open(hostfile_path, "w", encoding="utf-8") as hf:
        for i in range(args.num_peers):
            node_id = f"peer{i}"
            port = base_port + i
            # Asignar comunas
            c_str = ",".join(communes_list)
            entry = f"{node_id} 127.0.0.1 {port} {c_str}\n"
            hf.write(entry)
            peer_entries.append((node_id, port))

    processes: list[tuple[str, subprocess.Popen]] = []
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT_DIR)

    try:
        # 2. Iniciar Peers
        for node_id, port in peer_entries:
            log_file = open(run_dir / "logs" / f"{node_id}.log", "w", encoding="utf-8")
            cmd = [
                sys.executable,
                "-m",
                "network.peer",
                "--node-id",
                node_id,
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
                "--seeds-file",
                str(hostfile_path),
                "--fanout",
                str(args.fanout),
                "--pubsub-fanout",
                str(args.pubsub_fanout),
                "--seed",
                str(args.seed + int(node_id.replace("peer", ""))),
                "--topics",
                ",".join(communes_list),
                "--runs-dir",
                str(runs_dir),
                "--run-id",
                run_id,
            ]
            proc = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT, env=env)
            processes.append((node_id, proc))
            print(f"  [+] Iniciado {node_id} en 127.0.0.1:{port} (PID: {proc.pid})")

        time.sleep(1.0)  # Esperar que los sockets de los peers estén listos

        # 3. Iniciar Publicadores por Comuna
        pub_base_port = 9500
        for idx, comm in enumerate(communes_list):
            pub_id = f"pub_{comm.lower().replace(' ', '_')}"
            pub_port = pub_base_port + idx
            pub_log = open(run_dir / "logs" / f"{pub_id}.log", "w", encoding="utf-8")

            if args.domain == "crime":
                module = "domains.crime.publisher"
            else:
                module = "domains.air.publisher"

            pub_cmd = [
                sys.executable,
                "-m",
                module,
                "--node-id",
                pub_id,
                "--host",
                "127.0.0.1",
                "--port",
                str(pub_port),
                "--commune",
                comm,
                "--config",
                str(run_dir / "config.yaml"),
                "--seeds-file",
                str(hostfile_path),
                "--fanout",
                str(args.fanout),
                "--pubsub-fanout",
                str(args.pubsub_fanout),
                "--runs-dir",
                str(runs_dir),
                "--run-id",
                run_id,
                "--topics",
                ",".join(communes_list),
            ]
            if args.domain == "air":
                pub_cmd.append("--loop")

            proc = subprocess.Popen(pub_cmd, stdout=pub_log, stderr=subprocess.STDOUT, env=env)
            processes.append((pub_id, proc))
            print(f"  [+] Iniciado Publicador para '{comm}' ({pub_id}) en puerto {pub_port} (PID: {proc.pid})")

        print("\n  [Simulación en Curso...]")
        start_time = time.time()
        killed = False

        while True:
            elapsed = time.time() - start_time
            if elapsed >= args.duration:
                break

            if args.kill_peer and not killed and elapsed >= args.kill_time:
                for name, p in processes:
                    if name == args.kill_peer:
                        print(f"\n  [!] [EXPERIMENTO FALLO] Terminando nodo {name} (PID {p.pid}) en t={elapsed:.2f}s...")
                        p.terminate()
                        p.wait()
                        killed = True
                        break

            time.sleep(0.5)

        print(f"\n  [Simulación completada tras {args.duration} s]")

    finally:
        print("  [Cerrando procesos...]")
        for name, p in processes:
            if p.poll() is None:
                p.terminate()
                try:
                    p.wait(timeout=2.0)
                except subprocess.TimeoutExpired:
                    p.kill()

    # 4. Resumen de Métricas Generadas
    metrics = load_metrics_from_run(run_dir)
    print("\n" + "=" * 70)
    print(f"  RESUMEN DE MÉTRICAS - RUN: {run_id}")
    print("=" * 70)
    print(f"  Total de registros de métricas generados: {len(metrics)}")
    
    events_count: dict[str, int] = {}
    for r in metrics:
        evt = r.get("event", "unknown")
        events_count[evt] = events_count.get(evt, 0) + 1
        
    for evt, count in sorted(events_count.items()):
        print(f"  - Eventos '{evt}': {count}")

    print(f"\n  Para visualizar en el Dashboard:")
    print(f"  streamlit run frontend/app.py -- --runs-dir {runs_dir} --run-id {run_id}")
    print("=" * 70)

    return 0


if __name__ == "__main__":
    sys.exit(main())
