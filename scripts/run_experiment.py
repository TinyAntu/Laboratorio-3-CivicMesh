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

import yaml

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
    pubsub_fanout_objective: int,
    pubsub_fanout_subjective: int,
    seed: int,
    communes: list[str],
    ttl_objective: int,
    ttl_subjective: int,
    priority_objective: int,
    priority_subjective: int,
) -> Path:
    run_dir = runs_dir / run_id
    metrics_dir = run_dir / "metrics"
    logs_dir = run_dir / "logs"
    
    run_dir.mkdir(parents=True, exist_ok=True)
    metrics_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    # Generar config.yaml específico de la corrida. Los publishers leen
    # este archivo, por lo que aquí deben quedar registrados los valores
    # efectivos de TTL y prioridad usados en el experimento.
    config_target = run_dir / "config.yaml"

    with open(config_source, "r", encoding="utf-8") as src:
        runtime_config = yaml.safe_load(src)

    runtime_config["seed"] = seed
    runtime_config["pubsub"]["objective"]["fanout"] = pubsub_fanout_objective
    runtime_config["pubsub"]["subjective"]["fanout"] = pubsub_fanout_subjective
    runtime_config["pubsub"]["objective"]["ttl"] = ttl_objective
    runtime_config["pubsub"]["subjective"]["ttl"] = ttl_subjective
    runtime_config["pubsub"]["objective"]["priority"] = priority_objective
    runtime_config["pubsub"]["subjective"]["priority"] = priority_subjective

    with open(config_target, "w", encoding="utf-8") as dst:
        yaml.safe_dump(
            runtime_config,
            dst,
            sort_keys=False,
            allow_unicode=True,
        )

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
    parser.add_argument("--control-timeout", type=float, default=0.75, help="Timeout del plano de control PING/Gossip")
    parser.add_argument("--listen-backlog", type=int, default=512, help="Backlog TCP de cada Peer")
    parser.add_argument(
        "--pubsub-fanout",
        type=int,
        default=None,
        help="Legacy: aplica el mismo fanout PubSub a ambos canales",
    )
    parser.add_argument(
        "--pubsub-fanout-objective",
        type=int,
        default=None,
        help="Fanout del canal objetivo; si se omite usa config/civicmesh.yaml",
    )
    parser.add_argument(
        "--pubsub-fanout-subjective",
        type=int,
        default=None,
        help="Fanout del canal subjetivo; si se omite usa config/civicmesh.yaml",
    )
    parser.add_argument(
        "--ttl-objective",
        type=int,
        default=None,
        help="TTL del canal objetivo; si se omite usa config/civicmesh.yaml",
    )
    parser.add_argument(
        "--ttl-subjective",
        type=int,
        default=None,
        help="TTL del canal subjetivo; si se omite usa config/civicmesh.yaml",
    )
    parser.add_argument(
        "--priority-objective",
        type=int,
        default=None,
        help="Prioridad del canal objetivo; si se omite usa config/civicmesh.yaml",
    )
    parser.add_argument(
        "--priority-subjective",
        type=int,
        default=None,
        help="Prioridad del canal subjetivo; si se omite usa config/civicmesh.yaml",
    )
    parser.add_argument(
        "--extra-subjective-publishers",
        type=int,
        default=1,
        help="Cantidad de fuentes subjetivas adicionales por comuna (sin duplicar canal objetivo)",
    )
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

    base_config = ConfigLoader.load(args.config)

    pubsub_fanout_objective = (
        args.pubsub_fanout_objective
        if args.pubsub_fanout_objective is not None
        else (
            args.pubsub_fanout
            if args.pubsub_fanout is not None
            else base_config.pubsub.objective.fanout
        )
    )
    pubsub_fanout_subjective = (
        args.pubsub_fanout_subjective
        if args.pubsub_fanout_subjective is not None
        else (
            args.pubsub_fanout
            if args.pubsub_fanout is not None
            else base_config.pubsub.subjective.fanout
        )
    )

    ttl_objective = (
        args.ttl_objective
        if args.ttl_objective is not None
        else base_config.pubsub.objective.ttl
    )
    ttl_subjective = (
        args.ttl_subjective
        if args.ttl_subjective is not None
        else base_config.pubsub.subjective.ttl
    )
    priority_objective = (
        args.priority_objective
        if args.priority_objective is not None
        else base_config.pubsub.objective.priority
    )
    priority_subjective = (
        args.priority_subjective
        if args.priority_subjective is not None
        else base_config.pubsub.subjective.priority
    )

    print("=" * 70)
    print(f"  CivicMesh - Ejecución de Experimento [{args.domain.upper()}]")
    print(f"  Run ID:      {run_id}")
    print(f"  Shared FS:   {runs_dir / run_id}")
    print(f"  Comunas:     {communes_list}")
    print(
        f"  Peers:       {args.num_peers} "
        f"(Gossip fanout={args.fanout}, "
        f"PubSub objective={pubsub_fanout_objective}, "
        f"subjective={pubsub_fanout_subjective})"
    )
    print(f"  Rumores:     {args.extra_subjective_publishers} fuente(s) subjetiva(s) extra por comuna")
    print(
        "  PubSub cfg:  "
        f"objective(fanout={pubsub_fanout_objective}, TTL={ttl_objective}, priority={priority_objective}) | "
        f"subjective(fanout={pubsub_fanout_subjective}, TTL={ttl_subjective}, priority={priority_subjective})"
    )
    print(f"  Duración:    {args.duration} s")
    if args.kill_peer:
        print(f"  Fallo Simul: Matar '{args.kill_peer}' en t={args.kill_time}s")
    print("=" * 70)

    run_dir = create_experiment_run(
        run_id=run_id,
        runs_dir=runs_dir,
        config_source=Path(args.config),
        fanout=args.fanout,
        pubsub_fanout_objective=pubsub_fanout_objective,
        pubsub_fanout_subjective=pubsub_fanout_subjective,
        seed=args.seed,
        communes=communes_list,
        ttl_objective=ttl_objective,
        ttl_subjective=ttl_subjective,
        priority_objective=priority_objective,
        priority_subjective=priority_subjective,
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
                "--control-timeout",
                str(args.control_timeout),
                "--listen-backlog",
                str(args.listen_backlog),
                "--pubsub-fanout-objective",
                str(pubsub_fanout_objective),
                "--pubsub-fanout-subjective",
                str(pubsub_fanout_subjective),
                "--ttl-objective",
                str(ttl_objective),
                "--ttl-subjective",
                str(ttl_subjective),
                "--priority-objective",
                str(priority_objective),
                "--priority-subjective",
                str(priority_subjective),
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
        # Cada comuna tiene un publisher principal (objective + subjective)
        # y N publishers adicionales que solo emiten subjective.
        pub_base_port = 9500
        subjective_base_port = 9600

        if args.extra_subjective_publishers < 0:
            raise SystemExit("--extra-subjective-publishers no puede ser negativo")

        for idx, comm in enumerate(communes_list):
            slug = comm.lower().replace(" ", "_")

            if args.domain == "crime":
                module = "domains.crime.publisher"
            else:
                module = "domains.air.publisher"

            # Publisher principal: publica ground truth + percepción.
            pub_id = f"pub_{slug}"
            pub_port = pub_base_port + idx
            pub_log = open(
                run_dir / "logs" / f"{pub_id}.log",
                "w",
                encoding="utf-8",
            )

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
                "--control-timeout",
                str(args.control_timeout),
                "--listen-backlog",
                str(args.listen_backlog),
                "--runs-dir",
                str(runs_dir),
                "--run-id",
                run_id,
                # Solo necesita escuchar rumores de su propia comuna.
                "--topics",
                comm,
            ]

            if args.domain == "air":
                pub_cmd.append("--loop")

            proc = subprocess.Popen(
                pub_cmd,
                stdout=pub_log,
                stderr=subprocess.STDOUT,
                env=env,
            )
            processes.append((pub_id, proc))
            print(
                f"  [+] Publisher principal '{comm}' ({pub_id}) "
                f"en puerto {pub_port} (PID: {proc.pid})"
            )

            # Fuentes subjetivas adicionales: calculan el mismo ground truth
            # localmente, pero no lo publican. Solo inyectan percepción/rumor.
            for extra_idx in range(args.extra_subjective_publishers):
                rumor_id = f"rumor_{slug}_{extra_idx + 1}"
                rumor_port = (
                    subjective_base_port
                    + idx * max(1, args.extra_subjective_publishers)
                    + extra_idx
                )
                rumor_log = open(
                    run_dir / "logs" / f"{rumor_id}.log",
                    "w",
                    encoding="utf-8",
                )

                rumor_cmd = [
                    sys.executable,
                    "-m",
                    module,
                    "--node-id",
                    rumor_id,
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(rumor_port),
                    "--commune",
                    comm,
                    "--config",
                    str(run_dir / "config.yaml"),
                    "--seeds-file",
                    str(hostfile_path),
                    "--fanout",
                    str(args.fanout),
                    "--control-timeout",
                    str(args.control_timeout),
                    "--listen-backlog",
                    str(args.listen_backlog),
                    "--runs-dir",
                    str(runs_dir),
                    "--run-id",
                    run_id,
                    "--topics",
                    comm,
                    "--subjective-only",
                ]

                if args.domain == "air":
                    rumor_cmd.append("--loop")

                rumor_proc = subprocess.Popen(
                    rumor_cmd,
                    stdout=rumor_log,
                    stderr=subprocess.STDOUT,
                    env=env,
                )
                processes.append((rumor_id, rumor_proc))
                print(
                    f"  [+] Fuente subjetiva '{comm}' ({rumor_id}) "
                    f"en puerto {rumor_port} (PID: {rumor_proc.pid})"
                )

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
