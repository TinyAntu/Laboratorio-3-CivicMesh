#!/usr/bin/env python3
"""CivicMesh - Generador y Ejecutor de Lotes de Experimentos Aleatorios."""

from __future__ import annotations
import argparse
from pathlib import Path
import random
import subprocess
import sys
import time

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

# Comunas disponibles con datos reales de calidad del aire y tasas base
AVAILABLE_COMMUNES = ["Santiago", "Las Condes", "Quilicura"]
ALL_DOMAINS = ["crime", "air"]


def run_random_batch(
    num_experiments: int = 3,
    min_duration: float = 8.0,
    max_duration: float = 15.0,
    min_peers: int = 3,
    max_peers: int = 6,
    runs_dir: str = "runs",
) -> None:
    print("=" * 75)
    print(f"  CivicMesh - Ejecución de Lote de {num_experiments} Experimentos Aleatorios")
    print("=" * 75)

    for i in range(1, num_experiments + 1):
        domain = random.choice(ALL_DOMAINS)
        duration = round(random.uniform(min_duration, max_duration), 1)
        peers = random.randint(min_peers, max_peers)
        fanout = random.randint(1, min(3, peers - 1))
        pubsub_fanout = random.randint(1, min(3, peers - 1))
        seed = random.randint(1, 9999)

        # Selección aleatoria de 2 o 3 comunas
        k_communes = random.randint(2, len(AVAILABLE_COMMUNES))
        selected_communes = random.sample(AVAILABLE_COMMUNES, k_communes)
        communes_str = ",".join(selected_communes)

        # 50% probabilidad de inyectar fallo de nodo
        simulate_failure = random.choice([True, False])
        kill_peer_arg = []
        if simulate_failure and peers > 2:
            kill_target = f"peer{random.randint(1, peers - 1)}"
            kill_time = round(duration * random.uniform(0.3, 0.6), 1)
            kill_peer_arg = ["--kill-peer", kill_target, "--kill-time", str(kill_time)]

        run_id = f"rnd-{domain}-p{peers}-f{fanout}-s{seed}-{int(time.time())}"

        print(f"\n[{i}/{num_experiments}] Lanzando experimento aleatorio:")
        print(f"  • Dominio:       {domain.upper()}")
        print(f"  • Run ID:        {run_id}")
        print(f"  • Nodos Peers:   {peers} (Gossip fanout={fanout}, PubSub fanout={pubsub_fanout})")
        print(f"  • Comunas:       {selected_communes}")
        print(f"  • Duración:      {duration}s | Semilla: {seed}")
        if kill_peer_arg:
            print(f"  • Fallo Inyectado: Matar {kill_target} en t={kill_time}s")
        else:
            print("  • Fallo Inyectado: Ninguno (Red Estable)")

        cmd = [
            sys.executable,
            "scripts/run_experiment.py",
            "--domain",
            domain,
            "--communes",
            communes_str,
            "--duration",
            str(duration),
            "--num-peers",
            str(peers),
            "--fanout",
            str(fanout),
            "--pubsub-fanout",
            str(pubsub_fanout),
            "--seed",
            str(seed),
            "--runs-dir",
            runs_dir,
            "--run-id",
            run_id,
        ] + kill_peer_arg

        # Ejecutar el experimento
        subprocess.run(cmd, check=True)
        time.sleep(1.0)

    print("\n" + "=" * 75)
    print(f"  Lote de {num_experiments} experimentos aleatorios completado con éxito.")
    print("  Todas las métricas fueron registradas en el Shared FS ('runs/').")
    print("  Puedes inspeccionar cada corrida en el dashboard: http://localhost:8501")
    print("=" * 75)


def main() -> int:
    parser = argparse.ArgumentParser(description="Generador de experimentos aleatorios para CivicMesh")
    parser.add_argument("--count", type=int, default=3, help="Cantidad de experimentos aleatorios a ejecutar")
    parser.add_argument("--min-duration", type=float, default=8.0, help="Duración mínima en segundos")
    parser.add_argument("--max-duration", type=float, default=15.0, help="Duración máxima en segundos")
    parser.add_argument("--min-peers", type=int, default=3, help="Cantidad mínima de peers")
    parser.add_argument("--max-peers", type=int, default=6, help="Cantidad máxima de peers")
    parser.add_argument("--runs-dir", default="runs", help="Directorio base de corridas")
    args = parser.parse_args()

    run_random_batch(
        num_experiments=args.count,
        min_duration=args.min_duration,
        max_duration=args.max_duration,
        min_peers=args.min_peers,
        max_peers=args.max_peers,
        runs_dir=args.runs_dir,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
