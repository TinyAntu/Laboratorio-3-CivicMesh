#!/usr/bin/env python3
"""CivicMesh - Verificador y Generador de Ejemplos Numericos Paso a Paso (Seccion 4.3)."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from domains.config import ConfigLoader
from domains.crime.generator import CrimeGenerator
from domains.crime.perception import CrimePerceptionModel
from domains.air.perception import AirPerceptionModel
from domains.air.replay import AirQualityDataset, AirQualityReplay


def run_domain_a_trace(
    config_path: str,
    commune: str = "Santiago",
    steps: int = 2,
    mock_gossip: list[float] | None = None,) -> str:
    cfg = ConfigLoader.load(config_path)
    generator = CrimeGenerator(cfg.crime.rates, cfg.simulation.delta_t, cfg.seed)
    model = CrimePerceptionModel(cfg.crime_perception, cfg.seed)

    if mock_gossip is None:
        mock_gossip = [0.0, 0.35, 0.40, 0.45]

    lines = []
    lines.append("=" * 80)
    lines.append(f"DOMINIO A: DELITOS (Comuna: {commune}, Seed: {cfg.seed}, delta_t: {cfg.simulation.delta_t}s)")
    lines.append(f"Parametros: alpha={cfg.crime_perception.alpha}, beta0={cfg.crime_perception.beta0}, "
                 f"beta1={cfg.crime_perception.beta1}, beta2={cfg.crime_perception.beta2}, "
                 f"sigma_eps={cfg.crime_perception.sigma_epsilon}")
    lines.append("=" * 80)

    for t in range(steps):
        logical_time = t * cfg.simulation.delta_t
        events = generator.generate(commune, logical_time)
        total_crimes = generator.total(events)
        gossip_val = mock_gossip[t] if t < len(mock_gossip) else 0.0

        prev_memory = model.memory(commune)
        new_memory = (
            cfg.crime_perception.alpha * prev_memory
            + (1.0 - cfg.crime_perception.alpha) * float(total_crimes)
        )
        
        perception = model.update(commune, total_crimes, gossip_val)

        events_str = ", ".join(f"{e.crime_type}={e.count}" for e in events)
        lines.append(f"\n--- PASO t = {t} (Timestamp logico = {logical_time:.1f}s) ---")
        lines.append(f"1. Muestreo Poisson: [{events_str}] => R_{commune}({t}) = {total_crimes} delitos")
        lines.append(f"2. Memoria EMA:")
        lines.append(f"   M({t}) = {cfg.crime_perception.alpha:.2f} * {prev_memory:.4f} + (1 - {cfg.crime_perception.alpha:.2f}) * {total_crimes}")
        lines.append(f"          = {new_memory:.4f}")
        lines.append(f"3. Rumor Gossip recibido: P_gossip({t}) = {gossip_val:.4f}")
        lines.append(f"4. Percepcion Subjetiva (Logistica):")
        lines.append(f"   Z({t}) = beta0 + beta1 * M({t}) + beta2 * P_gossip({t}) + eps")
        lines.append(f"          = {cfg.crime_perception.beta0:.2f} + {cfg.crime_perception.beta1:.2f} * {new_memory:.4f} + {cfg.crime_perception.beta2:.2f} * {gossip_val:.4f} + eps")
        lines.append(f"   P_{commune}({t}) = sigma(Z({t})) = {perception:.4f} (Sensacion de Inseguridad: {perception * 100:.2f}%)")

    return "\n".join(lines)


def run_domain_b_trace(
    config_path: str,
    commune: str = "Quilicura",
    steps: int = 2,
    mock_gossip: list[float] | None = None,) -> str:
    cfg = ConfigLoader.load(config_path)
    csv_path = cfg.air.datasets.get(commune)
    if not csv_path or not Path(csv_path).exists():
        raise FileNotFoundError(f"Dataset de calidad de aire no encontrado para {commune}: {csv_path}")

    dataset = AirQualityDataset.from_csv(csv_path)
    replay = AirQualityReplay(dataset)
    model = AirPerceptionModel(cfg.air_perception, cfg.seed)

    if mock_gossip is None:
        mock_gossip = [0.0, 45.0, 50.0, 55.0]

    lines = []
    lines.append("\n" + "=" * 80)
    lines.append(f"DOMINIO B: CALIDAD DEL AIRE (Comuna: {commune}, Dataset: {csv_path}, Seed: {cfg.seed})")
    lines.append(f"Parametros: alpha={cfg.air_perception.alpha}, gamma={cfg.air_perception.gamma}, "
                 f"delta={cfg.air_perception.delta}, sigma_eps={cfg.air_perception.sigma_epsilon}, "
                 f"clip=[{cfg.air_perception.clip_min}, {cfg.air_perception.clip_max}]")
    lines.append("=" * 80)

    for t in range(steps):
        sample = replay.next_sample()
        obj_val = sample.pm2_5 if cfg.air.pollutant == "pm2_5" else sample.pm10
        gossip_val = mock_gossip[t] if t < len(mock_gossip) else 0.0

        prev_memory = model.memory(commune)
        stimulus = max(obj_val, prev_memory)
        new_memory = (
            cfg.air_perception.alpha * prev_memory
            + (1.0 - cfg.air_perception.alpha) * stimulus
        )

        perception = model.update(commune, obj_val, gossip_val)

        lines.append(f"\n--- PASO t = {t} (Hora de muestra: {sample.time}) ---")
        lines.append(f"1. Medicion Real Replay: v_{commune}({t}) = {obj_val:.2f} ug/m3 (PM2.5)")
        lines.append(f"2. Estimulo de Pico:")
        lines.append(f"   u({t}) = max(v({t}), M({t-1})) = max({obj_val:.2f}, {prev_memory:.4f}) = {stimulus:.4f} ug/m3")
        lines.append(f"3. Memoria EMA:")
        lines.append(f"   M({t}) = {cfg.air_perception.alpha:.2f} * {prev_memory:.4f} + (1 - {cfg.air_perception.alpha:.2f}) * {stimulus:.4f}")
        lines.append(f"          = {new_memory:.4f} ug/m3")
        lines.append(f"4. Rumor Gossip recibido: P_gossip({t}) = {gossip_val:.2f} ug/m3")
        lines.append(f"5. Percepcion Calculada con Sesgo y Saturacion:")
        lines.append(f"   P({t}) = v({t}) + gamma * (M({t}) - v({t})) + delta * P_gossip({t}) + eps")
        lines.append(f"          = {obj_val:.2f} + {cfg.air_perception.gamma:.2f} * ({new_memory:.4f} - {obj_val:.2f}) + {cfg.air_perception.delta:.2f} * {gossip_val:.2f} + eps")
        lines.append(f"   P_{commune}({t}) = {perception:.4f} ug/m3 (clip aplicado si excede [0, 500])")


    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Verificador y trazador numerico de pasos para CivicMesh")
    parser.add_argument("--config", default="config/civicmesh.yaml", help="Ruta al archivo civicmesh.yaml")
    parser.add_argument("--steps", type=int, default=2, help="Cantidad de pasos a simular")
    parser.add_argument("--commune-crime", default="Santiago", help="Comuna para Dominio A")
    parser.add_argument("--commune-air", default="Quilicura", help="Comuna para Dominio B")
    parser.add_argument("--output-md", default=None, help="Ruta opcional para guardar el reporte en Markdown")
    args = parser.parse_args()

    trace_a = run_domain_a_trace(args.config, args.commune_crime, args.steps)
    trace_b = run_domain_b_trace(args.config, args.commune_air, args.steps)

    full_output = f"{trace_a}\n\n{trace_b}\n"
    print(full_output)

    if args.output_md:
        md_path = Path(args.output_md)
        md_path.parent.mkdir(parents=True, exist_ok=True)
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(f"# Evidencia Numerica Paso a Paso - CivicMesh (Seccion 4.3)\n\n```text\n{full_output}```\n")
        print(f"[+] Archivo markdown guardado en: {md_path.resolve()}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
