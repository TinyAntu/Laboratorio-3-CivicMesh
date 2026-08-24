#!/usr/bin/env python3
"""CivicMesh - Generador de Gráficos y Tablas para el Informe Final."""

from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import sys
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from network.metrics import load_metrics_from_run


def generate_summary_tables(output_dir: Path) -> None:
    """Genera la tabla de parámetros y ejemplos numéricos requerida por la Sección 4.3."""
    table_content = r"""# Resumen Cuantitativo y Modelos de Generación - CivicMesh

## 1. Tabla de Parámetros de Dominio y Modelos Subjetivos

| Parámetro | Dominio A (Delitos) | Dominio B (Calidad del Aire) | Descripción / Rol Matemático |
| :--- | :---: | :---: | :--- |
| **Naturaleza del Dato** | Eventos Discretos ($X_{c,k} \sim \\text{Poisson}$) | Serie Temporal Continua ($\mu\\text{g/m}^3$) | Fuente del canal objetivo ($G_c(t)$) |
| **Fuente Objetivo** | Tasas $\\lambda_{c,k}$ simuladas en YAML | CSV Replay Open-Meteo / SINCA | Ground truth reproducible |
| $\\alpha$ (Factor EMA) | $0,80$ | $0,85$ | Inercia de memoria ($M_c$). Valores altos = olvido lento. |
| $\\beta_0$ | $-1,0$ | N/A | Sesgo base en función logística del delito. |
| $\\beta_1$ | $0,40$ | N/A | Ponderación de la memoria local de delitos. |
| $\\beta_2$ / $\\delta$ | $0,80$ ($\beta_2$) | $0,30$ ($\delta$) | Coeficiente de amplificación por rumor Gossip ($\hat{P}^{\\text{gossip}}$). |
| $\\gamma$ | N/A | $0,60$ | Sesgo de arrastre por retención de picos ($M_c - v_c$). |
| $\\sigma_\\varepsilon$ | $0,10$ | $2,0$ | Desviación estándar del ruido estocástico $\\varepsilon_c \sim \\mathcal{N}(0, \\sigma_\\varepsilon^2)$. |
| **Saturación / Rango** | $\\sigma(Z_c) \\in [0, 1]$ | $\\text{clip}([0, 500])$ | Rango físico válido del canal subjetivo. |

---

## 2. Ejemplo Numérico Paso a Paso (Paso $t$)

### Dominio A (Delitos - Inseguridad)
* **Entradas**: $R_c(t) = 3$ delitos, $M_c(t-1) = 1,5$, $\hat{P}^{\\text{gossip}}_c(t) = 0,60$, $\\varepsilon_c(t) = 0,02$.
1. **Actualización Memoria EMA**:
   $$M_c(t) = 0,80 \\cdot 1,5 + (1 - 0,80) \\cdot 3 = 1,20 + 0,60 = 1,80$$
2. **Combinación Lineal**:
   $$Z_c(t) = -1,0 + 0,40(1,80) + 0,80(0,60) + 0,02 = -1,0 + 0,72 + 0,48 + 0,02 = 0,22$$
3. **Índice de Percepción**:
   $$P_c(t) = \\sigma(0,22) = \\frac{1}{1 + e^{-0,22}} \\approx 0,5548 \\quad (55,48\\% \\text{ sensación de inseguridad})$$

### Dominio B (Calidad del Aire - PM2.5)
* **Entradas**: $v_c(t) = 25,0\,\\mu\\text{g/m}^3$, $M_c(t-1) = 45,0\,\\mu\\text{g/m}^3$, $\hat{P}^{\\text{gossip}}_c(t) = 30,0\,\\mu\\text{g/m}^3$, $\\varepsilon_c(t) = 0,5$.
1. **Estímulo con Memoria de Pico**:
   $$u_c(t) = \\max(25,0, 45,0) = 45,0$$
2. **Actualización Memoria EMA**:
   $$M_c(t) = 0,85 \\cdot 45,0 + (1 - 0,85) \\cdot 45,0 = 45,0$$
3. **Percepción Calculada**:
   $$P_c(t) = 25,0 + 0,60(45,0 - 25,0) + 0,30(30,0) + 0,5 = 25,0 + 12,0 + 9,0 + 0,5 = 46,5\,\\mu\\text{g/m}^3$$
4. **Saturación**: $\\text{clip}(46,5, [0, 500]) = 46,5\,\\mu\\text{g/m}^3$ (la percepción casi duplica la medición real debido al pico retenido y rumores).
"""
    with open(output_dir / "parameters_and_examples.md", "w", encoding="utf-8") as fh:
        fh.write(table_content)
    print(f"  [+] Generado reporte Markdown: {output_dir / 'parameters_and_examples.md'}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Generador de gráficos del informe CivicMesh")
    parser.add_argument("--runs-dir", default="runs", help="Directorio base de corridas")
    parser.add_argument("--output-dir", default="report_assets", help="Directorio destino para gráficos")
    args = parser.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("  CivicMesh - Generación de Activos para el Informe Final")
    print(f"  Directorio de Salida: {out_dir.resolve()}")
    print("=" * 70)

    generate_summary_tables(out_dir)

    # Intentar generar gráficos si matplotlib está disponible
    try:
        import matplotlib.pyplot as plt
        
        runs_path = Path(args.runs_dir)
        runs = [d for d in runs_path.iterdir() if d.is_dir() and (d / "metrics").exists()]
        
        if not runs:
            print(f"  [!] No se encontraron corridas en {runs_path} para graficar.")
            return 0

        for run in runs:
            records = load_metrics_from_run(run)
            step_events = [r for r in records if r.get("event") == "step"]
            if not step_events:
                continue

            communes = sorted(list(set(r["commune"] for r in step_events)))
            domain = step_events[0].get("domain", "unknown")

            fig, ax = plt.subplots(figsize=(10, 5), dpi=150)
            for comm in communes:
                c_steps = [r for r in step_events if r["commune"] == comm]
                x = [r["step"] for r in c_steps]
                y_obj = [r["objective_value"] for r in c_steps]
                y_sub = [r["subjective_value"] for r in c_steps]

                ax.plot(x, y_obj, "--", label=f"{comm} (Objetivo/Real)")
                ax.plot(x, y_sub, "-", label=f"{comm} (Percepción/Subjetivo)")

            ax.set_title(f"CivicMesh [{domain.upper()}]: Realidad vs Percepción ({run.name})")
            ax.set_xlabel("Paso de Simulación (t)")
            ax.set_ylabel("Valor / Índice")
            ax.grid(True, linestyle=":", alpha=0.6)
            ax.legend(bbox_to_anchor=(1.05, 1), loc="upper left")
            plt.tight_layout()

            chart_path = out_dir / f"chart_{domain}_{run.name}.png"
            fig.savefig(chart_path)
            plt.close(fig)
            print(f"  [+] Generado gráfico PNG: {chart_path}")

    except ImportError:
        print("  [!] matplotlib no está instalado; instala con 'pip install matplotlib' para exportar figuras PNG.")

    print("\n  [Proceso de Analítica Finalizado con Éxito]")
    return 0


if __name__ == "__main__":
    sys.exit(main())
