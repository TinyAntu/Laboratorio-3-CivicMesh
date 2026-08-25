import os
from pathlib import Path
import time
import streamlit as st

from network.metrics import load_metrics_from_run

# Configuración inicial de la página
st.set_page_config(
    page_title="CivicMesh - Dashboard de Monitoreo Ciudadano",
    page_icon="🏙️",
    layout="wide",
)

st.title("🏙️ CivicMesh: Monitoreo Ciudadano P2P")
st.markdown(
    "*Framework Distribuido de Publish/Subscribe — Análisis de Convergencia y Tensión Realidad vs Percepción*"
)

# ---------------------------------------------------------
# 1. Configuración de Directorio y Carga de Corridas
# ---------------------------------------------------------
default_runs_dir = os.getenv("CIVICMESH_RUNS", "runs")
runs_base = Path(default_runs_dir)

st.sidebar.header("⚙️ Configuración de Corrida")
runs_dir_input = st.sidebar.text_input("Directorio de Corridas ($CIVICMESH_RUNS)", str(runs_base))
runs_dir = Path(runs_dir_input)

available_runs = []
if runs_dir.exists() and runs_dir.is_dir():
    available_runs = [d.name for d in runs_dir.iterdir() if d.is_dir() and (d / "metrics").exists()]
    available_runs.sort(reverse=True)

if not available_runs:
    st.sidebar.warning(f"No se encontraron corridas con métricas en `{runs_dir}`.")
    st.info("💡 Ejecuta un experimento local primero: `python scripts/run_experiment.py --domain crime`")
    st.stop()

selected_run = st.sidebar.selectbox("Seleccionar Corrida (Run ID)", available_runs, index=0)
auto_refresh = st.sidebar.checkbox("Auto-actualizar (Live)", value=False)
refresh_interval = st.sidebar.slider("Intervalo de refresco (s)", 1, 10, 2) if auto_refresh else 2

# ---------------------------------------------------------
# 2. Carga de Métricas desde Shared FS
# ---------------------------------------------------------
metrics_path = runs_dir / selected_run / "metrics"


@st.cache_data(ttl=1.0 if auto_refresh else 60.0)
def load_data(run_metrics_dir: str):
    # Usar el mismo loader que análisis/experimentos para mantener un único
    # contrato de lectura y no perder forward/drop/gossip.
    return load_metrics_from_run(run_metrics_dir)


records = load_data(str(metrics_path))

if not records:
    st.warning(f"La corrida `{selected_run}` aún no contiene eventos registrados.")
    st.stop()

# Clasificación de eventos
step_events = [r for r in records if r.get("event") == "step"]
publish_events = [r for r in records if r.get("event") == "publish"]
delivery_events = [r for r in records if r.get("event") == "delivery"]
drop_events = [r for r in records if r.get("event") == "drop"]
gossip_events = [r for r in records if r.get("event") == "gossip"]

detected_domains = list(set(r.get("domain") for r in step_events if "domain" in r))
current_domain = detected_domains[0] if detected_domains else "General"
active_communes = sorted(list(set(r.get("commune") for r in step_events if r.get("commune"))))
all_topics = sorted(list(set(r.get("commune") or r.get("topic") for r in records if r.get("commune") or r.get("topic"))))
communes = active_communes if active_communes else all_topics

# ---------------------------------------------------------
# 3. Métricas Principales (KPIs)
# ---------------------------------------------------------
kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)
with kpi1:
    st.metric("Dominio Activo", current_domain.upper())
with kpi2:
    st.metric("Total Eventos", len(records))
with kpi3:
    st.metric("Mensajes Entregados", len(delivery_events))
with kpi4:
    total_drops = len(drop_events)
    st.metric("Descartes (Anti-Flooding)", total_drops)
with kpi5:
    avg_hops = (
        sum(r.get("hop_count", 0) for r in delivery_events) / len(delivery_events)
        if delivery_events
        else 0
    )
    st.metric("Saltos Promedio (Hops)", f"{avg_hops:.2f}")

st.divider()

# ---------------------------------------------------------
# 4. Pestañas de Visualización y Análisis
# ---------------------------------------------------------
tab_series, tab_gap, tab_convergence, tab_network = st.tabs([
    "📈 Tópico × Canal",
    "🔍 Brecha Percepción vs Realidad",
    "🎯 Convergencia entre Peers",
    "🌐 Topología y Salud de Red",
])

# =========================================================
# TAB 1: TÓPICO X CANAL
# =========================================================
with tab_series:
    st.subheader("Evolución Temporal: Dato Objetivo vs Percepción Subjetiva")
    
    selected_commune = st.selectbox("Seleccionar Comuna / Tópico", communes, index=0)
    
    # Deduplicar y ordenar por paso temporal
    steps_map = {}
    for r in step_events:
        if r.get("commune") == selected_commune and "step" in r:
            steps_map[r["step"]] = r
    sorted_steps = sorted(steps_map.values(), key=lambda x: x["step"])

    if sorted_steps:
        import pandas as pd
        df_series = pd.DataFrame({
            "Paso (t)": [int(r["step"]) for r in sorted_steps],
            "Dato Objetivo (Ground Truth)": [float(r.get("objective_value", 0.0)) for r in sorted_steps],
            "Dato Subjetivo (Percepción)": [float(r.get("subjective_value", 0.0)) for r in sorted_steps],
            "Memoria EMA": [float(r.get("memory", 0.0)) for r in sorted_steps],
            "Rumor Gossip Recibido": [float(r.get("gossip_value", 0.0)) for r in sorted_steps],
        }).set_index("Paso (t)")
        st.line_chart(df_series)

        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown("#### Últimos Registros")
            st.dataframe(sorted_steps[-5:], use_container_width=True)
        with col_b:
            st.markdown("#### Parámetros del Canal")
            if current_domain == "crime":
                st.info("• **Objetivo**: Conteo discreto Poisson.\n• **Subjetivo**: Índice logístico $\\in [0, 1]$ con rumor gossip.")
            else:
                st.info("• **Objetivo**: Serie horaria PM2.5/PM10 (Open-Meteo).\n• **Subjetivo**: Retención de picos y saturación física $[0, 500]$.")
    else:
        st.info(f"No hay pasos registrados para la comuna `{selected_commune}`.")

# =========================================================
# TAB 2: BRECHA PERCEPCIÓN VS REALIDAD
# =========================================================
with tab_gap:
    st.subheader("Análisis Cuantitativo de la Brecha Percepción - Realidad")
    st.markdown(
        "Mide la divergencia entre la percepción subjetiva y el dato objetivo local: $\\text{Brecha}_c(t) = P_c(t) - G_c(t)$"
    )

    if step_events and active_communes:
        import pandas as pd
        gap_rows = []
        for r in step_events:
            comm = r.get("commune")
            if not comm or "step" not in r:
                continue
            gap_val = r.get("gap", float(r.get("subjective_value", 0.0)) - float(r.get("objective_value", 0.0)))
            gap_rows.append({
                "Paso (t)": int(r["step"]),
                "Comuna": comm,
                "Brecha": float(gap_val),
                "Rumor Gossip": float(r.get("gossip_value", 0.0)),
                "Memoria EMA": float(r.get("memory", 0.0)),
            })

        df_gap_raw = pd.DataFrame(gap_rows).drop_duplicates(subset=["Paso (t)", "Comuna"]).sort_values("Paso (t)")

        if not df_gap_raw.empty:
            df_pivot_gap = df_gap_raw.pivot(index="Paso (t)", columns="Comuna", values="Brecha").ffill().bfill()
            st.line_chart(df_pivot_gap)

            st.markdown("### Factores de Amplificación de la Percepción")
            g1, g2 = st.columns(2)
            with g1:
                st.markdown(r"#### Impacto de Rumores Gossip ($\hat{P}^{\text{gossip}}$)")
                df_pivot_rumors = df_gap_raw.pivot(index="Paso (t)", columns="Comuna", values="Rumor Gossip").ffill().bfill()
                st.line_chart(df_pivot_rumors)

            with g2:
                st.markdown("#### Retención de Memoria Exponencial ($M_c$)")
                df_pivot_mem = df_gap_raw.pivot(index="Paso (t)", columns="Comuna", values="Memoria EMA").ffill().bfill()
                st.line_chart(df_pivot_mem)

# =========================================================
# TAB 3: CONVERGENCIA ENTRE PEERS
# =========================================================
with tab_convergence:
    st.subheader("Consistencia del Canal Objetivo entre Nodos Suscriptores")
    st.markdown(
        "Verifica que los eventos objetivos se propaguen a todos los peers suscritos con valores idénticos y latencia acotada."
    )

    if delivery_events:
        obj_deliveries = [r for r in delivery_events if r.get("channel") == "objective"]
        
        # Agrupar entregas por msg_id
        deliveries_by_msg = {}
        for d in obj_deliveries:
            mid = d.get("msg_id")
            if mid not in deliveries_by_msg:
                deliveries_by_msg[mid] = []
            deliveries_by_msg[mid].append(d)

        conv_rows = []
        for mid, dlist in list(deliveries_by_msg.items())[-15:]:
            first_ts = min(d["timestamp"] for d in dlist)
            last_ts = max(d["timestamp"] for d in dlist)
            conv_delay = (last_ts - first_ts) * 1000.0  # ms
            peers_reached = [d["node_id"] for d in dlist]
            topic = dlist[0].get("topic", "")
            val = dlist[0].get("value")
            
            conv_rows.append({
                "ID Mensaje": mid,
                "Tópico": topic,
                "Valor Entregado": str(val),
                "Nodos Alcanzados": f"{len(peers_reached)} ({', '.join(peers_reached)})",
                "Dispersión/Delay de Convergencia": f"{conv_delay:.1f} ms",
                "Saltos Máximos": max(d.get("hop_count", 0) for d in dlist),
            })

        st.dataframe(conv_rows, use_container_width=True)
    else:
        st.info("No hay eventos de entrega registrados aún en la corrida.")

# =========================================================
# TAB 4: TOPOLOGÍA Y SALUD DE RED
# =========================================================
with tab_network:
    st.subheader("Métricas de Tráfico, Gossip y Tolerancia a Fallos")

    net1, net2 = st.columns(2)
    with net1:
        st.markdown("#### Balance de Mensajería y Anti-Flooding")
        traffic_summary = {
            "Categoría": ["Publicados", "Entregados", "Descartes por Duplicado", "Descartes por TTL"],
            "Cantidad": [
                len(publish_events),
                len(delivery_events),
                len([r for r in drop_events if r.get("reason") == "duplicate"]),
                len([r for r in drop_events if r.get("reason") == "ttl_expired"]),
            ]
        }
        st.bar_chart(traffic_summary, x="Categoría", y="Cantidad")

    with net2:
        st.markdown("#### Distribución de Saltos (Hops) por Mensaje")
        hops_counts = {}
        for d in delivery_events:
            h = d.get("hop_count", 0)
            hops_counts[h] = hops_counts.get(h, 0) + 1
        
        if hops_counts:
            hops_data = {
                "Saltos (Hops)": [f"{k} hops" for k in sorted(hops_counts.keys())],
                "Mensajes": [hops_counts[k] for k in sorted(hops_counts.keys())],
            }
            st.bar_chart(hops_data, x="Saltos (Hops)", y="Mensajes")

    if gossip_events:
        st.markdown("#### Detección de Fallos y Vistas Gossip")
        latest_gossip = gossip_events[-10:]
        st.dataframe(latest_gossip, use_container_width=True)

# ---------------------------------------------------------
# 5. Live Loop
# ---------------------------------------------------------
if auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()
