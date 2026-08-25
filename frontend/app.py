import os
from pathlib import Path
import time
import pandas as pd
import plotly.graph_objects as go
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


def plot_interactive_highlight_chart(
    df: pd.DataFrame,
    title: str = "",
    y_title: str = "Valor",
    key_prefix: str = "chart",
    enable_fill: bool = True,
    default_selected: list[str] | None = None,
    default_highlight: str = "Ninguna (Todas iguales)",
):
    """Renderiza un gráfico interactivo Plotly con selección, sombreado y énfasis de variables."""
    if df.empty:
        st.info("No hay datos disponibles para graficar.")
        return

    columns = list(df.columns)

    col_sel, col_hl, col_fill = st.columns([3, 2, 1])
    with col_sel:
        selected_cols = st.multiselect(
            "📊 Variables a mostrar:",
            options=columns,
            default=default_selected if default_selected is not None else columns,
            key=f"{key_prefix}_cols",
        )
    with col_hl:
        highlight_options = ["Ninguna (Todas iguales)"] + selected_cols
        def_idx = highlight_options.index(default_highlight) if default_highlight in highlight_options else 0
        highlight_col = st.selectbox(
            "🎯 Destacar variable (Focus):",
            options=highlight_options,
            index=def_idx,
            key=f"{key_prefix}_highlight",
        )
    with col_fill:
        show_fill = st.checkbox("Sombreado", value=True, key=f"{key_prefix}_fill") if enable_fill else False

    if not selected_cols:
        st.warning("Selecciona al menos una variable para visualizar.")
        return

    color_palette = [
        "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728",
        "#9467bd", "#8c564b", "#e377c2", "#7f7f7f",
        "#bcbd22", "#17becf"
    ]

    fig = go.Figure()
    has_focus = (highlight_col != "Ninguna (Todas iguales)" and highlight_col in selected_cols)

    for i, col in enumerate(selected_cols):
        base_color = color_palette[i % len(color_palette)]
        is_highlighted = (has_focus and col == highlight_col)

        if has_focus:
            if is_highlighted:
                line_dict = dict(width=3.8, color=base_color)
                opacity = 1.0
                fill_mode = "tozeroy" if show_fill else "none"
                try:
                    r_val = int(base_color[1:3], 16)
                    g_val = int(base_color[3:5], 16)
                    b_val = int(base_color[5:7], 16)
                    fill_color = f"rgba({r_val}, {g_val}, {b_val}, 0.20)"
                except Exception:
                    fill_color = "rgba(31, 119, 180, 0.20)"
            else:
                line_dict = dict(width=1.3, dash="dot" if i % 2 == 1 else "solid")
                opacity = 0.25
                fill_mode = "none"
                fill_color = None
        else:
            line_dict = dict(width=2.4, color=base_color)
            opacity = 0.9
            fill_mode = "none"
            fill_color = None

        fig.add_trace(
            go.Scatter(
                x=df.index,
                y=df[col],
                name=f"⭐ {col}" if is_highlighted else col,
                mode="lines",
                line=line_dict,
                opacity=opacity,
                fill=fill_mode,
                fillcolor=fill_color,
                hovertemplate=f"<b>{col}</b>: %{{y:.3f}}<extra></extra>",
            )
        )

    fig.update_layout(
        title=dict(text=title, font=dict(size=14)) if title else None,
        xaxis_title="Paso Temporal (t)",
        yaxis_title=y_title,
        hovermode="x unified",
        margin=dict(l=20, r=20, t=35 if title else 15, b=20),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            bgcolor="rgba(0,0,0,0)",
        ),
        template="plotly_white",
        height=430,
    )

    st.plotly_chart(fig, use_container_width=True)


# ---------------------------------------------------------
# 1. Configuración de Directorio y Carga de Corridas
# ---------------------------------------------------------
default_runs_dir = os.getenv("CIVICMESH_RUNS", "runs")
runs_base = Path(default_runs_dir)

st.sidebar.header("⚙️ Configuración de Corrida")
runs_dir_input = st.sidebar.text_input("Directorio de Corridas ($CIVICMESH_RUNS)", str(runs_base))
runs_dir = Path(runs_dir_input)


def find_runs(base: Path) -> list[str]:
    if not base.exists() or not base.is_dir():
        return []
    runs = []
    for d in base.iterdir():
        if d.is_dir():
            if (d / "metrics").exists() or (d / "metricas").exists() or list(d.glob("*.jsonl")):
                runs.append(d.name)
    runs.sort(reverse=True)
    return runs


available_runs = find_runs(runs_dir)

if not available_runs:
    st.sidebar.warning(f"No se encontraron corridas con métricas en `{runs_dir}`.")
    st.info(
        "💡 Especifica la ruta del directorio de corridas (por ej. `runs` o `path/to/runs`) "
        "o ejecuta un experimento local: `python scripts/run_experiment.py --domain crime`"
    )
    st.stop()

selected_run = st.sidebar.selectbox("Seleccionar Corrida (Run ID)", available_runs, index=0)
auto_refresh = st.sidebar.checkbox("Auto-actualizar (Live)", value=False)
refresh_interval = st.sidebar.slider("Intervalo de refresco (s)", 1, 10, 2) if auto_refresh else 2

# ---------------------------------------------------------
# 2. Carga de Métricas desde Shared FS
# ---------------------------------------------------------
selected_run_dir = runs_dir / selected_run
metrics_path = selected_run_dir / "metrics"
if not metrics_path.exists():
    metrics_path = selected_run_dir / "metricas"
if not metrics_path.exists():
    metrics_path = selected_run_dir


@st.cache_data(ttl=1.0 if auto_refresh else 60.0)
def load_data(run_metrics_dir: str):
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
membership_events = [r for r in records if r.get("event") == "membership_change"]
control_events = [r for r in records if r.get("event") == "failure_injection"]

# Reconstrucción de pasos si la corrida proviene exclusivamente de logs de peers en clúster
if not step_events and delivery_events:
    step_dict = {}
    for r in records:
        if r.get("event") == "delivery" and r.get("channel") == "subjective":
            meta = r.get("metadata", {})
            step = meta.get("step")
            commune = r.get("topic")
            domain = meta.get("domain", "general")
            if step is not None and commune:
                key = (domain, commune, step)
                if key not in step_dict:
                    obj_val = meta.get("objective_value", meta.get("total_crimes", 0.0))
                    subj_val = float(r.get("value", 0.0))
                    step_dict[key] = {
                        "event": "step",
                        "timestamp": r.get("timestamp"),
                        "domain": domain,
                        "commune": commune,
                        "step": step,
                        "objective_value": float(obj_val),
                        "subjective_value": subj_val,
                        "gap": subj_val - float(obj_val),
                        "memory": float(meta.get("memory", 0.0)),
                        "gossip_value": float(meta.get("gossip_value", 0.0)),
                    }
    step_events = sorted(step_dict.values(), key=lambda x: (x["domain"], x["commune"], x["step"]))

detected_domains = sorted(list(set(r.get("domain") for r in step_events if "domain" in r)))
if len(detected_domains) > 1:
    selected_domain = st.sidebar.selectbox("🎯 Dominio de Análisis", detected_domains, index=0)
elif detected_domains:
    selected_domain = detected_domains[0]
else:
    selected_domain = "General"

current_domain = selected_domain
domain_step_events = [r for r in step_events if r.get("domain") == current_domain] if detected_domains else step_events

active_communes = sorted(list(set(r.get("commune") for r in domain_step_events if r.get("commune"))))
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
    st.subheader(f"Evolución Temporal: Dato Objetivo vs Percepción Subjetiva ({current_domain.upper()})")

    selected_commune = st.selectbox("Seleccionar Comuna / Tópico", communes, index=0)

    # Deduplicar y ordenar por paso temporal
    steps_map = {}
    for r in domain_step_events:
        if r.get("commune") == selected_commune and "step" in r:
            steps_map[r["step"]] = r
    sorted_steps = sorted(steps_map.values(), key=lambda x: x["step"])

    if sorted_steps:
        df_series = pd.DataFrame({
            "Paso (t)": [int(r["step"]) for r in sorted_steps],
            "Dato Objetivo (Ground Truth)": [float(r.get("objective_value", 0.0)) for r in sorted_steps],
            "Dato Subjetivo (Percepción)": [float(r.get("subjective_value", 0.0)) for r in sorted_steps],
            "Memoria EMA": [float(r.get("memory", 0.0)) for r in sorted_steps],
            "Rumor Gossip Recibido": [float(r.get("gossip_value", 0.0)) for r in sorted_steps],
        }).set_index("Paso (t)")

        plot_interactive_highlight_chart(
            df_series,
            title=f"Evolución de Variables en {selected_commune}",
            y_title="Valor / Conteo",
            key_prefix="series",
            enable_fill=True,
        )

        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown("#### Últimos Registros")
            st.dataframe(sorted_steps[-5:], width="stretch")
        with col_b:
            st.markdown("#### Parámetros del Canal")
            if current_domain == "crime":
                st.info("• **Objetivo**: Conteo discreto Poisson.\n• **Subjetivo**: Índice logístico $\\in [0, 1]$ con rumor gossip.")
            else:
                st.info("• **Objetivo**: Serie horaria PM2.5/PM10 (Open-Meteo).\n• **Subjetivo**: Retención de picos y saturación física $[0, 500]$.")
    else:
        st.info(f"No hay pasos registrados para la comuna `{selected_commune}` en el dominio `{current_domain}`.")

# =========================================================
# TAB 2: BRECHA PERCEPCIÓN VS REALIDAD
# =========================================================
with tab_gap:
    st.subheader(f"Análisis Cuantitativo de la Brecha Percepción - Realidad ({current_domain.upper()})")
    st.markdown(
        r"Mide la divergencia entre la percepción subjetiva y el dato objetivo local: $\text{Brecha}_c(t) = P_c(t) - G_c(t)$"
    )

    if domain_step_events and active_communes:
        gap_rows = []
        for r in domain_step_events:
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
            
            st.markdown("### Brecha de Percepción por Comuna")
            plot_interactive_highlight_chart(
                df_pivot_gap,
                title="Divergencia Temporal (Percepción - Ground Truth)",
                y_title="Brecha",
                key_prefix="gap",
                enable_fill=True,
            )

            st.markdown("### Factores de Amplificación de la Percepción")
            g1, g2 = st.columns(2)
            with g1:
                st.markdown(r"#### Impacto de Rumores Gossip ($\hat{P}^{\text{gossip}}$)")
                df_pivot_rumors = df_gap_raw.pivot(index="Paso (t)", columns="Comuna", values="Rumor Gossip").ffill().bfill()
                plot_interactive_highlight_chart(
                    df_pivot_rumors,
                    title="Rumores Gossip por Comuna",
                    y_title="P̂_gossip",
                    key_prefix="rumor",
                    enable_fill=False,
                )

            with g2:
                st.markdown(r"#### Retención de Memoria Exponencial ($M_c$)")
                df_pivot_mem = df_gap_raw.pivot(index="Paso (t)", columns="Comuna", values="Memoria EMA").ffill().bfill()
                plot_interactive_highlight_chart(
                    df_pivot_mem,
                    title="Memoria EMA por Comuna",
                    y_title="M_c",
                    key_prefix="mem",
                    enable_fill=False,
                )

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
        for mid, dlist in list(deliveries_by_msg.items())[-20:]:
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

    if control_events:
        st.markdown("#### ⚠️ Experimento de Tolerancia a Fallos / Partición")
        st.dataframe(control_events, use_container_width=True)

    net1, net2 = st.columns(2)
    with net1:
        st.markdown("#### Balance de Mensajería y Anti-Flooding")
        cats = ["Entregados", "Descartes por Duplicado", "Descartes por TTL"]
        vals = [
            len(delivery_events),
            len([r for r in drop_events if r.get("reason") == "duplicate"]),
            len([r for r in drop_events if r.get("reason") == "ttl_expired"]),
        ]
        if publish_events:
            cats.insert(0, "Publicados")
            vals.insert(0, len(publish_events))

        fig_traffic = go.Figure(data=[
            go.Bar(
                x=cats,
                y=vals,
                marker=dict(color=["#2ca02c", "#d62728", "#ff7f0e", "#1f77b4"][:len(cats)]),
                text=vals,
                textposition="auto",
            )
        ])
        fig_traffic.update_layout(
            margin=dict(l=10, r=10, t=20, b=20),
            height=320,
            template="plotly_white",
            yaxis_title="Cantidad de Mensajes",
        )
        st.plotly_chart(fig_traffic, use_container_width=True)

    with net2:
        st.markdown("#### Distribución de Saltos (Hops) por Mensaje")
        hops_counts = {}
        for d in delivery_events:
            h = d.get("hop_count", 0)
            hops_counts[h] = hops_counts.get(h, 0) + 1

        if hops_counts:
            hop_keys = sorted(hops_counts.keys())
            fig_hops = go.Figure(data=[
                go.Bar(
                    x=[f"{k} hops" for k in hop_keys],
                    y=[hops_counts[k] for k in hop_keys],
                    marker=dict(color="#1f77b4"),
                    text=[hops_counts[k] for k in hop_keys],
                    textposition="auto",
                )
            ])
            fig_hops.update_layout(
                margin=dict(l=10, r=10, t=20, b=20),
                height=320,
                template="plotly_white",
                xaxis_title="Saltos (Hops)",
                yaxis_title="Mensajes Entregados",
            )
            st.plotly_chart(fig_hops, use_container_width=True)

    if membership_events:
        st.markdown("#### Transiciones de Membresía Detectadas")
        latest_membership = membership_events[-20:]
        st.dataframe(latest_membership, use_container_width=True)

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

