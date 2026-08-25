# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0-lab3] - 2026-08-25

### Added
- **Scripts de Experimentos**: Orquestador local multi-proceso `scripts/run_experiment.py` y generador aleatorio de lotes de simulación `scripts/run_random_experiments.py`.
- **Slurm Jobs**: Corrección e integración total en `scripts/slurm/run_civicmesh.sbatch` para la ejecución real de publicadores de ambos dominios y servidor UI en nodos GPU/CPU.

### Changed
- **Renombrado Dockerfile**: Corrección de capitalización de `dockerfile` a `Dockerfile` para estándar POSIX/OCI.

### Removed
- **Workflows vacíos**: Eliminado `.github/workflows/build_base_container.yml` de 0 bytes.

---

## [1.6.0] - 2026-08-24

### Added
- **Módulo de Métricas**: `network/metrics.py` con recolección estructurada JSONL para métricas por peer, convergencia del canal objetivo y divergencia/brecha del canal subjetivo.
- **Frontend Estadístico UI**: Interfaz interactiva en Streamlit (`frontend/app.py`) alimentada dinámicamente desde el Shared FS (`$CIVICMESH_RUNS/<run_id>/metrics`).
- **Pruebas de Métricas**: Suite de tests unitarios en `tests/test_metrics.py`.

---

## [1.5.0] - 2026-08-23

### Added
- **Dominio A (Delitos)**: Generador Poisson estocástico (`domains/crime/generator.py`), modelo de percepción ciudadana EMA (`domains/crime/perception.py`) y publicador `domains/crime/publisher.py`.
- **Dominio B (Calidad del Aire)**: Motor de replay determinista (`domains/air/replay.py`), dataset real Open-Meteo (`data/air_quality/*.csv`), modelo de percepción con memoria de picos (`domains/air/perception.py`) y publicador `domains/air/publisher.py`.
- **Script Data Downloader**: Script de ingesta automatizada `scripts/data/download_air_quality.py`.
- **Pruebas Unitarias de Datos**: Tests unitarios en `tests/test_crime_generator.py`, `test_crime_perception.py`, `test_air_replay.py` y `test_air_perception.py`.

---

## [1.4.0] - 2026-08-22

### Added
- **Capa Publish/Subscribe**: Motor Pub/Sub (`network/pubsub.py`) con colas de prioridad de reenvío y deduplicación por LRU/TTL.
- **Enrutamiento Inteligente**: Función de reenvío explícito `should_forward` con filtrado por TTL, prioridad y fanout.
- **Topología Geográfica**: Grafo de adyacencia espacial por comunas de la Región Metropolitana (`network/topology.py`).
- **Pruebas de Red**: Tests unitarios e integración multi-peer (`tests/test_pubsub.py`, `test_pubsub_integration.py`, `test_should_forward.py`, `test_topology.py`).

---

## [1.3.0] - 2026-08-20

### Added
- **Pipeline CI/CD**: Workflow GitHub Actions (`.github/workflows/ci.yml`) con ejecución automática de suite `pytest` y construcciones Docker.
- **Contenedorización**: `Dockerfile` optimizado (Python 3.11-slim) y `docker-compose.yml` para 3 peers, publicadores y UI.
- **Scripts Slurm**: `scripts/slurm/run_civicmesh.sbatch` para el clúster DIINF y `scripts/slurm/partition_experiment.sh` para pruebas de caídas de peers.

---

## [1.2.0] - 2026-08-15

### Added
- **Protocolo Gossip**: Membresía distribuida y descubrimiento parcial de la malla (`network/gossip.py`, `network/membership.py`).
- **Detector de Fallos**: Módulo `FailureDetector` (`network/failure_detector.py`) para heartbeat y detección por timeout.
- **Pruebas Gossip**: Tests unitarios en `tests/test_gossip.py`, `test_membership.py` y `test_failure_detector.py`.

---

## [1.1.0] - 2026-08-11

### Added
- **Agente de Bugs IA**: Implementación de `scripts/agents/bug_agent.py` con lectura de `config/bug_agent_config.json` y escaneo prioritario por cuota de tokens.

---

## [1.0.0] - 2026-08-09

### Added
- **Estructura Inicial**: Creación de la base de código del proyecto CivicMesh.
- **Agentes de IA del Lab 2**: Integración de Agente Documentador (`doc_agent.py`) y Agente Revisor de MR (`MR_agent.py`).
