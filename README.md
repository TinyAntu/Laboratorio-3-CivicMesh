# CivicMesh - Laboratorio 3: Framework P2P de Publish/Subscribe

**CivicMesh** es una aplicación de monitoreo ciudadano distribuido basada en una capa de comunicación P2P reutilizable (Gossip + Pub/Sub por tópico geográfico por comuna o región). El sistema evalúa la tensión entre datos objetivos (*ground truth*) y datos subjetivos (*percepción ciudadana*) sobre dos dominios:
- **Dominio A (Delitos)**: Eventos discretos simulados por comuna frente a índices de sensación de inseguridad.
- **Dominio B (Calidad del Aire)**: Series temporales reales de contaminantes (PM2.5/PM10 de SINCA / Open-Meteo) inyectadas por publicadores de replay frente a la percepción ciudadana.

---

## 1. Organización del Equipo y Roles

| Rol | Encargado | Responsabilidades Concretas |
| :--- | :--- | :--- |
| **1. Líder de Capa de Red / Gossip** | Benjamin Moya | Membresía, descubrimiento, tolerancia a fallos. |
| **2. Líder de Capa Pub/Sub** | Braulio Bravo | Tópicos, suscripciones, should_forward, fanout. |
| **3. Líder de Datos** | Diego Molina | Ingesta/cache Dominio B (SINCA/Open-Meteo → replay); generadores Poisson y percepción (Sección 4.3); configuración de tasas/sesgos. |
| **4. Líder de Analítica y Estadística** | Sebastian de la Fuente | métricas de convergencia/divergencia; experimentos de caída/partición; frontend de estadísticas (Sección 5.4). |
| **5. Líder de CI/CD, Git y agentes** | Alonso Henriquez | Pipeline CI verde con tests; Dockerfile y docker-compose; ramas/issues/MR; los tres agentes del Lab 2; scripts sbatch/shared FS; README (local,Compose, clúster y cómo abrir la UI). |

---

## 2. Requisitos Previos e Instalación

### Requisitos
- **Python**: 3.11 o superior.
- **Docker & Docker Compose**: (Para ejecución en contenedores).
- **pytest**: (Para ejecución de suite de pruebas unitarias).

### Instalación de dependencias locales
```bash
pip install -r requirements.txt
```

---

## 3. Pruebas Unitarias e Integración

Para ejecutar la suite de pruebas unitarias localmente:
```bash
make test
# O directamente con pytest:
python -m pytest -v tests/
```

Las pruebas en el repositorio verifican:
- Membresía Gossip y actualización de vista de peers.
- Detección de fallos por timeout (`FailureDetector`).
- Lógica de reenvío `should_forward` y prioridad/TTL.

---

## 4. Ejecución de Experimentos Locales

Además de Docker Compose y Slurm, se incluyen scripts para orquestar simulación multi-proceso de forma local:

### Experimento Individual
```bash
# Ejecutar simulación local del Dominio A (Delitos)
python scripts/run_experiment.py --domain crime --num-peers 4 --duration 15

# Ejecutar simulación local del Dominio B (Aire) con simulación de caída de peer
python scripts/run_experiment.py --domain air --num-peers 5 --duration 20 --kill-peer peer1 --kill-time 8
```

### Lotes de Experimentos Aleatorios
```bash
# Ejecutar un lote de 3 experimentos con parámetros aleatorios
python scripts/run_random_experiments.py --count 3
```

---

## 5. Despliegue con Docker Compose

El proyecto incluye un entorno Docker en contenedores preparado con **3 peers + 1 publicador + 1 frontend**:

### Comandos de Ejecución
```bash
# Construir e iniciar el cluster en contenedores
docker compose up --build

# Para detener los servicios
docker compose down
```

### Arquitectura de Contenedores en Compose:
- **`peer0`**: Peer semilla inicial (Host: `peer0`, Puerto TCP: `9000`).
- **`peer1`**: Segundo Peer (Host: `peer1`, Puerto TCP: `9001`), se conecta a `peer0`.
- **`peer2`**: Tercer Peer (Host: `peer2`, Puerto TCP: `9002`), se conecta a `peer0`.
- **`publisher0`**: Proceso publicador que inyecta mensajes en la malla.
- **`frontend`**: Servidor Web UI expuesto en el puerto **8501**.

---

## 6. Despliegue en Clúster DIINF con Slurm

En el clúster DIINF, el trabajo se distribuye en **2 nodos CPU** (peers gossip/pub-sub) y **2 nodos GPU** (usando **solo la CPU del host** para publicadores y frontend UI), coordinados mediante el filesystem compartido (**Shared FS**).

### Convención de Directorios Shared FS
Todas las corridas utilizan la variable de entorno `$CIVICMESH_RUNS`. El directorio de salida para un job se estructura como:
```text
$CIVICMESH_RUNS/<run_id>/
├── hostfile.txt      # Direcciones host:port por cada peer activo
├── config.yaml       # Semilla RNG, comunas, TTL, fanout y parametros
├── metrics/          # Archivos JSONL de metricas por peer o agregadas
└── logs/             # Logs stdout/stderr de cada proceso
```

### Envío del Job a Slurm
```bash
# Enviar el script sbatch
sbatch scripts/slurm/run_civicmesh.sbatch

# Verificar el estado del job
squeue -u $USER
```

### Simulación de Caída de Peer / Partición de Red
Para evaluar la robustez de la malla ante caídas de nodos en Slurm:
```bash
./scripts/slurm/partition_experiment.sh <SLURM_JOB_ID>
```

---

## 7. Acceso al Frontend de Estadísticas

El Frontend (Streamlit / Web UI) lee las métricas en tiempo real desde `$CIVICMESH_RUNS/<run_id>/metrics/`.

- **En ejecución local / Docker Compose**:
  Abrir en el navegador: `http://localhost:8501`

- **En ejecución en Clúster Slurm**:
  Crear un túnel SSH desde su máquina local hacia el nodo donde corre la UI:
  ```bash
  ssh -L 8501:<NODO_GPU_UI>:8501 usuario@xi.diinf.usach.cl
  ```
  Luego acceder localmente a `http://localhost:8501`.

---

## 8. Agentes de Inteligencia Artificial (Lab 2)

El proyecto cuenta con 3 agentes automatizados en GitHub Actions:
1. **Agente Documentador (`doc_agent.py`)**: Actualiza README/CHANGELOG e inspecciona cuestiones de documentación.
2. **Agente Revisor de Bugs (`bug_agent.py`)**: Revisa el código en busca de posibles fallos de sincronización o red.
3. **Agente Revisor de MR (`MR_agent.py`)**: Analiza y comenta en cada Pull Request / Merge Request.

---

## 9. Parametrización y Uso de Semillas Estocásticas (Seeds) para Reproducibilidad

CivicMesh garantiza la **reproducibilidad determinista** de todos sus experimentos mediante un esquema explícito de semillas estocásticas y aislamiento de generadores pseudorandom.

### 9.1 Distinción de Conceptos: Semillas Gossip vs. Semillas Estocásticas

Es fundamental diferenciar los dos tipos de "semillas" presentes en el proyecto:

1. **Semillas Gossip (Discovery Seeds)**:
   - **Propósito**: Descubrimiento inicial de nodos en la red P2P.
   - **Archivos**: `seeds.txt` (local), `seeds_compose.txt` (Docker Compose) y `hostfile.txt` (Slurm / Shared FS).
   - **Formato**: Tuplas `node_id host port [comunas]`.

2. **Semillas Estocásticas (RNG Seeds)**:
   - **Propósito**: Inicialización del generador de números aleatorios (Random Number Generator) para garantizar la reproducibilidad exacta de los procesos estocásticos (delitos Poisson y ruidos de percepción).
   - **Configuración**: Campo `seed` en `config/civicmesh.yaml` (ej. `seed: 42`) o argumento de línea de comandos `--seed <INT>`.

### 9.2 Mecánica Interna de Generación y Aislamiento RNG

Para evitar interferencia y variaciones por el orden de ejecución de hilos o subprocesos, CivicMesh crea instancias aisladas de `random.Random` utilizando claves jerárquicas hash deterministas derivadas de la semilla global (`seed`):

- **Generador de Delitos (Dominio A)**:
  ```python
  rng = random.Random(f"{seed}:crime:{commune}:{crime_type}")
  ```
  Genera eventos Poisson $X_{c,k}(t) \sim \text{Poisson}(\lambda_{c,k} \Delta t)$ idénticos en cada simulación para la misma comuna y tipo de delito.

- **Modelo Subjetivo de Inseguridad (Dominio A)**:
  ```python
  rng = random.Random(f"{seed}:crime-perception:{commune}")
  ```
  Genera el ruido gaussiano $\varepsilon_c(t) \sim \mathcal{N}(0, \sigma_\varepsilon^2)$ utilizado en el índice de percepción $P_c(t) = \sigma(Z_c(t))$.

- **Modelo Subjetivo de Calidad del Aire (Dominio B)**:
  ```python
  rng = random.Random(f"{seed}:air-perception:{commune}")
  ```
  Genera el ruido gaussiano $\varepsilon_c(t)$ en el modelo subjetivo de picos de contaminación.

- **Canal Objetivo del Aire (Dominio B)**:
  Serie temporal real (Open-Meteo / SINCA) reproducida secuencial y deterministamente mediante `AirQualityReplay` desde `data/air_quality/*.csv`.

### 9.3 Inyección de Semilla y Ejecución Reproducible

Cuando se ejecuta un experimento:

1. **En Orquestador Local (`scripts/run_experiment.py`)**:
   Se puede especificar el parámetro `--seed`:
   ```bash
   python scripts/run_experiment.py --domain crime --num-peers 3 --seed 42 --duration 15
   ```
   El orquestador escribe este valor en `$CIVICMESH_RUNS/<run_id>/config.yaml`, garantizando que todos los publicadores lean exactamente la misma semilla.

2. **En Lotes Aleatorios (`scripts/run_random_experiments.py`)**:
   Cada corrida genera y registra una semilla aleatoria (e.g. `--seed 8492`) que queda plasmada en los logs y en la carpeta `$CIVICMESH_RUNS/<run_id>/config.yaml` para permitir su réplica exacta en defensas o análisis.

3. **En Docker Compose / Slurm**:
   Los contenedores y tareas de Slurm leen la semilla configurada en el archivo de configuración versionado `config/civicmesh.yaml` o el `config.yaml` creado en la corrida Shared FS.

4. **Verificación Automática en CI/CD**:
   La suite de pruebas unitarias incluye verificaciones explícitas de reproducibilidad:
   ```bash
   python -m pytest tests/test_crime_generator.py tests/test_crime_perception.py tests/test_air_perception.py
   ```
   Estas pruebas confirman que ante una misma semilla se obtiene la misma secuencia de datos objetivos y subjetivos.

