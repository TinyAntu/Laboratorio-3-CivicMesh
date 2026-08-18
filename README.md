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

## 4. Despliegue con Docker Compose

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

## 5. Despliegue en Clúster DIINF con Slurm

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

## 6. Acceso al Frontend de Estadísticas

El Frontend (Streamlit / Web UI) lee las métricas en tiempo real desde `$CIVICMESH_RUNS/<run_id>/metrics/`.

- **En ejecución local / Docker Compose**:
  Abrir en el navegador: `http://localhost:8501`

- **En ejecución en Clúster Slurm**:
  Crear un túnel SSH desde su máquina local hacia el nodo donde corre la UI:
  ```bash
  ssh -L 8501:<NODO_GPU_UI>:8501 usuario@cluster.diinf.usach.cl
  ```
  Luego acceder localmente a `http://localhost:8501`.

---

## 7. Agentes de Inteligencia Artificial (Lab 2)

El proyecto cuenta con 3 agentes automatizados en GitHub Actions:
1. **Agente Documentador (`doc_agent.py`)**: Actualiza README/CHANGELOG e inspecciona cuestiones de documentación.
2. **Agente Revisor de Bugs (`bug_agent.py`)**: Revisa el código en busca de posibles fallos de sincronización o red.
3. **Agente Revisor de MR (`MR_agent.py`)**: Analiza y comenta en cada Pull Request / Merge Request.
