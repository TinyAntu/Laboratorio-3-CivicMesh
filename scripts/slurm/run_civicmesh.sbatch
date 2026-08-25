#!/bin/bash

#SBATCH --job-name=civicmesh
#SBATCH --partition=batch
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=2
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --time=00:30:00
#SBATCH --output=civicmesh_%j.log
#SBATCH --error=civicmesh_%j.err

#SBATCH hetjob

#SBATCH --partition=GPU
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=8
#SBATCH --cpus-per-task=1
#SBATCH --mem=16G
#SBATCH --time=00:30:00

set -uo pipefail

# ------------------------------------------------------------------------------
# Configuración general
# ------------------------------------------------------------------------------
PROJECT_ROOT="${CIVICMESH_PROJECT_ROOT:-${SLURM_SUBMIT_DIR:-$PWD}}"
cd "${PROJECT_ROOT}"
export PYTHONPATH="${PROJECT_ROOT}:${PYTHONPATH:-}"

# ------------------------------------------------------------------------------
# Python del clúster
# ------------------------------------------------------------------------------
PYTHON_BIN="python3"

if ! command -v "${PYTHON_BIN}" >/dev/null 2>&1; then
    echo "[ERROR] No se encontró python3 en el entorno del clúster." >&2
    exit 1
fi

echo "[SLURM] Python=$("${PYTHON_BIN}" --version 2>&1)"

if ! "${PYTHON_BIN}" -c "import yaml" >/dev/null 2>&1; then
    echo "[ERROR] PyYAML no está disponible para python3." >&2
    echo "        Instálalo con: python3 -m pip install --user -r requirements.txt" >&2
    exit 1
fi

if ! "${PYTHON_BIN}" -c "import streamlit" >/dev/null 2>&1; then
    echo "[ERROR] Streamlit no está disponible para python3." >&2
    echo "        Instálalo con: python3 -m pip install --user -r requirements.txt" >&2
    exit 1
fi

echo "[SLURM] Dependencias Python básicas OK"


RUN_ID="${SLURM_JOB_ID}"
CPU_JOB_ID="${SLURM_JOB_ID_HET_GROUP_0:-${SLURM_JOB_ID}}"
GPU_JOB_ID="${SLURM_JOB_ID_HET_GROUP_1:-}"
BASE_RUNS_DIR="${CIVICMESH_RUNS:-$HOME/civicmesh_runs}"
RUN_DIR="${BASE_RUNS_DIR}/${RUN_ID}"
RUN_CONFIG="${RUN_DIR}/config.yaml"
HOSTFILE="${RUN_DIR}/hostfile.txt"
PROCESS_REGISTRY="${RUN_DIR}/processes.tsv"
DOMAIN="${CIVICMESH_DOMAIN:-both}"   # crime | air | both

mkdir -p "${RUN_DIR}/metrics" "${RUN_DIR}/logs"

if [[ ! -f "config/civicmesh.yaml" ]]; then
    echo "[ERROR] No se encontró config/civicmesh.yaml desde ${PROJECT_ROOT}" >&2
    exit 1
fi

# La configuración efectiva de la corrida queda versionada en el shared FS.
cp "config/civicmesh.yaml" "${RUN_CONFIG}"

# ------------------------------------------------------------------------------
# Validar allocation heterogénea: 2 CPU + 2 GPU
# ------------------------------------------------------------------------------
if [[ "${SLURM_HET_SIZE:-0}" -lt 2 ]]; then
    echo "[ERROR] Este script requiere un heterogeneous job de Slurm (2 grupos)." >&2
    echo "        Componente 0 = CPU, componente 1 = GPU." >&2
    exit 1
fi

CPU_NODELIST="${SLURM_JOB_NODELIST_HET_GROUP_0:-}"
GPU_NODELIST="${SLURM_JOB_NODELIST_HET_GROUP_1:-}"

if [[ -z "${CPU_NODELIST}" || -z "${GPU_NODELIST}" ]]; then
    echo "[ERROR] Slurm no expuso los node lists de ambos grupos heterogéneos." >&2
    exit 1
fi

mapfile -t CPU_NODES < <(scontrol show hostnames "${CPU_NODELIST}")
mapfile -t GPU_NODES < <(scontrol show hostnames "${GPU_NODELIST}")

if [[ ${#CPU_NODES[@]} -ne 2 || ${#GPU_NODES[@]} -ne 2 ]]; then
    echo "[ERROR] Se esperaban exactamente 2 hosts CPU + 2 hosts GPU." >&2
    echo "        CPU=${CPU_NODES[*]} GPU=${GPU_NODES[*]}" >&2
    exit 1
fi

CPU_NODE_1="${CPU_NODES[0]}"
CPU_NODE_2="${CPU_NODES[1]}"
GPU_NODE_1="${GPU_NODES[0]}"
GPU_NODE_2="${GPU_NODES[1]}"

cat > "${RUN_DIR}/allocation.txt" <<EOF_ALLOC
run_id=${RUN_ID}
project_root=${PROJECT_ROOT}
domain=${DOMAIN}
cpu_node_1=${CPU_NODE_1}
cpu_node_2=${CPU_NODE_2}
gpu_node_1=${GPU_NODE_1}
gpu_node_2=${GPU_NODE_2}
cpu_job_id=${CPU_JOB_ID}
gpu_job_id=${GPU_JOB_ID}
frontend=http://${GPU_NODE_2}:8501
EOF_ALLOC

printf '[SLURM] RUN_ID=%s\n' "${RUN_ID}"
printf '[SLURM] Shared FS=%s\n' "${RUN_DIR}"
printf '[SLURM] CPU hosts: %s %s (component job %s)\n' "${CPU_NODE_1}" "${CPU_NODE_2}" "${CPU_JOB_ID}"
printf '[SLURM] GPU hosts (CPU only): %s %s (component job %s)\n' "${GPU_NODE_1}" "${GPU_NODE_2}" "${GPU_JOB_ID}"

# ------------------------------------------------------------------------------
# Leer parámetros efectivos desde config.yaml
# ------------------------------------------------------------------------------
mapfile -t CFG_VALUES < <("${PYTHON_BIN}" - "${RUN_CONFIG}" <<'PY'
import sys
import yaml

with open(sys.argv[1], encoding="utf-8") as fh:
    cfg = yaml.safe_load(fh)

print(cfg.get("seed", 42))
print(cfg["pubsub"]["objective"].get("fanout", 3))
print(cfg["pubsub"]["subjective"].get("fanout", 2))
print(cfg["pubsub"]["objective"].get("ttl", 3))
print(cfg["pubsub"]["subjective"].get("ttl", 5))
print(cfg["pubsub"]["objective"].get("priority", 80))
print(cfg["pubsub"]["subjective"].get("priority", 50))
PY
)

if [[ ${#CFG_VALUES[@]} -ne 7 ]]; then
    echo "[ERROR] No se pudo leer correctamente config.yaml." >&2
    echo "        Se esperaban 7 valores y se obtuvieron ${#CFG_VALUES[@]}." >&2
    exit 1
fi

SEED="${CFG_VALUES[0]}"
FANOUT_OBJECTIVE="${CFG_VALUES[1]}"
FANOUT_SUBJECTIVE="${CFG_VALUES[2]}"
TTL_OBJECTIVE="${CFG_VALUES[3]}"
TTL_SUBJECTIVE="${CFG_VALUES[4]}"
PRIORITY_OBJECTIVE="${CFG_VALUES[5]}"
PRIORITY_SUBJECTIVE="${CFG_VALUES[6]}"
GOSSIP_FANOUT="${CIVICMESH_GOSSIP_FANOUT:-2}"
MAX_VIEW_SIZE="${CIVICMESH_MAX_VIEW_SIZE:-8}"
CONTROL_TIMEOUT="${CIVICMESH_CONTROL_TIMEOUT:-0.75}"
LISTEN_BACKLOG="${CIVICMESH_LISTEN_BACKLOG:-512}"
# Comunas disponibles en ambos dominios según config.
mapfile -t COMMUNES < <("${PYTHON_BIN}" - "${RUN_CONFIG}" <<'PY'
import sys
import yaml

with open(sys.argv[1], encoding="utf-8") as fh:
    cfg = yaml.safe_load(fh)

crime = set(cfg.get("crime", {}).get("rates", {}).keys())
air = set(cfg.get("air", {}).get("datasets", {}).keys())
for commune in sorted(crime & air):
    print(commune)
PY
)

if [[ ${#COMMUNES[@]} -eq 0 ]]; then
    echo "[ERROR] No existen comunas compartidas entre crime.rates y air.datasets." >&2
    exit 1
fi

# Lista CSV para el argumento --topics. El hostfile no incluye tópicos porque
# su parser usa espacios como separador y comunas como "Las Condes" contienen
# espacios; las suscripciones se anuncian por el protocolo Pub/Sub.
TOPICS="$(IFS=,; echo "${COMMUNES[*]}")"

# ------------------------------------------------------------------------------
# Hostfile de los peers CPU
# ------------------------------------------------------------------------------
cat > "${HOSTFILE}" <<EOF_HOSTS
p0 ${CPU_NODE_1} 9000
p1 ${CPU_NODE_1} 9001
p2 ${CPU_NODE_2} 9002
p3 ${CPU_NODE_2} 9003
EOF_HOSTS

printf "name\thet_group\thost\tport\tlog\n" > "${PROCESS_REGISTRY}"

# Mantener PIDs locales de los clientes srun para cleanup del batch.
CHILDREN=()

cleanup() {
    local rc=$?
    trap - EXIT INT TERM
    echo "[SLURM] Finalizando pasos restantes..."
    for pid in "${CHILDREN[@]:-}"; do
        kill "${pid}" 2>/dev/null || true
    done
    exit "${rc}"
}
trap cleanup EXIT INT TERM

launch_step() {
    local name="$1"
    local het_group="$2"
    local host="$3"
    local port="$4"
    local logfile="$5"
    shift 5

    echo "[SLURM] launch ${name} group=${het_group} host=${host} port=${port}"
    printf "%s\t%s\t%s\t%s\t%s\n" \
        "${name}" "${het_group}" "${host}" "${port}" "${logfile}" \
        >> "${PROCESS_REGISTRY}"

    srun \
        --het-group="${het_group}" \
        --nodelist="${host}" \
        --nodes=1 \
        --ntasks=1 \
        --cpus-per-task=1 \
        --exact \
        --overlap \
        --job-name="${name}" \
        "$@" \
        > "${logfile}" 2>&1 &

    CHILDREN+=("$!")
}

# ------------------------------------------------------------------------------
# Peers: 2 procesos por cada host CPU
# ------------------------------------------------------------------------------
PEER_COMMON=(
    --fanout "${GOSSIP_FANOUT}"
    --max-view-size "${MAX_VIEW_SIZE}"
    --control-timeout "${CONTROL_TIMEOUT}"
    --listen-backlog "${LISTEN_BACKLOG}"
    --pubsub-fanout-objective "${FANOUT_OBJECTIVE}"
    --pubsub-fanout-subjective "${FANOUT_SUBJECTIVE}"
    --ttl-objective "${TTL_OBJECTIVE}"
    --ttl-subjective "${TTL_SUBJECTIVE}"
    --priority-objective "${PRIORITY_OBJECTIVE}"
    --priority-subjective "${PRIORITY_SUBJECTIVE}"
    --topics "${TOPICS}"
    --runs-dir "${BASE_RUNS_DIR}"
    --run-id "${RUN_ID}"
)

launch_step "peer-p0" 0 "${CPU_NODE_1}" 9000 "${RUN_DIR}/logs/peer_p0.log" \
    "${PYTHON_BIN}" -m network.peer --node-id p0 --host "${CPU_NODE_1}" --port 9000 \
    --seed "$((SEED + 0))" "${PEER_COMMON[@]}"

launch_step "peer-p1" 0 "${CPU_NODE_1}" 9001 "${RUN_DIR}/logs/peer_p1.log" \
    "${PYTHON_BIN}" -m network.peer --node-id p1 --host "${CPU_NODE_1}" --port 9001 \
    --seeds-file "${HOSTFILE}" --seed "$((SEED + 1))" "${PEER_COMMON[@]}"

launch_step "peer-p2" 0 "${CPU_NODE_2}" 9002 "${RUN_DIR}/logs/peer_p2.log" \
    "${PYTHON_BIN}" -m network.peer --node-id p2 --host "${CPU_NODE_2}" --port 9002 \
    --seeds-file "${HOSTFILE}" --seed "$((SEED + 2))" "${PEER_COMMON[@]}"

launch_step "peer-p3" 0 "${CPU_NODE_2}" 9003 "${RUN_DIR}/logs/peer_p3.log" \
    "${PYTHON_BIN}" -m network.peer --node-id p3 --host "${CPU_NODE_2}" --port 9003 \
    --seeds-file "${HOSTFILE}" --seed "$((SEED + 3))" "${PEER_COMMON[@]}"

# Dar tiempo a que los sockets estén escuchando y se estabilice JOIN/Gossip.
sleep "${CIVICMESH_BOOTSTRAP_SECONDS:-8}"

# ------------------------------------------------------------------------------
# Publishers: hosts GPU, pero explícitamente sin uso de CUDA/GPU.
# Se levanta un publisher principal y una fuente subjective-only por comuna.
# ------------------------------------------------------------------------------
if [[ "${DOMAIN}" == "crime" || "${DOMAIN}" == "both" ]]; then
    idx=0
    for commune in "${COMMUNES[@]}"; do
        slug="$(printf '%s' "${commune}" | tr '[:upper:] ' '[:lower:]_' | tr -cd '[:alnum:]_-')"
        main_port=$((9500 + idx * 2))
        rumor_port=$((main_port + 1))

        launch_step "crime-${slug}" 1 "${GPU_NODE_1}" "${main_port}" \
            "${RUN_DIR}/logs/crime_${slug}.log" \
            env CUDA_VISIBLE_DEVICES="" "${PYTHON_BIN}" -m domains.crime.publisher \
            --node-id "pub_crime_${slug}" --host "${GPU_NODE_1}" --port "${main_port}" \
            --commune "${commune}" --config "${RUN_CONFIG}" \
            --seeds-file "${HOSTFILE}" --topics "${commune}" \
            --control-timeout "${CONTROL_TIMEOUT}" --listen-backlog "${LISTEN_BACKLOG}" \
            --runs-dir "${BASE_RUNS_DIR}" --run-id "${RUN_ID}"

        launch_step "crime-rumor-${slug}" 1 "${GPU_NODE_1}" "${rumor_port}" \
            "${RUN_DIR}/logs/crime_rumor_${slug}.log" \
            env CUDA_VISIBLE_DEVICES="" "${PYTHON_BIN}" -m domains.crime.publisher \
            --node-id "rumor_crime_${slug}" --host "${GPU_NODE_1}" --port "${rumor_port}" \
            --commune "${commune}" --config "${RUN_CONFIG}" \
            --seeds-file "${HOSTFILE}" --topics "${commune}" --subjective-only \
            --control-timeout "${CONTROL_TIMEOUT}" --listen-backlog "${LISTEN_BACKLOG}" \
            --runs-dir "${BASE_RUNS_DIR}" --run-id "${RUN_ID}"

        idx=$((idx + 1))
    done
fi

if [[ "${DOMAIN}" == "air" || "${DOMAIN}" == "both" ]]; then
    idx=0
    for commune in "${COMMUNES[@]}"; do
        slug="$(printf '%s' "${commune}" | tr '[:upper:] ' '[:lower:]_' | tr -cd '[:alnum:]_-')"
        main_port=$((9600 + idx * 2))
        rumor_port=$((main_port + 1))

        launch_step "air-${slug}" 1 "${GPU_NODE_2}" "${main_port}" \
            "${RUN_DIR}/logs/air_${slug}.log" \
            env CUDA_VISIBLE_DEVICES="" "${PYTHON_BIN}" -m domains.air.publisher \
            --node-id "pub_air_${slug}" --host "${GPU_NODE_2}" --port "${main_port}" \
            --commune "${commune}" --config "${RUN_CONFIG}" \
            --seeds-file "${HOSTFILE}" --topics "${commune}" --loop \
            --control-timeout "${CONTROL_TIMEOUT}" --listen-backlog "${LISTEN_BACKLOG}" \
            --runs-dir "${BASE_RUNS_DIR}" --run-id "${RUN_ID}"

        launch_step "air-rumor-${slug}" 1 "${GPU_NODE_2}" "${rumor_port}" \
            "${RUN_DIR}/logs/air_rumor_${slug}.log" \
            env CUDA_VISIBLE_DEVICES="" "${PYTHON_BIN}" -m domains.air.publisher \
            --node-id "rumor_air_${slug}" --host "${GPU_NODE_2}" --port "${rumor_port}" \
            --commune "${commune}" --config "${RUN_CONFIG}" \
            --seeds-file "${HOSTFILE}" --topics "${commune}" --subjective-only --loop \
            --control-timeout "${CONTROL_TIMEOUT}" --listen-backlog "${LISTEN_BACKLOG}" \
            --runs-dir "${BASE_RUNS_DIR}" --run-id "${RUN_ID}"

        idx=$((idx + 1))
    done
fi

# ------------------------------------------------------------------------------
# Frontend en segundo host GPU, también solo CPU del host.
# ------------------------------------------------------------------------------
launch_step "frontend" 1 "${GPU_NODE_2}" 8501 "${RUN_DIR}/logs/frontend.log" \
    env CUDA_VISIBLE_DEVICES="" CIVICMESH_RUNS="${BASE_RUNS_DIR}" \
    "${PYTHON_BIN}" -m streamlit run frontend/app.py \
    --server.port 8501 \
    --server.address 0.0.0.0 \
    --server.headless true

cat > "${RUN_DIR}/frontend.txt" <<EOF_FRONTEND
Frontend node: ${GPU_NODE_2}
Frontend port: 8501
SSH tunnel (ajustar login del DIINF):
ssh -L 8501:${GPU_NODE_2}:8501 <usuario>@<login-diinf>
EOF_FRONTEND

# ------------------------------------------------------------------------------
# Experimento de caída opcional automático.
# Ejemplo:
# CIVICMESH_FAILURE_AFTER=60 CIVICMESH_FAILURE_PEER=p1 sbatch run_civicmesh.sbatch
# ------------------------------------------------------------------------------
if [[ -n "${CIVICMESH_FAILURE_AFTER:-}" ]]; then
    (
        sleep "${CIVICMESH_FAILURE_AFTER}"
        "${PROJECT_ROOT}/scripts/slurm/partition_experiment.sh" \
            "${RUN_ID}" "${CIVICMESH_FAILURE_PEER:-p1}"
    ) > "${RUN_DIR}/logs/failure_controller.log" 2>&1 &
    CHILDREN+=("$!")
fi

cat <<EOF_READY
========================================================================
[SLURM] CivicMesh desplegado
RUN_ID:      ${RUN_ID}
CPU peers:   ${CPU_NODE_1}, ${CPU_NODE_2}
GPU hosts:   ${GPU_NODE_1}, ${GPU_NODE_2} (CPU only)
Config:      ${RUN_CONFIG}
Hostfile:    ${HOSTFILE}
Metrics:     ${RUN_DIR}/metrics
Logs:        ${RUN_DIR}/logs
Frontend:    http://${GPU_NODE_2}:8501

Caída manual de un peer:
  scripts/slurm/partition_experiment.sh ${RUN_ID} p1
========================================================================
EOF_READY

# Los procesos principales son de larga duración. Una caída intencional de un
# peer hará terminar un srun con código != 0, pero NO debe abortar toda la corrida.
wait || true
