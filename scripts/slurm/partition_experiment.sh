#!/bin/bash

set -euo pipefail

RUN_ID="${1:-${SLURM_JOB_ID:-}}"
TARGET_PEER="${2:-p1}"
BASE_RUNS_DIR="${CIVICMESH_RUNS:-$HOME/civicmesh_runs}"
RUN_DIR="${BASE_RUNS_DIR}/${RUN_ID}"
HOSTFILE="${RUN_DIR}/hostfile.txt"
ALLOCATION_FILE="${RUN_DIR}/allocation.txt"
EXPERIMENT_LOG="${RUN_DIR}/logs/experiment.log"
CONTROL_METRICS="${RUN_DIR}/metrics/control_events.jsonl"
OBSERVE_SECONDS="${CIVICMESH_FAILURE_OBSERVE_SECONDS:-12}"

if [[ -z "${RUN_ID}" ]]; then
    echo "Uso: $0 <RUN_ID|SLURM_JOB_ID> [PEER_ID]" >&2
    exit 1
fi

if [[ ! -f "${HOSTFILE}" || ! -f "${ALLOCATION_FILE}" ]]; then
    echo "[ERROR] No se encontró hostfile.txt/allocation.txt en ${RUN_DIR}" >&2
    exit 1
fi

TARGET_HOST="$(awk -v peer="${TARGET_PEER}" '$1 == peer {print $2; exit}' "${HOSTFILE}")"
CPU_JOB_ID="$(awk -F= '$1 == "cpu_job_id" {print $2; exit}' "${ALLOCATION_FILE}")"

if [[ -z "${TARGET_HOST}" ]]; then
    echo "[ERROR] Peer ${TARGET_PEER} no aparece en ${HOSTFILE}" >&2
    exit 1
fi

if [[ -z "${CPU_JOB_ID}" ]]; then
    echo "[ERROR] No se encontró cpu_job_id en ${ALLOCATION_FILE}" >&2
    exit 1
fi

mkdir -p "${RUN_DIR}/logs" "${RUN_DIR}/metrics"

log() {
    local line="[$(date -Iseconds)] $*"
    echo "${line}" | tee -a "${EXPERIMENT_LOG}"
}

record_control_event() {
    local phase="$1"
    local method="$2"
    python - "${CONTROL_METRICS}" "${RUN_ID}" "${TARGET_PEER}" "${TARGET_HOST}" "${phase}" "${method}" <<'PY'
import json
import sys
import time

path, run_id, peer, host, phase, method = sys.argv[1:]
record = {
    "timestamp": time.time(),
    "event": "failure_injection",
    "run_id": run_id,
    "peer": peer,
    "host": host,
    "phase": phase,
    "method": method,
}
with open(path, "a", encoding="utf-8") as fh:
    fh.write(json.dumps(record, separators=(",", ":")) + "\n")
PY
}

remote_peer_is_alive() {
    local pattern="[n]etwork.peer --node-id ${TARGET_PEER}"
    srun \
        --jobid="${CPU_JOB_ID}" \
        --overlap \
        --nodes=1 \
        --ntasks=1 \
        --nodelist="${TARGET_HOST}" \
        bash -lc "pgrep -f '${pattern}' >/dev/null"
}

remote_kill_peer() {
    local signal="$1"
    local pattern="[n]etwork.peer --node-id ${TARGET_PEER}"
    srun \
        --jobid="${CPU_JOB_ID}" \
        --overlap \
        --nodes=1 \
        --ntasks=1 \
        --nodelist="${TARGET_HOST}" \
        bash -lc "pkill -${signal} -f '${pattern}' || true"
}

log "Iniciando caída de ${TARGET_PEER} en ${TARGET_HOST} (run=${RUN_ID}, cpu_job=${CPU_JOB_ID})"
record_control_event "requested" "pending"

# Primero intentamos cancelar solamente el job step asociado al peer.
STEP_ID="$(
    squeue --steps -h -j "${CPU_JOB_ID}" -o '%i|%j|%N' 2>/dev/null \
    | awk -F'|' -v name="peer-${TARGET_PEER}" '$2 == name {print $1; exit}'
)"

METHOD=""
if [[ -n "${STEP_ID}" ]]; then
    log "Cancelando step ${STEP_ID} (peer-${TARGET_PEER})"
    if scancel "${STEP_ID}"; then
        METHOD="scancel-step"
    fi
fi

sleep 2

# Fallback/garantía: matar el proceso en el host CPU exacto dentro de la
# allocation del componente CPU. Para una invocación externa no usamos
# --het-group: usamos directamente el job ID del componente CPU.
if remote_peer_is_alive; then
    log "El peer sigue vivo; enviando SIGTERM remotamente"
    remote_kill_peer TERM
    METHOD="${METHOD:-remote-pkill-term}"
    sleep 2
fi

if remote_peer_is_alive; then
    log "El peer sigue vivo tras SIGTERM; escalando a SIGKILL"
    remote_kill_peer KILL
    METHOD="remote-pkill-kill"
    sleep 1
fi

if remote_peer_is_alive; then
    log "ERROR: ${TARGET_PEER} sigue vivo después de la inyección de fallo"
    record_control_event "kill_failed" "${METHOD:-unknown}"
    exit 1
fi

record_control_event "killed" "${METHOD:-remote-pkill}"
log "${TARGET_PEER} detenido. Esperando ${OBSERVE_SECONDS}s para observar suspect/failed..."
sleep "${OBSERVE_SECONDS}"

EVIDENCE_FILE="${RUN_DIR}/logs/failure_evidence_${TARGET_PEER}.log"
{
    echo "=== CivicMesh failure experiment ==="
    echo "run_id=${RUN_ID}"
    echo "cpu_component_job_id=${CPU_JOB_ID}"
    echo "target_peer=${TARGET_PEER}"
    echo "target_host=${TARGET_HOST}"
    echo "method=${METHOD:-remote-pkill}"
    echo "observe_seconds=${OBSERVE_SECONDS}"
    echo
    echo "=== Gossip/failure detector evidence ==="
    grep -H -E "changes=.*${TARGET_PEER}|failed.*${TARGET_PEER}|suspect.*${TARGET_PEER}" \
        "${RUN_DIR}"/logs/peer_*.log 2>/dev/null || true
} > "${EVIDENCE_FILE}"

record_control_event "observation_complete" "${METHOD:-remote-pkill}"
log "Observación terminada. Evidencia: ${EVIDENCE_FILE}"
