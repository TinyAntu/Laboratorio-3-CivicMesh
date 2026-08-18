#!/bin/bash
# ==============================================================================
# Script de Simulacion de Caida de Peers / Particion de Red en Slurm
# ==============================================================================

RUN_ID="${1:-$SLURM_JOB_ID}"

if [ -z "$RUN_ID" ]; then
    echo "Uso: $0 <RUN_ID|SLURM_JOB_ID>"
    echo "Ejemplo: $0 12345"
    exit 1
fi

BASE_RUNS_DIR="${CIVICMESH_RUNS:-$HOME/civicmesh_runs}"
RUN_DIR="${BASE_RUNS_DIR}/${RUN_ID}"

echo "========================================================================"
echo "[EXPERIMENTO] Iniciando prueba de tolerancia a fallos para RUN_ID: ${RUN_ID}"
echo "========================================================================"

if [ ! -d "${RUN_DIR}" ]; then
    echo "[ERROR] El directorio de la corrida ${RUN_DIR} no existe."
    exit 1
fi

# Registrar timestamp del evento de caida
TIMESTAMP=$(date +"%Y-%m-%dT%H:%M:%S")
echo "[${TIMESTAMP}] Simulando caida del Peer p1..." >> "${RUN_DIR}/logs/experiment.log"

# Buscar y terminar el proceso peer p1 en la maquina
TARGET_PID=$(pgrep -f "network.peer --node-id p1" || true)

if [ -n "$TARGET_PID" ]; then
    echo "[EXPERIMENTO] Terminando proceso Peer p1 (PID: ${TARGET_PID})..."
    kill -9 $TARGET_PID
    echo "[EXPERIMENTO] Peer p1 eliminado exitosamente."
else
    echo "[AVISO] No se encontro proceso p1 localmente. Si se ejecuta en Slurm, scancel step o kill remoto..."
    # Intentar scancel de step si aplica o kill general
    pkill -f "network.peer --node-id p1" || true
fi

echo "[EXPERIMENTO] Observando reaccion del detector de fallos y gossip (5 segundos)..."
sleep 5

echo "[EXPERIMENTO] Evento registrado en ${RUN_DIR}/logs/experiment.log"
echo "========================================================================"
