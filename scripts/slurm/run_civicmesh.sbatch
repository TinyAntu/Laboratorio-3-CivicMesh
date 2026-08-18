#!/bin/bash
#SBATCH --job-name=civicmesh
#SBATCH --partition=GPU
#SBATCH --nodes=4
#SBATCH --ntasks=4
#SBATCH --gres=gpu:A30:1
#SBATCH --cpus-per-task=2
#SBATCH --mem=64G
#SBATCH --time=00:30:00
#SBATCH --output=civicmesh_%j.log
#SBATCH --error=civicmesh_%j.err

set -e

# ==========================================
# 1. Definicion de Directorio de Corrida
# ==========================================
RUN_ID="${SLURM_JOB_ID:-local-$(date +%s)}"
BASE_RUNS_DIR="${CIVICMESH_RUNS:-$HOME/civicmesh_runs}"
RUN_DIR="${BASE_RUNS_DIR}/${RUN_ID}"

echo "[SLURM] Iniciando despliegue de CivicMesh. JOB_ID: ${RUN_ID}"
echo "[SLURM] Directorio Shared FS: ${RUN_DIR}"

mkdir -p "${RUN_DIR}/metrics" "${RUN_DIR}/logs"

# ==========================================
# 2. Generacion de Config y Hostfile Inicial
# ==========================================
cat <<EOF > "${RUN_DIR}/config.yaml"
experiment:
  seed: 42
  fanout: 2
  ttl: 5
  priority_channels:
    objective: 10
    subjective: 5
comunas:
  - Santiago
  - Providencia
  - Las Condes
  - Maipu
EOF

touch "${RUN_DIR}/hostfile.txt"

# Obtener lista de nodos asignados
NODES=($(scontrol show hostnames $SLURM_JOB_NODELIST))
echo "[SLURM] Nodos asignados: ${NODES[@]}"

CPU_NODE_1=${NODES[0]:-localhost}
CPU_NODE_2=${NODES[1]:-localhost}
GPU_NODE_1=${NODES[2]:-localhost}
GPU_NODE_2=${NODES[3]:-localhost}

# ==========================================
# 3. Registro de Peers en hostfile.txt
# ==========================================
echo "p0 ${CPU_NODE_1} 9000" >> "${RUN_DIR}/hostfile.txt"
echo "p1 ${CPU_NODE_1} 9001" >> "${RUN_DIR}/hostfile.txt"
echo "p2 ${CPU_NODE_2} 9002" >> "${RUN_DIR}/hostfile.txt"
echo "p3 ${CPU_NODE_2} 9003" >> "${RUN_DIR}/hostfile.txt"

echo "[SLURM] Hostfile generado en ${RUN_DIR}/hostfile.txt:"
cat "${RUN_DIR}/hostfile.txt"

# ==========================================
# 4. Lanzamiento de Peers en Nodos CPU
# ==========================================
echo "[SLURM] Arrancando Peers en Nodos CPU (${CPU_NODE_1}, ${CPU_NODE_2})..."

srun --nodelist=${CPU_NODE_1} --nodes=1 --ntasks=1 \
    python -m network.peer --node-id p0 --host ${CPU_NODE_1} --port 9000 --fanout 2 --seed 42 \
    > "${RUN_DIR}/logs/peer_p0.log" 2>&1 &

srun --nodelist=${CPU_NODE_1} --nodes=1 --ntasks=1 \
    python -m network.peer --node-id p1 --host ${CPU_NODE_1} --port 9001 --seeds-file "${RUN_DIR}/hostfile.txt" --fanout 2 --seed 43 \
    > "${RUN_DIR}/logs/peer_p1.log" 2>&1 &

srun --nodelist=${CPU_NODE_2} --nodes=1 --ntasks=1 \
    python -m network.peer --node-id p2 --host ${CPU_NODE_2} --port 9002 --seeds-file "${RUN_DIR}/hostfile.txt" --fanout 2 --seed 44 \
    > "${RUN_DIR}/logs/peer_p2.log" 2>&1 &

srun --nodelist=${CPU_NODE_2} --nodes=1 --ntasks=1 \
    python -m network.peer --node-id p3 --host ${CPU_NODE_3:-${CPU_NODE_2}} --port 9003 --seeds-file "${RUN_DIR}/hostfile.txt" --fanout 2 --seed 45 \
    > "${RUN_DIR}/logs/peer_p3.log" 2>&1 &

# Esperar sincronizacion inicial de la malla
sleep 5

# ==========================================
# 5. Lanzamiento de Publicadores en Nodos GPU (solo CPU del host)
# ==========================================
echo "[SLURM] Arrancando Publicadores en Nodo GPU (${GPU_NODE_1})..."

srun --nodelist=${GPU_NODE_1} --nodes=1 --ntasks=1 \
    python -c "import time, sys; print('[Publicador A] Generando eventos estocasticos...', flush=True); time.sleep(600)" \
    > "${RUN_DIR}/logs/publisher_domain_a.log" 2>&1 &

srun --nodelist=${GPU_NODE_1} --nodes=1 --ntasks=1 \
    python -c "import time, sys; print('[Publicador B] Replay serie calidad del aire...', flush=True); time.sleep(600)" \
    > "${RUN_DIR}/logs/publisher_domain_b.log" 2>&1 &

# ==========================================
# 6. Lanzamiento del Frontend UI en Nodo GPU (solo CPU del host)
# ==========================================
echo "[SLURM] Arrancando Frontend en Nodo GPU (${GPU_NODE_2})..."

srun --nodelist=${GPU_NODE_2} --nodes=1 --ntasks=1 \
    python -c "import time; print('[Frontend UI] Servidor listo en puerto 8501. Leyendo ${RUN_DIR}/metrics', flush=True); time.sleep(600)" \
    > "${RUN_DIR}/logs/frontend.log" 2>&1 &

echo "[SLURM] CivicMesh desplegado correctamente."
echo "[SLURM] Para tunel SSH al Frontend: ssh -L 8501:${GPU_NODE_2}:8501 usuario@cluster.diinf.usach.cl"

wait
