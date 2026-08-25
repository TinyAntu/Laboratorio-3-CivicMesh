#!/bin/bash
#SBATCH --job-name=civicmesh-het-smoke
#SBATCH --partition=batch
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G
#SBATCH --time=00:05:00
#SBATCH --output=het-smoke-%j.out
#SBATCH --error=het-smoke-%j.err

#SBATCH hetjob

#SBATCH --partition=GPU
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G
#SBATCH --time=00:05:00

set -euo pipefail

echo "===== HETEROGENEOUS JOB ====="
echo "SLURM_JOB_ID=${SLURM_JOB_ID}"
echo "SLURM_HET_SIZE=${SLURM_HET_SIZE:-NO_DEFINIDO}"
echo "CPU_JOB_ID=${SLURM_JOB_ID_HET_GROUP_0:-NO_DEFINIDO}"
echo "GPU_JOB_ID=${SLURM_JOB_ID_HET_GROUP_1:-NO_DEFINIDO}"
echo "CPU_NODELIST=${SLURM_JOB_NODELIST_HET_GROUP_0:-NO_DEFINIDO}"
echo "GPU_NODELIST=${SLURM_JOB_NODELIST_HET_GROUP_1:-NO_DEFINIDO}"

echo
echo "===== CPU / batch ====="
srun --het-group=0 --nodes=2 --ntasks=2 --ntasks-per-node=1 hostname

echo
echo "===== GPU partition (CPU only; no GRES requested) ====="
srun --het-group=1 --nodes=2 --ntasks=2 --ntasks-per-node=1 \
    env CUDA_VISIBLE_DEVICES="" bash -lc 'echo "$(hostname) CUDA_VISIBLE_DEVICES=${CUDA_VISIBLE_DEVICES}"'

echo
echo "===== OK ====="
