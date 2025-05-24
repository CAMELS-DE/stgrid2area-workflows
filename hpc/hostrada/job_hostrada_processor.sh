#!/bin/bash
#SBATCH --job-name=hostrada
#SBATCH --partition=multiple              # e.g. multi-node queue
#SBATCH --nodes=20                        # 80 maximum number of nodes and tasks per node
#SBATCH --ntasks-per-node=40              # Request 40 tasks per node.
#SBATCH --cpus-per-task=1                 # Threads per MPI process
#SBATCH --time=10:00:00                   # Set the walltime.
#SBATCH --mem=90000MB                     # 124800MB Total memory per node (64 x 1950MB).
#SBATCH --output=hostrada%j.out           # Standard output file.
#SBATCH --error=hostrada%j.err            # Standard error file.

module load compiler/gnu/13.3 
module load mpi/openmpi/5.0
module load devel/cuda/12.2 # prevent mpi4py errors

# Activate virtual environment
source /home/kit/iwu/qt7760/venvs/s2a/bin/activate

# Start Dask MPI cluster, set variable to process with -x HOSTRADA_VARIABLE
mpirun -np $SLURM_NTASKS -x HOSTRADA_VARIABLE=all -x LD_LIBRARY_PATH -x DASK_TEMPORARY_DIRECTORY=$TMPDIR -x DASK_LOGGING__DISTRIBUTED=warning --mca coll_hcoll_enable 0 --bind-to core --map-by core /home/kit/iwu/qt7760/venvs/s2a/bin/python mpiprocessor_hostrada.py