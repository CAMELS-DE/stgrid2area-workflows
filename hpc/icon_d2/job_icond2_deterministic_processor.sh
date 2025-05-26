#!/bin/bash
#SBATCH --job-name=icond2_deterministic
#SBATCH --partition=cpu                 # Standard CPU nodes queue
#SBATCH --nodes=8                           # 70 maximum nodes available
#SBATCH --ntasks-per-node=70                # Can use up to 96 tasks per node
#SBATCH --cpus-per-task=1                   # Threads per MPI process
#SBATCH --time=48:00:00                     # Set the walltime (max: 72:00:00)
#SBATCH --mem=240000MB                       # Maximum memory per node in the cpu queue is 380GB
#SBATCH --output=icond2_deterministic%j.out # Standard output file.
#SBATCH --error=icond2_deterministic%j.err  # Standard error file.

module load mpi/openmpi/5.0-gnu-14.2

# Activate virtual environment
source /pfs/data6/home/ka/ka_iwu/ka_qt7760/venvs/s2a/bin/activate

# Start Dask MPI cluster
mpirun -np $SLURM_NTASKS \
  -x LD_LIBRARY_PATH \
  -x DASK_TEMPORARY_DIRECTORY=$TMPDIR \
  -x DASK_WORKER_SPACE=$TMPDIR \
  -x TMPDIR=$TMPDIR \
  -x DASK_LOGGING__DISTRIBUTED=warning \
  --mca coll_hcoll_enable 0 \
  --bind-to core --map-by core \
  python icond2_deterministic_processor.py