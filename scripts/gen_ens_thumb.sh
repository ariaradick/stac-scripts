#!/bin/bash 

#SBATCH --partition=analysis
#SBATCH --time=48:00:00
#SBATCH --nodes=1

#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=128GB
#SBATCH --constraint=avx2

#SBATCH --account=gfdl_a

#SBATCH -o /work/a3r/logs/ens_thumb/job_log_%j.out
#SBATCH -e /work/a3r/logs/ens_thumb/job_log_%j.err

module load miniforge
# conda activate python_venv
# python /work/a3r/Documents/code/stac-scripts/scripts/generate_ens_thumbnails.py $1

conda activate aria
python /work/a3r/Documents/code/stac-scripts/scripts/gen_ens_thumb.py $1