#!/bin/bash 

#SBATCH --partition=analysis
#SBATCH --time=24:00:00
#SBATCH --nodes=1

#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=32GB

#SBATCH --account=gfdl_a

#SBATCH -o /work/a3r/logs/job_log_%j.output
#SBATCH -e /work/a3r/logs/job_log_%j.error

module load miniforge
conda activate python_venv
python /home/a3r/Documents/code/stac-scripts/scripts/generate_ens_thumbnails.py $1