#!/bin/bash 

#SBATCH --partition=analysis
#SBATCH --time=72:00:00
#SBATCH --nodes=1

#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=128GB
#SBATCH --constraint=avx2

#SBATCH --account=gfdl_a

#SBATCH -o /work/a3r/logs/item_thumb/item_thumb_log_%j.output
#SBATCH -e /work/a3r/logs/item_thumb/item_thumb_log_%j.error

module load miniforge
conda activate aria
python /work/a3r/Documents/code/stac-scripts/scripts/gen_item_thumb.py $1