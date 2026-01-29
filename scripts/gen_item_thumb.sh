#!/bin/bash 

#SBATCH --partition=hugemem
#SBATCH --time=24:00:00
#SBATCH --nodes=1

#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=256GB

#SBATCH --account=gfdl_a

#SBATCH -o /work/a3r/logs/item_thumb_log_%j.output
#SBATCH -e /work/a3r/logs/item_thumb_log_%j.error

module load miniforge
conda activate python_venv
python /home/a3r/Documents/code/stac-scripts/scripts/gen_item_thumb.py