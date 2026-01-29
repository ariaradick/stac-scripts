#!/bin/bash

for i in $(seq 1 30); do
    sbatch /home/a3r/Documents/code/stac-scripts/scripts/gen_ens_thumb.sh $i
done