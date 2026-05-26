#!/bin/bash

for i in $(seq 1 30); do
    # sbatch /work/a3r/Documents/code/stac-scripts/scripts/gen_ens_thumb.sh $i
    sbatch /work/a3r/Documents/code/stac-scripts/scripts/gen_item_thumb.sh $i
done