#!/usr/bin/env bash
# run_tiingo_daily.sh

# Exit on error
set -e

# OPTIONAL: change to the directory where this script lives
cd "$(dirname "$0")"

# ---- Conda setup ----
# Adjust this path to where your conda/mamba is installed.
# Common locations:
#   ~/miniconda3/etc/profile.d/conda.sh
#   ~/anaconda3/etc/profile.d/conda.sh
#   ~/mambaforge/etc/profile.d/conda.sh

CONDA_SH="$HOME/miniconda3/etc/profile.d/conda.sh"

if [ -f "$CONDA_SH" ]; then
    # Load conda into this non-interactive shell
    . "$CONDA_SH"
else
    echo "Could not find conda.sh at: $CONDA_SH"
    echo "Edit run_tiingo_daily.sh and fix the CONDA_SH path."
    exit 1
fi

# ---- Activate environment ----
conda activate quant_env

# ---- Run Tiingo maintenance pipeline ----
python /home/rthomas/thomasr/quant_lab/data_pipelines/tiingo_maintenance_pipeline.py

