#!/usr/bin/env bash
set -euo pipefail

REPORT_DIR="${1:-/Users/mohammad/Documents/GitHub/QDArchive-report}"
SRC="src/phase2_classification/data_analysis/output/by_repository"

rsync -a --include='*/' --include='*_primary_class_histogram*.svg' --exclude='*' \
  "$SRC/" "$REPORT_DIR/res/by_repository/"

uv run src/phase2_classification/data_analysis/generate_latex_report.py \
  --tables-root "$SRC" \
  --res-dir res/by_repository \
  --output "$REPORT_DIR/sections/classification_results.tex"

echo "Synced histogram SVGs and regenerated classification_results.tex into $REPORT_DIR"
