#!/usr/bin/env bash
# Turn report_labN.md into report_labN.pdf in the course's common format (A4, 11 pt, 2 cm margins).
# Needs Docker. The first run downloads the pandoc image (about 370 MB).
#   usage: bash make_pdf.sh lab1
set -euo pipefail
cd "$(dirname "$0")"
lab=${1:?usage: bash make_pdf.sh labN   (for example: bash make_pdf.sh lab1)}
[ -f "report_$lab.md" ] || { echo "report_$lab.md not found in $(pwd)"; exit 1; }
docker run --rm -v "$PWD":/data -u "$(id -u):$(id -g)" pandoc/typst:3.6 \
  --defaults report.yaml "report_$lab.md" -o "report_$lab.pdf"
echo "wrote $(pwd)/report_$lab.pdf"
