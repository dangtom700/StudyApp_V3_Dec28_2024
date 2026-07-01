#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

echo "Building C++ retrieval engine..."
cmake -B build -DCMAKE_PREFIX_PATH=C:/msys64/ucrt64 -G "MinGW Makefiles" .
cmake --build build

echo "Setting up NLTK corpora..."
conda run -n StudyAssistant python src/setup_nltk.py

echo "Running StudyApp V3 build pipeline..."
conda run -n StudyAssistant python src/pipeline.py "$@"

echo "Done."
