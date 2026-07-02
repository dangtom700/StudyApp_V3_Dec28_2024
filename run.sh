#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

# Query the corpus with your prompt.
#   1. Setup the program once:   ./setup.sh   (builds the index)
#   2. Set the prompt:           edit PROMPT.txt and paste your query (essay-length is fine)
#   3. Query the result:         ./run.sh     (add --top N or --all to change how many show)

conda run -n StudyAssistant python src/query.py "$@"
