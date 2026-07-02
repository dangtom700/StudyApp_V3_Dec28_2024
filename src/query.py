"""Query the built index with a free-text prompt.

The prompt lives in a file (default: prompt.txt at the repo root) so you can
paste an essay-length query into your editor instead of typing it into a
terminal. This tokenizes the prompt the same way articles are tokenized, runs
the C++ engine's --process-prompt, and prints a ranked, readable result list.

Run via run.bat / run.sh, or directly:
    conda run -n StudyAssistant python src/query.py [prompt_file] [--top N] [--all]
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

from config import DB_PATH, REPO_ROOT
from db import get_connection
from modules.tokenize_prompt import tokenize_prompt

ENGINE_BINARY = REPO_ROOT / "build" / "retrieval_engine.exe"
UCRT64_BIN = r"C:\msys64\ucrt64\bin"
DEFAULT_PROMPT_PATH = REPO_ROOT / "PROMPT.txt"


def read_prompt(prompt_path: Path) -> str:
    """Return the prompt text, dropping blank/`#`-comment lines so the file can
    carry instructions at the top without polluting the query."""
    lines = prompt_path.read_text(encoding="utf-8").splitlines()
    kept = [line for line in lines if not line.lstrip().startswith("#")]
    return "\n".join(kept).strip()


def rank_prompt(prompt_text: str, db_path: Path = DB_PATH, top: int | None = None):
    """Tokenize `prompt_text`, run the engine, and return a list of
    (rank, file_id, score, file_name) tuples, best first."""
    freq = tokenize_prompt(prompt_text)
    if not freq:
        return []

    tmp = tempfile.NamedTemporaryFile(
        "w", suffix=".json", delete=False, encoding="utf-8"
    )
    try:
        json.dump(freq, tmp)
        tmp.close()
        raw = _run_engine(Path(tmp.name), db_path)
    finally:
        os.unlink(tmp.name)

    ranked: list[tuple[str, float]] = []
    for line in raw.splitlines():
        if not line.strip():
            continue
        file_id, score = line.split("\t")
        ranked.append((file_id, float(score)))
    if top is not None:
        ranked = ranked[:top]

    conn = get_connection(db_path)
    try:
        results = []
        for i, (file_id, score) in enumerate(ranked, start=1):
            row = conn.execute(
                "SELECT file_name FROM file_info WHERE file_id = ?", (file_id,)
            ).fetchone()
            results.append((i, file_id, score, row[0] if row else ""))
        return results
    finally:
        conn.close()


def _run_engine(query_path: Path, db_path: Path) -> str:
    env = dict(os.environ)
    env["PATH"] = UCRT64_BIN + os.pathsep + env.get("PATH", "")
    result = subprocess.run(
        [str(ENGINE_BINARY), "--process-prompt", str(db_path), str(query_path)],
        env=env,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"retrieval_engine failed: {result.stderr.strip()}")
    return result.stdout


def main() -> int:
    parser = argparse.ArgumentParser(description="Rank articles against a free-text prompt.")
    parser.add_argument(
        "prompt_file",
        nargs="?",
        default=str(DEFAULT_PROMPT_PATH),
        help="File containing the prompt text (default: prompt.txt at repo root).",
    )
    parser.add_argument("--top", type=int, default=20, help="Show only the top N results (default: 20).")
    parser.add_argument("--all", action="store_true", help="Show all matching articles, ignoring --top.")
    parser.add_argument("--db", default=str(DB_PATH), help="Path to the SQLite database.")
    args = parser.parse_args()

    prompt_path = Path(args.prompt_file)
    db_path = Path(args.db)

    if not ENGINE_BINARY.exists():
        print(f"error: {ENGINE_BINARY} not found -- run setup.bat / setup.sh first.", file=sys.stderr)
        return 1
    if not db_path.exists():
        print(f"error: {db_path} not found -- run setup.bat / setup.sh to build the index first.", file=sys.stderr)
        return 1
    if not prompt_path.exists():
        print(f"error: prompt file {prompt_path} not found.", file=sys.stderr)
        return 1

    prompt_text = read_prompt(prompt_path)
    if not prompt_text:
        print(f"Prompt is empty -- add your query text to {prompt_path}.", file=sys.stderr)
        return 1

    top = None if args.all else args.top
    results = rank_prompt(prompt_text, db_path=db_path, top=top)

    if not results:
        print("No matching articles (none of the prompt's terms appear in the corpus).")
        return 0

    print(f"Prompt: {prompt_path.name}\n")
    print(f"{'Rank':>4}  {'Score':>7}  Article")
    print(f"{'-' * 4}  {'-' * 7}  {'-' * 48}")
    for rank, file_id, score, file_name in results:
        label = file_name or file_id
        print(f"{rank:>4}  {score:>7.4f}  {label}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
