import argparse
import os
import subprocess
from pathlib import Path

from config import ARTICLES_DIR, DB_PATH, REPO_ROOT
from db import get_connection
from ingest import ingest_folder
from tag_topics import tag_all_articles

ENGINE_BINARY = REPO_ROOT / "build" / "retrieval_engine.exe"
UCRT64_BIN = r"C:\msys64\ucrt64\bin"


def _run_engine(args: list[str]) -> None:
    env = dict(os.environ)
    env["PATH"] = UCRT64_BIN + os.pathsep + env.get("PATH", "")
    result = subprocess.run([str(ENGINE_BINARY), *args], env=env)
    if result.returncode != 0:
        raise RuntimeError(f"retrieval_engine {args[0]} failed with exit code {result.returncode}")


def build(folder: Path = ARTICLES_DIR, db_path: Path = DB_PATH, full: bool = False) -> None:
    if not ENGINE_BINARY.exists():
        raise FileNotFoundError(f"{ENGINE_BINARY} not found -- build it first (see run.bat/run.sh)")

    conn = get_connection(db_path)
    try:
        ingest_folder(folder, conn)
        tag_all_articles(conn)
    finally:
        conn.close()

    distance_args = ["--compute-relational-distance", str(db_path)]
    comparison_args = ["--compute-comparison", str(db_path)]
    if not full:
        distance_args.append("--incremental")
        comparison_args.append("--incremental")

    _run_engine(distance_args)
    _run_engine(["--compute-tfidf", str(db_path)])
    _run_engine(comparison_args)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the StudyApp V3 ingest + retrieval build pipeline.")
    parser.add_argument("--full", action="store_true", help="Force a full rebuild instead of an incremental one.")
    args = parser.parse_args()
    build(full=args.full)
    print(f"Build complete: {DB_PATH}")
