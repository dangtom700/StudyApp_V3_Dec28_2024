import argparse
import os
import subprocess
import time
from pathlib import Path

from config import ARTICLES_DIR, DATAMUSE_CACHE_PATH, DB_PATH, REPO_ROOT
from db import get_connection
from ingest import ingest_folder
from tag_topics import tag_all_articles

ENGINE_BINARY = REPO_ROOT / "build" / "retrieval_engine.exe"
UCRT64_BIN = r"C:\msys64\ucrt64\bin"

TOTAL_STAGES = 5


def _run_engine(args: list[str]) -> None:
    env = dict(os.environ)
    env["PATH"] = UCRT64_BIN + os.pathsep + env.get("PATH", "")
    result = subprocess.run([str(ENGINE_BINARY), *args], env=env)
    if result.returncode != 0:
        raise RuntimeError(f"retrieval_engine {args[0]} failed with exit code {result.returncode}")


def _stage(n: int, title: str) -> float:
    print(f"\n[{n}/{TOTAL_STAGES}] {title}...", flush=True)
    return time.perf_counter()


def _done(started: float) -> None:
    print(f"      done in {time.perf_counter() - started:.1f}s", flush=True)


def build(
    folder: Path = ARTICLES_DIR,
    db_path: Path = DB_PATH,
    full: bool = False,
    cache_path: Path = DATAMUSE_CACHE_PATH,
) -> None:
    if not ENGINE_BINARY.exists():
        raise FileNotFoundError(f"{ENGINE_BINARY} not found -- build it first (see run.bat/run.sh)")

    print("StudyApp V3 build pipeline", flush=True)
    print(f"  Mode:     {'full rebuild' if full else 'incremental'}", flush=True)
    print(f"  Articles: {folder}", flush=True)
    print(f"  Database: {db_path}", flush=True)

    conn = get_connection(db_path)
    try:
        started = _stage(1, f"Ingesting PDFs from {folder}")
        file_ids = ingest_folder(folder, conn)
        print(f"  {len(file_ids)} articles in corpus", flush=True)
        _done(started)

        started = _stage(2, "Tagging topics (Datamuse expansion)")
        tagged = tag_all_articles(conn, cache_path=cache_path)
        print(f"  {tagged} newly tagged", flush=True)
        _done(started)
    finally:
        conn.close()

    distance_args = ["--compute-relational-distance", str(db_path)]
    comparison_args = ["--compute-comparison", str(db_path)]
    if not full:
        distance_args.append("--incremental")
        comparison_args.append("--incremental")

    started = _stage(3, "Computing relational distance")
    _run_engine(distance_args)
    _done(started)

    started = _stage(4, "Computing TF-IDF")
    _run_engine(["--compute-tfidf", str(db_path)])
    _done(started)

    started = _stage(5, "Computing article comparisons")
    _run_engine(comparison_args)
    _done(started)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the StudyApp V3 ingest + retrieval build pipeline.")
    parser.add_argument("--full", action="store_true", help="Force a full rebuild instead of an incremental one.")
    args = parser.parse_args()
    build_started = time.perf_counter()
    build(full=args.full)
    print(f"\nBuild complete in {time.perf_counter() - build_started:.1f}s -> {DB_PATH}")
