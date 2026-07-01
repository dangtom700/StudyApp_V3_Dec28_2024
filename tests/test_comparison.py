import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from config import ARTICLES_DIR
from db import get_connection
from ingest import ingest_pdf

from conftest import run_engine

FIXTURE_FILES = [
    "Bifurcation-analysis-for-a-COVID-19-cell-model-with-pa_2026_Alexandria-Engin.pdf",
    "Reduced-order-echo-state-networks-for-model-predictive_2026_European-Journal.pdf",
    "A-closed-loop-control-strategy-for-automotive-EHB-base_2026_Control-Engineer.pdf",
    "A-dual-coupled-hysteresis-model-and-hybrid-control-strategy-for-_2026_Measur.pdf",
    "A-fractional-order-mathematical-model-for-malaria-transmission-i_2026_Frankl.pdf",
    "A-frequency-interval-criterion-for-modeling-secondary_2026_Journal-of-Sound-.pdf",
    "A-hybrid-calibrated-improved-dynamic-wake-meandering-model-for_2026_Ocean-En.pdf",
    "A-hybrid-hierarchical-synergistic-control-framework-for-i_2026_Advanced-Engi.pdf",
]


def _make_eight_article_db(tmp_path, retrieval_engine_binary):
    db_path = tmp_path / "test.db"
    token_freq_dir = tmp_path / "token_freq"
    conn = get_connection(db_path)
    for filename in FIXTURE_FILES:
        ingest_pdf(ARTICLES_DIR / filename, conn, token_freq_dir=token_freq_dir)
    conn.close()
    run_engine(retrieval_engine_binary, ["--compute-relational-distance", str(db_path), "--persist-frequency-threshold", "3"])
    run_engine(retrieval_engine_binary, ["--compute-tfidf", str(db_path)])
    return db_path


def test_comparison_is_asymmetric_and_matches_known_scores(tmp_path, retrieval_engine_binary):
    db_path = _make_eight_article_db(tmp_path, retrieval_engine_binary)
    result = run_engine(retrieval_engine_binary,
                         ["--compute-comparison", str(db_path), "--comparison-score-threshold", "0.15"])
    assert result.returncode == 0, result.stderr

    conn = sqlite3.connect(db_path)
    forward = conn.execute(
        "SELECT distance FROM comparison WHERE source_id='10.1016/j.conengprac.2026.106951' "
        "AND target_id='10.1016/j.ejcon.2026.101527'"
    ).fetchone()[0]
    backward = conn.execute(
        "SELECT distance FROM comparison WHERE source_id='10.1016/j.ejcon.2026.101527' "
        "AND target_id='10.1016/j.conengprac.2026.106951'"
    ).fetchone()[0]
    assert forward == 0.6364606364488763
    assert backward == 0.6358402716507063
    assert forward != backward


def test_comparison_row_count_and_no_self_pairs(tmp_path, retrieval_engine_binary):
    db_path = _make_eight_article_db(tmp_path, retrieval_engine_binary)
    run_engine(retrieval_engine_binary, ["--compute-comparison", str(db_path), "--comparison-score-threshold", "0.15"])

    conn = sqlite3.connect(db_path)
    assert conn.execute("SELECT COUNT(*) FROM comparison").fetchone()[0] == 54
    assert conn.execute("SELECT COUNT(*) FROM comparison WHERE source_id = target_id").fetchone()[0] == 0


def test_comparison_incremental_skips_old_old_pairs(tmp_path, retrieval_engine_binary):
    db_path = tmp_path / "test.db"
    token_freq_dir = tmp_path / "token_freq"
    conn = get_connection(db_path)
    ingest_pdf(ARTICLES_DIR / FIXTURE_FILES[0], conn, token_freq_dir=token_freq_dir)
    ingest_pdf(ARTICLES_DIR / FIXTURE_FILES[1], conn, token_freq_dir=token_freq_dir)
    conn.close()

    run_engine(retrieval_engine_binary, ["--compute-relational-distance", str(db_path)])
    run_engine(retrieval_engine_binary, ["--compute-tfidf", str(db_path)])
    run_engine(retrieval_engine_binary,
               ["--compute-comparison", str(db_path), "--incremental", "--comparison-score-threshold", "0.0"])

    conn = sqlite3.connect(db_path)
    before = conn.execute(
        "SELECT distance FROM comparison WHERE source_id='10.1016/j.aej.2026.04.048' "
        "AND target_id='10.1016/j.ejcon.2026.101527'"
    ).fetchone()[0]
    conn.close()

    conn = get_connection(db_path)
    ingest_pdf(ARTICLES_DIR / FIXTURE_FILES[2], conn, token_freq_dir=token_freq_dir)
    ingest_pdf(ARTICLES_DIR / FIXTURE_FILES[3], conn, token_freq_dir=token_freq_dir)
    conn.close()

    run_engine(retrieval_engine_binary, ["--compute-relational-distance", str(db_path), "--incremental"])
    run_engine(retrieval_engine_binary, ["--compute-tfidf", str(db_path)])
    run_engine(retrieval_engine_binary,
               ["--compute-comparison", str(db_path), "--incremental", "--comparison-score-threshold", "0.0"])

    conn = sqlite3.connect(db_path)
    after = conn.execute(
        "SELECT distance FROM comparison WHERE source_id='10.1016/j.aej.2026.04.048' "
        "AND target_id='10.1016/j.ejcon.2026.101527'"
    ).fetchone()[0]
    new_pair_count = conn.execute(
        "SELECT COUNT(*) FROM comparison WHERE source_id = '10.1016/j.conengprac.2026.106951'"
    ).fetchone()[0]

    assert after == before
    assert new_pair_count > 0


def test_comparison_incremental_on_empty_db_equals_full(tmp_path, retrieval_engine_binary):
    db_path = _make_eight_article_db(tmp_path, retrieval_engine_binary)
    result = run_engine(retrieval_engine_binary,
                         ["--compute-comparison", str(db_path), "--incremental", "--comparison-score-threshold", "0.15"])
    assert result.returncode == 0, result.stderr

    conn = sqlite3.connect(db_path)
    assert conn.execute("SELECT COUNT(*) FROM comparison").fetchone()[0] == 54
    forward = conn.execute(
        "SELECT distance FROM comparison WHERE source_id='10.1016/j.conengprac.2026.106951' "
        "AND target_id='10.1016/j.ejcon.2026.101527'"
    ).fetchone()[0]
    assert forward == 0.6364606364488763
