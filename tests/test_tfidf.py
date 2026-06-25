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
]


def _make_two_article_db_with_distances(tmp_path, retrieval_engine_binary):
    db_path = tmp_path / "test.db"
    token_freq_dir = tmp_path / "token_freq"
    conn = get_connection(db_path)
    for filename in FIXTURE_FILES:
        ingest_pdf(ARTICLES_DIR / filename, conn, token_freq_dir=token_freq_dir)
    conn.close()
    run_engine(retrieval_engine_binary, ["--compute-relational-distance", str(db_path)])
    return db_path


def test_tfidf_known_word_value(tmp_path, retrieval_engine_binary):
    db_path = _make_two_article_db_with_distances(tmp_path, retrieval_engine_binary)
    result = run_engine(retrieval_engine_binary, ["--compute-tfidf", str(db_path)])
    assert result.returncode == 0, result.stderr

    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT freq, doc_count, tf_idf FROM tf_idf WHERE word = 'covid'").fetchone()
    # tf_idf depends on corpus-wide totals (sum_freq, total_docs), so this
    # value is specific to this test's 2-article fixture -- not the
    # 8-article value documented in the plan's Ground-truth fixtures.
    assert row == (3, 1, 0.0003475446983025063)


def test_tfidf_rerun_does_not_duplicate(tmp_path, retrieval_engine_binary):
    db_path = _make_two_article_db_with_distances(tmp_path, retrieval_engine_binary)
    run_engine(retrieval_engine_binary, ["--compute-tfidf", str(db_path)])
    run_engine(retrieval_engine_binary, ["--compute-tfidf", str(db_path)])

    conn = sqlite3.connect(db_path)
    count = conn.execute("SELECT COUNT(*) FROM tf_idf WHERE word = 'covid'").fetchone()[0]
    assert count == 1
