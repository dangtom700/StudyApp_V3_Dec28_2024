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


def _make_two_article_db(tmp_path):
    db_path = tmp_path / "test.db"
    token_freq_dir = tmp_path / "token_freq"
    conn = get_connection(db_path)
    for filename in FIXTURE_FILES:
        ingest_pdf(ARTICLES_DIR / filename, conn, token_freq_dir=token_freq_dir)
    conn.close()
    return db_path


def test_relational_distance_produces_expected_row_count(tmp_path, retrieval_engine_binary):
    db_path = _make_two_article_db(tmp_path)
    result = run_engine(retrieval_engine_binary,
                         ["--compute-relational-distance", str(db_path), "--persist-frequency-threshold", "3"])
    assert result.returncode == 0, result.stderr

    conn = sqlite3.connect(db_path)
    counts = dict(conn.execute(
        "SELECT file_id, COUNT(*) FROM relation_distance_filtered GROUP BY file_id"
    ).fetchall())
    assert counts == {
        "10.1016/j.aej.2026.04.048": 407,
        "10.1016/j.ejcon.2026.101527": 583,
    }


def test_relational_distance_weight_matches_known_value(tmp_path, retrieval_engine_binary):
    db_path = _make_two_article_db(tmp_path)
    run_engine(retrieval_engine_binary,
               ["--compute-relational-distance", str(db_path), "--persist-frequency-threshold", "3"])

    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "SELECT frequency, weight FROM relation_distance_filtered "
        "WHERE file_id = '10.1016/j.aej.2026.04.048' AND token = 'model'"
    ).fetchone()
    assert row == (150, 0.4736954953733674)


def test_relational_distance_is_full_rebuild_not_incremental(tmp_path, retrieval_engine_binary):
    db_path = _make_two_article_db(tmp_path)
    run_engine(retrieval_engine_binary, ["--compute-relational-distance", str(db_path)])
    run_engine(retrieval_engine_binary, ["--compute-relational-distance", str(db_path)])

    conn = sqlite3.connect(db_path)
    count = conn.execute(
        "SELECT COUNT(*) FROM relation_distance_filtered WHERE file_id = '10.1016/j.aej.2026.04.048' AND token = 'model'"
    ).fetchone()[0]
    assert count == 1


def test_relational_distance_incremental_skips_existing_files(tmp_path, retrieval_engine_binary):
    db_path = _make_two_article_db(tmp_path)
    run_engine(retrieval_engine_binary, ["--compute-relational-distance", str(db_path)])

    conn = sqlite3.connect(db_path)
    before = conn.execute(
        "SELECT frequency, weight FROM relation_distance_filtered "
        "WHERE file_id = '10.1016/j.aej.2026.04.048' AND token = 'model'"
    ).fetchone()
    conn.close()

    token_freq_dir = tmp_path / "token_freq"
    conn = get_connection(db_path)
    ingest_pdf(
        ARTICLES_DIR / "A-closed-loop-control-strategy-for-automotive-EHB-base_2026_Control-Engineer.pdf",
        conn,
        token_freq_dir=token_freq_dir,
    )
    conn.close()

    run_engine(retrieval_engine_binary, ["--compute-relational-distance", str(db_path), "--incremental"])

    conn = sqlite3.connect(db_path)
    after = conn.execute(
        "SELECT frequency, weight FROM relation_distance_filtered "
        "WHERE file_id = '10.1016/j.aej.2026.04.048' AND token = 'model'"
    ).fetchone()
    new_file_count = conn.execute(
        "SELECT COUNT(*) FROM relation_distance_filtered WHERE file_id = '10.1016/j.conengprac.2026.106951'"
    ).fetchone()[0]

    assert after == before
    assert new_file_count > 0
