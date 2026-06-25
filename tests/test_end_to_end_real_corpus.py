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


def test_full_pipeline_composes_correctly_on_real_corpus(tmp_path, retrieval_engine_binary):
    db_path = tmp_path / "studyapp.db"
    token_freq_dir = tmp_path / "token_freq"
    conn = get_connection(db_path)
    for filename in FIXTURE_FILES:
        ingest_pdf(ARTICLES_DIR / filename, conn, token_freq_dir=token_freq_dir)
    conn.close()

    r1 = run_engine(retrieval_engine_binary,
                     ["--compute-relational-distance", str(db_path), "--persist-frequency-threshold", "3"])
    assert r1.returncode == 0, r1.stderr
    r2 = run_engine(retrieval_engine_binary, ["--compute-tfidf", str(db_path)])
    assert r2.returncode == 0, r2.stderr
    r3 = run_engine(retrieval_engine_binary,
                     ["--compute-comparison", str(db_path), "--comparison-score-threshold", "0.15"])
    assert r3.returncode == 0, r3.stderr

    conn = sqlite3.connect(db_path)
    assert conn.execute("SELECT COUNT(DISTINCT file_id) FROM relation_distance_filtered").fetchone()[0] == 8
    assert conn.execute("SELECT COUNT(*) FROM relation_distance_filtered").fetchone()[0] == 3864
    assert conn.execute("SELECT COUNT(*) FROM tf_idf").fetchone()[0] == 1827
    assert conn.execute("SELECT COUNT(*) FROM comparison").fetchone()[0] == 54
    assert conn.execute("SELECT COUNT(*) FROM comparison WHERE source_id = target_id").fetchone()[0] == 0
