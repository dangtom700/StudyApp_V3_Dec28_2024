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


def test_incremental_build_equals_full_rebuild_for_new_pairs(tmp_path, retrieval_engine_binary, monkeypatch):
    import pipeline as pipeline_module
    monkeypatch.setattr(pipeline_module, "ENGINE_BINARY", retrieval_engine_binary)

    folder = tmp_path / "corpus"
    folder.mkdir()
    for filename in FIXTURE_FILES[:6]:
        (folder / filename).write_bytes((ARTICLES_DIR / filename).read_bytes())

    incremental_db = tmp_path / "incremental.db"
    pipeline_module.build(folder=folder, db_path=incremental_db, full=True, cache_path=tmp_path / "cache.json")

    conn = sqlite3.connect(incremental_db)
    old_pairs_before = dict(
        conn.execute("SELECT source_id || '|' || target_id, distance FROM comparison").fetchall()
    )
    conn.close()
    assert len(old_pairs_before) > 0

    for filename in FIXTURE_FILES[6:]:
        (folder / filename).write_bytes((ARTICLES_DIR / filename).read_bytes())
    pipeline_module.build(folder=folder, db_path=incremental_db, full=False, cache_path=tmp_path / "cache.json")

    conn = sqlite3.connect(incremental_db)
    placeholders = ",".join("?" * len(old_pairs_before))
    old_pairs_after = dict(
        conn.execute(
            f"SELECT source_id || '|' || target_id, distance FROM comparison "
            f"WHERE source_id || '|' || target_id IN ({placeholders})",
            list(old_pairs_before.keys()),
        ).fetchall()
    )
    all_pairs_after = conn.execute(
        "SELECT source_id, target_id, distance FROM comparison"
    ).fetchall()
    conn.close()
    assert old_pairs_after == old_pairs_before

    full_db = tmp_path / "full.db"
    full_folder = tmp_path / "full_corpus"
    full_folder.mkdir()
    for filename in FIXTURE_FILES:
        (full_folder / filename).write_bytes((ARTICLES_DIR / filename).read_bytes())
    pipeline_module.build(folder=full_folder, db_path=full_db, full=True, cache_path=tmp_path / "full_cache.json")

    old_pair_keys = set(old_pairs_before.keys())
    new_pairs = [
        (source_id, target_id, distance)
        for source_id, target_id, distance in all_pairs_after
        if f"{source_id}|{target_id}" not in old_pair_keys
    ]
    assert len(new_pairs) > 0

    conn_full = sqlite3.connect(full_db)
    for source_id, target_id, distance in new_pairs:
        full_distance = conn_full.execute(
            "SELECT distance FROM comparison WHERE source_id = ? AND target_id = ?",
            (source_id, target_id),
        ).fetchone()
        assert full_distance is not None
        assert full_distance[0] == distance
    conn_full.close()
