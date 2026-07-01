import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from config import ARTICLES_DIR

import pipeline as pipeline_module

FIXTURE_FILES = [
    "Bifurcation-analysis-for-a-COVID-19-cell-model-with-pa_2026_Alexandria-Engin.pdf",
    "Reduced-order-echo-state-networks-for-model-predictive_2026_European-Journal.pdf",
]
EXTRA_FILE = "A-closed-loop-control-strategy-for-automotive-EHB-base_2026_Control-Engineer.pdf"


def _fixture_folder(tmp_path, filenames):
    folder = tmp_path / "corpus"
    folder.mkdir(exist_ok=True)
    for filename in filenames:
        (folder / filename).write_bytes((ARTICLES_DIR / filename).read_bytes())
    return folder


def test_pipeline_populates_all_tables(tmp_path, retrieval_engine_binary, monkeypatch):
    monkeypatch.setattr(pipeline_module, "ENGINE_BINARY", retrieval_engine_binary)
    folder = _fixture_folder(tmp_path, FIXTURE_FILES)
    db_path = tmp_path / "studyapp.db"

    pipeline_module.build(folder=folder, db_path=db_path, full=True)

    conn = sqlite3.connect(db_path)
    assert conn.execute("SELECT COUNT(*) FROM file_info").fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM article_tags").fetchone()[0] > 0
    assert conn.execute("SELECT COUNT(*) FROM relation_distance_filtered").fetchone()[0] > 0
    assert conn.execute("SELECT COUNT(*) FROM tf_idf").fetchone()[0] > 0
    assert conn.execute("SELECT COUNT(*) FROM comparison").fetchone()[0] > 0


def test_pipeline_incremental_adds_new_file_without_touching_old_pairs(tmp_path, retrieval_engine_binary, monkeypatch):
    monkeypatch.setattr(pipeline_module, "ENGINE_BINARY", retrieval_engine_binary)
    folder = _fixture_folder(tmp_path, FIXTURE_FILES)
    db_path = tmp_path / "studyapp.db"

    pipeline_module.build(folder=folder, db_path=db_path, full=True)
    conn = sqlite3.connect(db_path)
    before = conn.execute(
        "SELECT distance FROM comparison WHERE source_id='10.1016/j.aej.2026.04.048' "
        "AND target_id='10.1016/j.ejcon.2026.101527'"
    ).fetchone()[0]
    conn.close()

    (folder / EXTRA_FILE).write_bytes((ARTICLES_DIR / EXTRA_FILE).read_bytes())
    pipeline_module.build(folder=folder, db_path=db_path, full=False)

    conn = sqlite3.connect(db_path)
    after = conn.execute(
        "SELECT distance FROM comparison WHERE source_id='10.1016/j.aej.2026.04.048' "
        "AND target_id='10.1016/j.ejcon.2026.101527'"
    ).fetchone()[0]
    assert after == before
    assert conn.execute("SELECT COUNT(*) FROM file_info").fetchone()[0] == 3
    assert conn.execute(
        "SELECT COUNT(*) FROM comparison WHERE source_id = '10.1016/j.conengprac.2026.106951'"
    ).fetchone()[0] > 0
