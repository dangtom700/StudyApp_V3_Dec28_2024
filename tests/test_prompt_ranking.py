import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from config import ARTICLES_DIR
from db import get_connection
from ingest import ingest_pdf
from modules.tokenize_prompt import tokenize_prompt

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


def test_prompt_ranks_control_papers_above_malaria_paper(tmp_path, retrieval_engine_binary):
    db_path = _make_eight_article_db(tmp_path, retrieval_engine_binary)

    query_freq = tokenize_prompt(
        "model predictive control strategy for nonlinear dynamic systems with closed-loop feedback"
    )
    query_path = tmp_path / "query.json"
    query_path.write_text(json.dumps(query_freq), encoding="utf-8")

    result = run_engine(retrieval_engine_binary, ["--process-prompt", str(db_path), str(query_path)])
    assert result.returncode == 0, result.stderr

    lines = [line.split("\t") for line in result.stdout.strip().splitlines() if line]
    ranked_ids = [row[0] for row in lines]
    scores = {row[0]: float(row[1]) for row in lines}

    assert len(ranked_ids) == 8
    assert ranked_ids[0] == "10.1016/j.conengprac.2026.106951"
    assert ranked_ids[-1] == "10.1016/j.fraope.2026.100655"
    assert abs(scores["10.1016/j.conengprac.2026.106951"] - 0.54141) < 1e-4


def test_prompt_with_no_matching_tokens_returns_empty(tmp_path, retrieval_engine_binary):
    db_path = _make_eight_article_db(tmp_path, retrieval_engine_binary)

    query_path = tmp_path / "query.json"
    query_path.write_text(json.dumps({"zzznonexistenttoken": 5}), encoding="utf-8")

    result = run_engine(retrieval_engine_binary, ["--process-prompt", str(db_path), str(query_path)])
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == ""
