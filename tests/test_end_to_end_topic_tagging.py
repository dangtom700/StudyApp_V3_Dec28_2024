from config import ARTICLES_DIR
from db import get_connection
from ingest import ingest_pdf
from tag_topics import tag_all_articles

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


def test_full_topic_tagging_pipeline_on_real_corpus_with_real_datamuse(tmp_path):
    conn = get_connection(tmp_path / "studyapp.db")
    for filename in FIXTURE_FILES:
        ingest_pdf(ARTICLES_DIR / filename, conn, token_freq_dir=tmp_path / "token_freq")

    tagged_count = tag_all_articles(conn, cache_path=tmp_path / "cache.json")
    assert tagged_count == 8

    intrinsic_count = conn.execute(
        "SELECT COUNT(*) FROM article_tags WHERE source = 'intrinsic'"
    ).fetchone()[0]
    assert intrinsic_count >= 8 * 3  # every fixture has at least 3 verified intrinsic keywords

    expanded_count = conn.execute(
        "SELECT COUNT(*) FROM article_tags WHERE source = 'expanded'"
    ).fetchone()[0]
    assert expanded_count > 0

    conengprac_tags = {
        row[0]
        for row in conn.execute(
            "SELECT tag FROM article_tags WHERE file_id = ? AND source = 'intrinsic'",
            ("10.1016/j.conengprac.2026.106951",),
        ).fetchall()
    }
    assert conengprac_tags == {
        "ehb systems",
        "hysteresis characteristics",
        "rc operator",
        "rbf neural network",
        "closed-loop control",
        "stability proof",
    }
