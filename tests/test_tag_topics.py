from config import ARTICLES_DIR
from db import get_connection
from ingest import ingest_pdf
from tag_topics import tag_all_articles, tag_article

FIXTURE_WITH_KEYWORDS = (
    ARTICLES_DIR / "A-closed-loop-control-strategy-for-automotive-EHB-base_2026_Control-Engineer.pdf"
)
FIXTURE_NO_KEYWORDS = (
    ARTICLES_DIR / "Dynamic-modeling--linearization-and-characteristic-anal_2026_Chinese-Journal.pdf"
)


def _fake_expand_keyword(keyword, cache, limit=8, request_delay=0.0):
    cache[keyword] = [f"{keyword}-related"]
    return cache[keyword]


def test_tag_article_writes_intrinsic_and_expanded_tags(tmp_path, monkeypatch):
    monkeypatch.setattr("tag_topics.expand_keyword", _fake_expand_keyword)

    conn = get_connection(tmp_path / "test.db")
    file_id = ingest_pdf(FIXTURE_WITH_KEYWORDS, conn, token_freq_dir=tmp_path / "token_freq")
    full_text = conn.execute(
        "SELECT full_text FROM article_text WHERE file_id = ?", (file_id,)
    ).fetchone()[0]

    tag_article(file_id, full_text, conn, cache={})

    intrinsic = {
        row[0]
        for row in conn.execute(
            "SELECT tag FROM article_tags WHERE file_id = ? AND source = 'intrinsic'",
            (file_id,),
        ).fetchall()
    }
    assert intrinsic == {
        "ehb systems",
        "hysteresis characteristics",
        "rc operator",
        "rbf neural network",
        "closed-loop control",
        "stability proof",
    }

    expanded = {
        row[0]
        for row in conn.execute(
            "SELECT tag FROM article_tags WHERE file_id = ? AND source = 'expanded'",
            (file_id,),
        ).fetchall()
    }
    assert expanded == {f"{kw}-related" for kw in intrinsic}


def test_tag_article_falls_back_when_no_keywords_field(tmp_path, monkeypatch):
    monkeypatch.setattr("tag_topics.expand_keyword", _fake_expand_keyword)

    conn = get_connection(tmp_path / "test.db")
    file_id = ingest_pdf(FIXTURE_NO_KEYWORDS, conn, token_freq_dir=tmp_path / "token_freq")
    full_text = conn.execute(
        "SELECT full_text FROM article_text WHERE file_id = ?", (file_id,)
    ).fetchone()[0]

    tag_article(file_id, full_text, conn, cache={})

    intrinsic = {
        row[0]
        for row in conn.execute(
            "SELECT tag FROM article_tags WHERE file_id = ? AND source = 'intrinsic'",
            (file_id,),
        ).fetchall()
    }
    assert intrinsic == {"model", "system", "dynam", "linear", "analysi"}


def test_tag_all_articles_skips_already_tagged_files(tmp_path, monkeypatch):
    monkeypatch.setattr("tag_topics.expand_keyword", _fake_expand_keyword)

    conn = get_connection(tmp_path / "test.db")
    ingest_pdf(FIXTURE_WITH_KEYWORDS, conn, token_freq_dir=tmp_path / "token_freq")

    first_count = tag_all_articles(conn, cache_path=tmp_path / "cache.json")
    second_count = tag_all_articles(conn, cache_path=tmp_path / "cache.json")

    assert first_count == 1
    assert second_count == 0
