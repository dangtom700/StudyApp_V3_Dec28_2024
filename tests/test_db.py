import sqlite3

from db import get_connection


def _column_names(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {row[1] for row in rows}


def test_creates_file_info_table(tmp_path):
    conn = get_connection(tmp_path / "test.db")
    assert _column_names(conn, "file_info") == {
        "file_id",
        "source_path",
        "file_name",
        "content_sha256",
        "page_count",
        "ingested_at",
        "token_freq_path",
    }


def test_creates_article_text_table(tmp_path):
    conn = get_connection(tmp_path / "test.db")
    assert _column_names(conn, "article_text") == {"file_id", "full_text"}


def test_creates_article_tags_table(tmp_path):
    conn = get_connection(tmp_path / "test.db")
    assert _column_names(conn, "article_tags") == {"file_id", "tag", "source"}


def test_creates_relation_distance_filtered_table(tmp_path):
    conn = get_connection(tmp_path / "test.db")
    assert _column_names(conn, "relation_distance_filtered") == {"file_id", "token", "frequency", "weight"}


def test_creates_tf_idf_table(tmp_path):
    conn = get_connection(tmp_path / "test.db")
    assert _column_names(conn, "tf_idf") == {"word", "freq", "doc_count", "tf_idf"}


def test_creates_comparison_table(tmp_path):
    conn = get_connection(tmp_path / "test.db")
    assert _column_names(conn, "comparison") == {"source_id", "target_id", "distance"}


def test_creates_comparison_seen_table(tmp_path):
    conn = get_connection(tmp_path / "test.db")
    assert _column_names(conn, "comparison_seen") == {"file_id"}


def test_is_idempotent(tmp_path):
    db_path = tmp_path / "test.db"
    get_connection(db_path).close()
    conn = get_connection(db_path)
    assert _column_names(conn, "file_info") == {
        "file_id",
        "source_path",
        "file_name",
        "content_sha256",
        "page_count",
        "ingested_at",
        "token_freq_path",
    }
