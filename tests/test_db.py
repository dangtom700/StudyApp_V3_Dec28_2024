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
