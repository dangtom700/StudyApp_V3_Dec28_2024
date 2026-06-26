import sqlite3
from pathlib import Path

from config import DB_PATH

SCHEMA = """
CREATE TABLE IF NOT EXISTS file_info (
    file_id TEXT PRIMARY KEY,
    source_path TEXT NOT NULL,
    file_name TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    page_count INTEGER NOT NULL,
    ingested_at TEXT NOT NULL,
    token_freq_path TEXT
);

CREATE TABLE IF NOT EXISTS article_text (
    file_id TEXT PRIMARY KEY REFERENCES file_info(file_id),
    full_text TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS article_tags (
    file_id TEXT NOT NULL REFERENCES file_info(file_id),
    tag TEXT NOT NULL,
    source TEXT NOT NULL CHECK(source IN ('intrinsic', 'expanded')),
    PRIMARY KEY (file_id, tag, source)
);
"""


def get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA)
    return conn
