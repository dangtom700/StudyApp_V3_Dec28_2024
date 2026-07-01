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

CREATE TABLE IF NOT EXISTS relation_distance_filtered (
    file_id   TEXT NOT NULL,
    token     TEXT NOT NULL,
    frequency INTEGER NOT NULL,
    weight    REAL NOT NULL,
    PRIMARY KEY (file_id, token)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS tf_idf (
    word      TEXT NOT NULL PRIMARY KEY,
    freq      INTEGER NOT NULL,
    doc_count INTEGER NOT NULL,
    tf_idf    REAL NOT NULL
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS comparison (
    source_id TEXT NOT NULL,
    target_id TEXT NOT NULL,
    distance  REAL NOT NULL CHECK(distance > 0.0),
    PRIMARY KEY (source_id, target_id)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS comparison_seen (
    file_id TEXT PRIMARY KEY REFERENCES file_info(file_id)
);
"""


def get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA)
    return conn
