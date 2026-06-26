import sqlite3
from pathlib import Path

from config import DATAMUSE_CACHE_PATH, DB_PATH
from db import get_connection
from modules.datamuse import expand_keyword, load_cache, save_cache
from modules.topic_tags import (
    extract_abstract_text,
    extract_intrinsic_keywords,
    fallback_keywords_from_abstract,
)


def tag_article(
    file_id: str, full_text: str, conn: sqlite3.Connection, cache: dict[str, list[str]]
) -> None:
    keywords = extract_intrinsic_keywords(full_text)
    if not keywords:
        abstract_text = extract_abstract_text(full_text)
        keywords = fallback_keywords_from_abstract(abstract_text) if abstract_text else []

    intrinsic_tags = {keyword.strip().lower() for keyword in keywords if keyword.strip()}
    for tag in intrinsic_tags:
        conn.execute(
            "INSERT OR IGNORE INTO article_tags (file_id, tag, source) VALUES (?, ?, 'intrinsic')",
            (file_id, tag),
        )

    expanded_tags: set[str] = set()
    for keyword in intrinsic_tags:
        for related in expand_keyword(keyword, cache):
            expanded_tags.add(related.strip().lower())
    expanded_tags -= intrinsic_tags

    for tag in expanded_tags:
        conn.execute(
            "INSERT OR IGNORE INTO article_tags (file_id, tag, source) VALUES (?, ?, 'expanded')",
            (file_id, tag),
        )

    conn.commit()


def tag_all_articles(conn: sqlite3.Connection, cache_path: Path = DATAMUSE_CACHE_PATH) -> int:
    cache = load_cache(cache_path)

    untagged = conn.execute(
        """
        SELECT file_info.file_id, article_text.full_text
        FROM file_info
        JOIN article_text ON article_text.file_id = file_info.file_id
        WHERE file_info.file_id NOT IN (SELECT DISTINCT file_id FROM article_tags)
        """
    ).fetchall()

    for file_id, full_text in untagged:
        tag_article(file_id, full_text, conn, cache)

    save_cache(cache, cache_path)
    return len(untagged)


if __name__ == "__main__":
    db_conn = get_connection(DB_PATH)
    try:
        count = tag_all_articles(db_conn)
        print(f"Tagged {count} articles")
    finally:
        db_conn.close()
