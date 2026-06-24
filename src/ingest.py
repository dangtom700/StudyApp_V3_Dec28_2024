import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from config import ARTICLES_DIR, TOKEN_FREQ_DIR
from db import get_connection
from modules.extract_text import extract_article
from modules.word_freq import tokenize


def ingest_pdf(
    pdf_path: Path, conn: sqlite3.Connection, token_freq_dir: Path = TOKEN_FREQ_DIR
) -> str:
    article = extract_article(pdf_path)

    existing = conn.execute(
        "SELECT file_id FROM file_info WHERE file_id = ?", (article.file_id,)
    ).fetchone()
    if existing is not None:
        return article.file_id

    freq = tokenize(article.full_text)
    token_freq_dir.mkdir(parents=True, exist_ok=True)
    safe_name = article.file_id.replace("/", "_").replace(":", "_")
    token_freq_path = token_freq_dir / f"{safe_name}.json"
    token_freq_path.write_text(json.dumps(freq, ensure_ascii=False, indent=2), encoding="utf-8")

    conn.execute(
        """
        INSERT INTO file_info
            (file_id, source_path, file_name, content_sha256, page_count, ingested_at, token_freq_path)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            article.file_id,
            str(pdf_path),
            Path(pdf_path).name,
            article.content_sha256,
            article.page_count,
            datetime.now(timezone.utc).isoformat(),
            str(token_freq_path),
        ),
    )
    conn.execute(
        "INSERT INTO article_text (file_id, full_text) VALUES (?, ?)",
        (article.file_id, article.full_text),
    )
    conn.commit()
    return article.file_id


def ingest_folder(
    folder: Path, conn: sqlite3.Connection, token_freq_dir: Path = TOKEN_FREQ_DIR
) -> list[str]:
    return [ingest_pdf(pdf_path, conn, token_freq_dir) for pdf_path in sorted(folder.glob("*.pdf"))]


if __name__ == "__main__":
    db_conn = get_connection()
    try:
        ids = ingest_folder(ARTICLES_DIR, db_conn)
        print(f"Ingested {len(ids)} articles from {ARTICLES_DIR}")
    finally:
        db_conn.close()
