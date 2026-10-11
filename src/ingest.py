import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from config import ARTICLES_DIR, TOKEN_FREQ_DIR
from db import get_connection
from modules.extract_text import extract_article
from modules.word_freq import tokenize


def _purge_file(conn: sqlite3.Connection, file_id: str) -> None:
    conn.execute("DELETE FROM article_tags WHERE file_id = ?", (file_id,))
    conn.execute("DELETE FROM relation_distance_filtered WHERE file_id = ?", (file_id,))
    conn.execute("DELETE FROM comparison WHERE source_id = ? OR target_id = ?", (file_id, file_id))
    conn.execute("DELETE FROM comparison_seen WHERE file_id = ?", (file_id,))
    conn.execute("DELETE FROM article_text WHERE file_id = ?", (file_id,))
    conn.execute("DELETE FROM file_info WHERE file_id = ?", (file_id,))


def ingest_pdf(
    pdf_path: Path, conn: sqlite3.Connection, token_freq_dir: Path = TOKEN_FREQ_DIR
) -> str:
    file_id, _ = _ingest_pdf(pdf_path, conn, token_freq_dir)
    return file_id


def _ingest_pdf(
    pdf_path: Path, conn: sqlite3.Connection, token_freq_dir: Path
) -> tuple[str, str]:
    """Ingest one PDF; returns (file_id, status) where status is
    "new", "updated" or "unchanged"."""
    article = extract_article(pdf_path)

    existing = conn.execute(
        "SELECT content_sha256 FROM file_info WHERE file_id = ?", (article.file_id,)
    ).fetchone()
    if existing is not None and existing[0] == article.content_sha256:
        return article.file_id, "unchanged"
    if existing is not None:
        _purge_file(conn, article.file_id)

    orphan = conn.execute(
        "SELECT file_id FROM file_info WHERE source_path = ? AND file_id != ?",
        (str(pdf_path), article.file_id),
    ).fetchone()
    if orphan is not None:
        _purge_file(conn, orphan[0])

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
    return article.file_id, "updated" if existing is not None else "new"


def ingest_folder(
    folder: Path, conn: sqlite3.Connection, token_freq_dir: Path = TOKEN_FREQ_DIR
) -> list[str]:
    pdf_paths = sorted(folder.glob("*.pdf"))
    total = len(pdf_paths)
    print(f"  Found {total} PDFs", flush=True)

    file_ids = []
    counts = {"new": 0, "updated": 0, "unchanged": 0}
    for i, pdf_path in enumerate(pdf_paths, start=1):
        print(f"  ({i}/{total}) {pdf_path.name} ... ", end="", flush=True)
        file_id, status = _ingest_pdf(pdf_path, conn, token_freq_dir)
        print(status, flush=True)
        counts[status] += 1
        file_ids.append(file_id)

    print(
        f"  new: {counts['new']}, updated: {counts['updated']}, unchanged: {counts['unchanged']}",
        flush=True,
    )
    return file_ids


if __name__ == "__main__":
    db_conn = get_connection()
    try:
        ids = ingest_folder(ARTICLES_DIR, db_conn)
        print(f"Ingested {len(ids)} articles from {ARTICLES_DIR}")
    finally:
        db_conn.close()
