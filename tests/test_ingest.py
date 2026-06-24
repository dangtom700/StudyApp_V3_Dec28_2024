import json

from config import ARTICLES_DIR
from db import get_connection
from ingest import ingest_folder, ingest_pdf

FIXTURE = ARTICLES_DIR / "Bifurcation-analysis-for-a-COVID-19-cell-model-with-pa_2026_Alexandria-Engin.pdf"


def test_ingest_pdf_writes_file_info_and_article_text(tmp_path):
    conn = get_connection(tmp_path / "test.db")
    token_freq_dir = tmp_path / "token_freq"

    file_id = ingest_pdf(FIXTURE, conn, token_freq_dir=token_freq_dir)

    assert file_id == "10.1016/j.aej.2026.04.048"

    row = conn.execute(
        "SELECT page_count, token_freq_path FROM file_info WHERE file_id = ?", (file_id,)
    ).fetchone()
    assert row is not None
    assert row[0] == 18
    token_freq_path = row[1]
    assert token_freq_path is not None

    text_row = conn.execute(
        "SELECT full_text FROM article_text WHERE file_id = ?", (file_id,)
    ).fetchone()
    assert text_row is not None
    assert "Keywords:" in text_row[0]

    freq = json.loads(open(token_freq_path, encoding="utf-8").read())
    assert freq["antibodi"] > 0


def test_ingest_pdf_is_idempotent(tmp_path):
    conn = get_connection(tmp_path / "test.db")
    token_freq_dir = tmp_path / "token_freq"

    first_id = ingest_pdf(FIXTURE, conn, token_freq_dir=token_freq_dir)
    second_id = ingest_pdf(FIXTURE, conn, token_freq_dir=token_freq_dir)

    assert first_id == second_id
    count = conn.execute(
        "SELECT COUNT(*) FROM file_info WHERE file_id = ?", (first_id,)
    ).fetchone()[0]
    assert count == 1


def test_ingest_folder_processes_all_pdfs(tmp_path):
    conn = get_connection(tmp_path / "test.db")
    token_freq_dir = tmp_path / "token_freq"

    file_ids = ingest_folder(ARTICLES_DIR, conn, token_freq_dir=token_freq_dir)

    assert len(file_ids) == len(list(ARTICLES_DIR.glob("*.pdf")))
    assert len(file_ids) == len(set(file_ids))
