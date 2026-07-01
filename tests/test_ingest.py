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


def test_ingest_pdf_updates_on_content_change(tmp_path):
    conn = get_connection(tmp_path / "test.db")
    token_freq_dir = tmp_path / "token_freq"

    file_id = ingest_pdf(FIXTURE, conn, token_freq_dir=token_freq_dir)
    conn.execute("UPDATE file_info SET content_sha256 = 'stale' WHERE file_id = ?", (file_id,))
    conn.commit()

    ingest_pdf(FIXTURE, conn, token_freq_dir=token_freq_dir)

    row = conn.execute("SELECT content_sha256 FROM file_info WHERE file_id = ?", (file_id,)).fetchone()
    assert row[0] != "stale"


def test_ingest_pdf_purges_stale_downstream_data_on_content_change(tmp_path):
    conn = get_connection(tmp_path / "test.db")
    token_freq_dir = tmp_path / "token_freq"

    file_id = ingest_pdf(FIXTURE, conn, token_freq_dir=token_freq_dir)
    conn.execute("UPDATE file_info SET content_sha256 = 'stale' WHERE file_id = ?", (file_id,))
    conn.execute("INSERT INTO article_tags (file_id, tag, source) VALUES (?, 'oldtag', 'intrinsic')", (file_id,))
    conn.execute(
        "INSERT INTO relation_distance_filtered (file_id, token, frequency, weight) VALUES (?, 'oldtok', 1, 1.0)",
        (file_id,),
    )
    conn.execute("INSERT INTO comparison_seen (file_id) VALUES (?)", (file_id,))
    conn.commit()

    ingest_pdf(FIXTURE, conn, token_freq_dir=token_freq_dir)

    assert conn.execute("SELECT COUNT(*) FROM article_tags WHERE file_id = ?", (file_id,)).fetchone()[0] == 0
    assert conn.execute(
        "SELECT COUNT(*) FROM relation_distance_filtered WHERE file_id = ?", (file_id,)
    ).fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM comparison_seen WHERE file_id = ?", (file_id,)).fetchone()[0] == 0


def test_ingest_pdf_cleans_up_orphaned_row_on_source_path_reuse(tmp_path):
    conn = get_connection(tmp_path / "test.db")
    token_freq_dir = tmp_path / "token_freq"

    conn.execute(
        """
        INSERT INTO file_info
            (file_id, source_path, file_name, content_sha256, page_count, ingested_at, token_freq_path)
        VALUES ('sha256:deadbeef', ?, 'stale.pdf', 'deadbeef', 1, '2020-01-01T00:00:00+00:00', NULL)
        """,
        (str(FIXTURE),),
    )
    conn.commit()

    real_file_id = ingest_pdf(FIXTURE, conn, token_freq_dir=token_freq_dir)

    assert real_file_id != "sha256:deadbeef"
    assert conn.execute(
        "SELECT COUNT(*) FROM file_info WHERE file_id = 'sha256:deadbeef'"
    ).fetchone()[0] == 0
    assert conn.execute(
        "SELECT COUNT(*) FROM file_info WHERE file_id = ?", (real_file_id,)
    ).fetchone()[0] == 1


def test_ingest_folder_processes_all_pdfs(tmp_path):
    conn = get_connection(tmp_path / "test.db")
    token_freq_dir = tmp_path / "token_freq"

    file_ids = ingest_folder(ARTICLES_DIR, conn, token_freq_dir=token_freq_dir)

    assert len(file_ids) == len(list(ARTICLES_DIR.glob("*.pdf")))
    assert len(file_ids) == len(set(file_ids))
