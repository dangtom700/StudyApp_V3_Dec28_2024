import fitz
import pytest

from config import ARTICLES_DIR
from modules.extract_text import extract_article


@pytest.mark.parametrize(
    "filename,expected_page_count,expected_file_id,expected_sha256,expected_text_len",
    [
        (
            "Bifurcation-analysis-for-a-COVID-19-cell-model-with-pa_2026_Alexandria-Engin.pdf",
            18,
            "10.1016/j.aej.2026.04.048",
            "b191466e144919ac41b0bfed167487347553efeaf88c3f36c2ea169e6bc18e5f",
            61899,
        ),
        (
            "Reduced-order-echo-state-networks-for-model-predictive_2026_European-Journal.pdf",
            20,
            "10.1016/j.ejcon.2026.101527",
            "6fd9c43314798d9f540c9ccdc26ce182e7da3bc5e1cd7ba2dcdf9163c462b36c",
            95303,
        ),
    ],
)
def test_extract_article_real_fixture(
    filename, expected_page_count, expected_file_id, expected_sha256, expected_text_len
):
    article = extract_article(ARTICLES_DIR / filename)
    assert article.page_count == expected_page_count
    assert article.file_id == expected_file_id
    assert article.content_sha256 == expected_sha256
    assert len(article.full_text) == expected_text_len
    assert "Keywords:" in article.full_text


def test_extract_article_falls_back_to_sha256_when_no_doi(tmp_path):
    pdf_path = tmp_path / "no_doi.pdf"
    doc = fitz.open()
    page = doc.new_page()
    page.insert_text((72, 72), "This page has no DOI anywhere on it.")
    doc.save(pdf_path)
    doc.close()

    article = extract_article(pdf_path)

    assert article.file_id == f"sha256:{article.content_sha256}"
    assert article.page_count == 1
    assert "no DOI" in article.full_text
