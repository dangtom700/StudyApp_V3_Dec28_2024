import hashlib
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path

import fitz

DOI_RE = re.compile(r'10\.\d{4,9}/[^\s"<>,;]+')


@dataclass
class ExtractedArticle:
    file_id: str
    content_sha256: str
    page_count: int
    full_text: str


def extract_article(pdf_path: Path) -> ExtractedArticle:
    pdf_bytes = Path(pdf_path).read_bytes()
    content_sha256 = hashlib.sha256(pdf_bytes).hexdigest()

    doc = fitz.open(pdf_path)
    try:
        page_count = len(doc)
        pages_text = [page.get_text() for page in doc]
    finally:
        doc.close()

    page1_text = pages_text[0] if pages_text else ""
    doi_match = DOI_RE.search(page1_text)
    file_id = doi_match.group(0).rstrip(".") if doi_match else f"sha256:{content_sha256}"

    full_text = unicodedata.normalize("NFKC", "".join(pages_text))

    return ExtractedArticle(
        file_id=file_id,
        content_sha256=content_sha256,
        page_count=page_count,
        full_text=full_text,
    )
