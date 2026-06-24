# Phase 1: Python Ingestion + Schema Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the Python half of V3's pipeline that turns a folder of article PDFs into queryable SQLite rows (`file_info`, `article_text`) plus per-article JSON token-frequency files, ready for Phase 2's C++ engine to consume.

**Architecture:** A trimmed conda env (`StudyAssistant`) provides PyMuPDF/NLTK/pytest. `config.py` is the single source of truth for paths (no hardcoded absolute paths). `extract_text.py` opens a PDF once, computes a DOI-or-content-hash ID, and returns whole-article text (NFKC-normalized, no chunking). `word_freq.py` tokenizes that text into a stemmed word-frequency dict. `ingest.py` wires these into an idempotent per-PDF pipeline that writes to `db.py`'s schema and to a JSON token-frequency file — the exact handoff format Phase 2's C++ engine will read.

**Tech Stack:** Python 3.12 (conda env `StudyAssistant`), PyMuPDF (`fitz`) for PDF text extraction, NLTK (Porter stemmer + stopwords corpus) for tokenization, stdlib `sqlite3` for storage, pytest for testing.

## Global Constraints

- Input is PDFs only, one row per article — no book-style multi-chunk sliding window (per spec Architecture section).
- File identity is DOI-based where extractable from page 1, else `sha256:<hexdigest>` of the raw PDF bytes — never MD5-of-path (V2's rename-churn bug, per spec's "V2 bugs avoided by construction").
- Tokenization keeps V2's proven algorithm (NLTK word_tokenize + Porter stemming + stopword/punctuation/repeated-char filtering) — it is not book/article-specific, per spec's Architecture section.
- Python → C++ handoff is via SQLite + JSON token-frequency files — no pybind11 (explicit scale-driven choice in spec).
- One config source (`config.py`), no per-language hardcoded paths like V2's `D:\READING LIST` (per spec's bug-avoidance section).
- Dependencies are trimmed to what Phase 1 actually uses — no chromadb/langchain/langgraph/ollama/onnxruntime/opentelemetry/polars/numpy/scipy. Those backed features already dropped from scope (ChromaDB/LangChain integration, Ollama ideation chat, Python fast-approximate similarity path) and must not be carried into the new environment manifest.
- No GUI, no `.env` file support — plain Python constants in `config.py` are sufficient (YAGNI; nothing in the spec calls for runtime-configurable paths beyond fixing V2's hardcoded-path bug).

---

## Ground-truth fixtures used by this plan

Verified directly against real files in `articles/` (not assumed):

**`articles/Bifurcation-analysis-for-a-COVID-19-cell-model-with-pa_2026_Alexandria-Engin.pdf`**
- `page_count`: 18
- `file_id` (DOI on page 1): `10.1016/j.aej.2026.04.048`
- `content_sha256`: `b191466e144919ac41b0bfed167487347553efeaf88c3f36c2ea169e6bc18e5f`
- NFKC-normalized full-text length: 61899 characters
- Contains the literal substring `"Keywords:"`

**`articles/Reduced-order-echo-state-networks-for-model-predictive_2026_European-Journal.pdf`**
- `page_count`: 20
- `file_id` (DOI on page 1): `10.1016/j.ejcon.2026.101527`
- `content_sha256`: `6fd9c43314798d9f540c9ccdc26ce182e7da3bc5e1cd7ba2dcdf9163c462b36c`
- NFKC-normalized full-text length: 95303 characters
- Contains the literal substring `"Keywords:"`

Two real-data gotchas discovered while grounding these fixtures, both handled by this plan's code:
1. PDF text extraction renders ligatures (e.g. U+FB00 `ﬀ` in "oﬀer") that NLTK/stopword matching won't recognize as plain ASCII letters unless the text is Unicode-NFKC-normalized first. `extract_text.py` normalizes immediately after extraction.
2. A random 10-PDF sample of the corpus confirmed every article has its own DOI reliably extractable from **page 1 only** (searching all pages would also match reference-list citations to other papers' DOIs).

---

## Task 1: Trimmed environment manifest

**Files:**
- Create: `src/env.yml`

- [ ] **Step 1: Write the trimmed environment manifest**

```yaml
name: StudyAssistant
channels:
  - defaults
dependencies:
  - python=3.12
  - pip
  - pip:
      - pymupdf==1.27.2.3
      - nltk==3.9.4
      - pytest==9.1.1
```

- [ ] **Step 2: Create the conda environment**

Run: `conda env create -f src/env.yml`
Expected: environment `StudyAssistant` created successfully. (If it already exists from a prior run, first run `conda env remove -n StudyAssistant -y`.)

- [ ] **Step 3: Verify the environment has the right packages**

Run: `conda run -n StudyAssistant python -c "import fitz, nltk, pytest; print('ok')"`
Expected: `ok`

- [ ] **Step 4: Commit**

```bash
git add src/env.yml
git commit -m "Add trimmed StudyAssistant env manifest (pymupdf, nltk, pytest only)"
```

---

## Task 2: NLTK corpora setup script

**Files:**
- Create: `src/setup_nltk.py`

**Interfaces:**
- Produces: downloaded `punkt`, `punkt_tab`, `stopwords` NLTK corpora, required by Task 6's `tokenize()`.

- [ ] **Step 1: Write the setup script**

```python
import nltk


def download_corpora() -> None:
    for corpus in ("punkt", "punkt_tab", "stopwords"):
        nltk.download(corpus)


if __name__ == "__main__":
    download_corpora()
```

- [ ] **Step 2: Run the script**

Run: `conda run -n StudyAssistant python src/setup_nltk.py`
Expected: output showing `punkt`, `punkt_tab`, and `stopwords` downloaded (or already up to date).

- [ ] **Step 3: Verify the corpora are importable**

Run: `conda run -n StudyAssistant python -c "import nltk; from nltk.corpus import stopwords; nltk.data.find('tokenizers/punkt_tab'); print(len(stopwords.words('english')))"`
Expected: a number printed (198 with this NLTK version) and no `LookupError`.

- [ ] **Step 4: Commit**

```bash
git add src/setup_nltk.py
git commit -m "Add NLTK corpora setup script"
```

---

## Task 3: Project config and pytest setup

**Files:**
- Create: `src/config.py`
- Create: `pyproject.toml`
- Test: `tests/test_config.py`
- Delete: `src/modules/path.py` (hardcoded `D:\READING LIST` path; superseded by `config.py`)
- Delete: `src/main.py` (old chunked-book CLI orchestration; its only consumers — `modules.path`, the old `extract_text`/`word_freq` APIs — are being replaced in this plan; Phase 4 will define V3's real orchestration entrypoint)

**Interfaces:**
- Produces: `ARTICLES_DIR`, `DATA_DIR`, `DB_PATH`, `TOKEN_FREQ_DIR` (all `pathlib.Path`), used by Tasks 4, 5, and 7.

- [ ] **Step 1: Write the failing test**

```python
from pathlib import Path

from config import ARTICLES_DIR, DATA_DIR, DB_PATH, TOKEN_FREQ_DIR


def test_articles_dir_points_at_real_corpus():
    assert ARTICLES_DIR.name == "articles"
    assert ARTICLES_DIR.is_dir()
    assert any(ARTICLES_DIR.glob("*.pdf"))


def test_data_paths_are_under_data_dir():
    assert DB_PATH.parent == DATA_DIR
    assert TOKEN_FREQ_DIR.parent == DATA_DIR
    assert DB_PATH.name == "studyapp.db"
    assert TOKEN_FREQ_DIR.name == "token_freq"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `conda run -n StudyAssistant python -m pytest tests/test_config.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'config'`

- [ ] **Step 3: Write `pyproject.toml` so pytest can find `src/`**

```toml
[tool.pytest.ini_options]
pythonpath = ["src"]
testpaths = ["tests"]
```

- [ ] **Step 4: Write `src/config.py`**

```python
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parent
REPO_ROOT = SRC_DIR.parent
PROJECT_ROOT = REPO_ROOT.parent

ARTICLES_DIR = PROJECT_ROOT / "articles"
DATA_DIR = REPO_ROOT / "data"
DB_PATH = DATA_DIR / "studyapp.db"
TOKEN_FREQ_DIR = DATA_DIR / "token_freq"
```

- [ ] **Step 5: Run test to verify it passes**

Run: `conda run -n StudyAssistant python -m pytest tests/test_config.py -v`
Expected: PASS (2 passed)

- [ ] **Step 6: Delete the superseded files**

```bash
git rm src/modules/path.py src/main.py
```

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml src/config.py tests/test_config.py
git commit -m "Add config.py as single path source; remove hardcoded-path module and stale CLI"
```

---

## Task 4: Database schema (`db.py`)

**Files:**
- Create: `src/db.py`
- Test: `tests/test_db.py`

**Interfaces:**
- Consumes: `config.DB_PATH` (Task 3).
- Produces: `get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection`, used by Task 7.

- [ ] **Step 1: Write the failing test**

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `conda run -n StudyAssistant python -m pytest tests/test_db.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'db'`

- [ ] **Step 3: Write `src/db.py`**

```python
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
"""


def get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA)
    return conn
```

- [ ] **Step 4: Run test to verify it passes**

Run: `conda run -n StudyAssistant python -m pytest tests/test_db.py -v`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add src/db.py tests/test_db.py
git commit -m "Add file_info/article_text schema"
```

---

## Task 5: PDF extraction and ID generation (`modules/extract_text.py`)

**Files:**
- Modify (full rewrite): `src/modules/extract_text.py`
- Create: `src/modules/__init__.py` (empty)
- Test: `tests/test_extract_text.py`

**Interfaces:**
- Produces: `ExtractedArticle` (dataclass: `file_id: str`, `content_sha256: str`, `page_count: int`, `full_text: str`) and `extract_article(pdf_path: Path) -> ExtractedArticle`, used by Task 7.

- [ ] **Step 1: Write the failing test**

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `conda run -n StudyAssistant python -m pytest tests/test_extract_text.py -v`
Expected: FAIL — `extract_article` does not exist in the current `modules/extract_text.py` (current file only has the old `extract_text_from_pdf`/chunking functions).

- [ ] **Step 3: Write `src/modules/__init__.py`**

```python
```

(empty file — makes `modules` an explicit package)

- [ ] **Step 4: Rewrite `src/modules/extract_text.py`**

```python
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
```

- [ ] **Step 5: Run test to verify it passes**

Run: `conda run -n StudyAssistant python -m pytest tests/test_extract_text.py -v`
Expected: PASS (3 passed)

- [ ] **Step 6: Commit**

```bash
git add src/modules/extract_text.py src/modules/__init__.py tests/test_extract_text.py
git commit -m "Rewrite extract_text.py: whole-article extraction, DOI/sha256 IDs, NFKC normalization"
```

---

## Task 6: Tokenization (`modules/word_freq.py`)

**Files:**
- Modify (full rewrite): `src/modules/word_freq.py`
- Test: `tests/test_word_freq.py`

**Interfaces:**
- Consumes: NLTK corpora downloaded in Task 2.
- Produces: `tokenize(text: str) -> dict[str, int]`, used by Task 7.

- [ ] **Step 1: Write the failing test**

```python
from modules.word_freq import tokenize


def test_tokenize_stems_and_counts_words():
    text = (
        "Control systems control models. The model offers control of "
        "control systems, et al. 2023."
    )
    result = tokenize(text)
    assert result == {"control": 4, "system": 2, "model": 2, "offer": 1}


def test_tokenize_drops_stopwords_only_input():
    assert tokenize("a an the") == {}


def test_tokenize_drops_repeated_char_tokens():
    assert tokenize("aaaaaa loooong wooord") == {}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `conda run -n StudyAssistant python -m pytest tests/test_word_freq.py -v`
Expected: FAIL — current `modules/word_freq.py` has no top-level `tokenize` function (it has `clean_text` wired to a chunk-table DB pipeline) and imports `modules.path`, which Task 3 deleted.

- [ ] **Step 3: Rewrite `src/modules/word_freq.py`**

```python
import re
from collections import defaultdict

import nltk
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer

# Words V2 found needed manual exclusion beyond NLTK's stopword list (mostly
# modal verbs that don't discriminate topic), plus "et"/"al" which pollute
# academic-article word frequencies via "et al." citations.
_EXTRA_STOPWORDS = {
    "also", "could", "done", "enough", "far", "get", "got", "gotten", "may",
    "might", "must", "near", "need", "ought", "shall", "since", "theirselves",
    "us", "would", "et", "al",
}

_REPEATED_CHAR_RE = re.compile(r"([a-zA-Z])\1{2,}")
_NON_WORD_RE = re.compile(r"[^\w\s]")

_stemmer = PorterStemmer()
_stop_words = frozenset(stopwords.words("english")) | _EXTRA_STOPWORDS


def tokenize(text: str) -> dict[str, int]:
    text = _NON_WORD_RE.sub("", text).lower()
    tokens = nltk.word_tokenize(text)

    freq: dict[str, int] = defaultdict(int)
    for token in tokens:
        if (
            token.isalpha()
            and token not in _stop_words
            and not _REPEATED_CHAR_RE.search(token)
        ):
            freq[_stemmer.stem(token)] += 1

    return dict(freq)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `conda run -n StudyAssistant python -m pytest tests/test_word_freq.py -v`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add src/modules/word_freq.py tests/test_word_freq.py
git commit -m "Rewrite word_freq.py: standalone tokenize(), drop chunk-table dependency"
```

---

## Task 7: Ingestion orchestration (`ingest.py`)

**Files:**
- Create: `src/ingest.py`
- Test: `tests/test_ingest.py`

**Interfaces:**
- Consumes: `config.ARTICLES_DIR`/`config.TOKEN_FREQ_DIR` (Task 3), `db.get_connection` (Task 4), `modules.extract_text.extract_article` (Task 5), `modules.word_freq.tokenize` (Task 6).
- Produces: `ingest_pdf(pdf_path: Path, conn: sqlite3.Connection, token_freq_dir: Path = TOKEN_FREQ_DIR) -> str` and `ingest_folder(folder: Path, conn: sqlite3.Connection, token_freq_dir: Path = TOKEN_FREQ_DIR) -> list[str]`. Phase 2's C++ engine reads the `file_info`/`article_text` rows and the JSON files at `token_freq_path` these produce.

- [ ] **Step 1: Write the failing test**

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `conda run -n StudyAssistant python -m pytest tests/test_ingest.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ingest'`

- [ ] **Step 3: Write `src/ingest.py`**

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `conda run -n StudyAssistant python -m pytest tests/test_ingest.py -v`
Expected: PASS (3 passed). Note: `test_ingest_folder_processes_all_pdfs` ingests the full 99-article corpus, so this run will take longer than the other tests (real PDF extraction + tokenization x99) — that is expected, not a hang.

- [ ] **Step 5: Run the full Phase 1 test suite**

Run: `conda run -n StudyAssistant python -m pytest tests/ -v`
Expected: all tests pass (test_config: 2, test_db: 3, test_extract_text: 3, test_word_freq: 3, test_ingest: 3 — 14 total).

- [ ] **Step 6: Commit**

```bash
git add src/ingest.py tests/test_ingest.py
git commit -m "Add idempotent ingest pipeline tying extraction, tokenization, db, and JSON handoff together"
```

---

## Self-Review

**Spec coverage:**
- "PyMuPDF (fitz), one row per article" → Task 5 (`extract_article`), Task 4 schema (`article_text` one row per `file_id`). ✓
- "ID generation moves off MD5 of file path... to a content-hash or DOI-based ID where extractable" → Task 5 (`DOI_RE`, `sha256:` fallback), validated against a 10-PDF random sample. ✓
- "tokenize — NLTK + stopword filtering + Porter stemming (unchanged from V2)" → Task 6, ported V2's algorithm and full custom stopword set (verified 19 of V2's 122 custom stopwords aren't covered by the current NLTK stopword corpus, so they're kept, not dropped). ✓
- "Python → C++ handoff via SQLite + JSON token-frequency files" → Task 7 writes both the `file_info`/`article_text` rows and the JSON file at `token_freq_path`. ✓
- "`file_info` kept... `article_text` renamed from `pdf_chunks`... one row per article" → Task 4 schema. ✓
- "4+ inconsistent hardcoded `D:\READING LIST`-style paths... fixed via one config source" → Task 3 (`config.py`, deletion of `modules/path.py`). ✓
- Incremental-build-friendliness at the ingestion layer (formalizing `skim_files`) → Task 7's idempotent skip-if-`file_id`-exists check. Full corpus-level incremental-vs-full-rebuild equivalence (the C++ `comparison` table) is Phase 2/4 scope, not Phase 1.
- Topic tagging, prompt ranking, C++ engine, orchestration/.bat replacement → explicitly out of scope for Phase 1 (Phases 2-4).

**Placeholder scan:** No TBD/TODO markers; every step has complete, runnable code and exact expected command output.

**Type consistency:** `ExtractedArticle` fields (`file_id: str`, `content_sha256: str`, `page_count: int`, `full_text: str`) declared in Task 5 are used with matching names/types in Task 7's `ingest_pdf`. `tokenize(text: str) -> dict[str, int]` declared in Task 6 matches its call site in Task 7. `get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection` declared in Task 4 matches all call sites in Task 7's tests and `__main__` block.

---

## Out of scope for this phase (later phases)

- C++ relational-distance/TF-IDF/comparison engine reading these JSON files (Phase 2).
- Topic tagging (`article_tags` table, Keywords-field heuristic, Datamuse expansion) and prompt-based ranking (Phase 3). Note for that phase: real fixtures show `"Keywords:"` is followed by one keyword phrase per line (not semicolon-separated on one line as an earlier draft assumed), and section headings like `"Abstract"` are rendered letter-spaced (`"A B S T R A C T"`) by PDF extraction — any heading-detection heuristic must collapse internal whitespace before comparing, not use plain substring search.
- Incremental full-corpus rebuild semantics for the C++ `comparison` table, and orchestration/.bat replacement (Phase 4).
- `ingest_pdf`'s idempotency check (Task 7) keys on `file_id` only, not content. A PDF replaced under the same DOI is silently skipped (stale `content_sha256`/text), and a sha256-fallback-id file that changes content gets a new `file_id` (orphaning the old row) instead of being updated. Flagged by final review and explicitly deferred to Phase 4 by the project owner — Phase 4's incremental-build design must decide whether to add a content_sha256 comparison before skipping.
