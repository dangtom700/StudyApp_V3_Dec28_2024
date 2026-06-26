# Phase 3: Topic Tagging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract each article's intrinsic keywords (with an abstract-derived fallback), expand them via the Datamuse "means like" API for cross-article topic clustering, and persist both as rows in a new `article_tags` table.

**Architecture:** Pure Python, sitting between Phase 1's ingestion and Phase 2's C++ engine in the pipeline (`extract -> tokenize -> topic tagging -> [C++ handoff]`). `src/modules/topic_tags.py` extracts keywords from already-extracted full text (no new PDF parsing); `src/modules/datamuse.py` is a thin, disk-cached HTTP client; `src/tag_topics.py` orchestrates both against the existing `file_info`/`article_text` tables and writes `article_tags`.

**Tech Stack:** Python 3.12, `requests` (new dependency, pinned `2.34.2`), reuses Phase 1's `modules.word_freq.tokenize`. No new C++ code — `article_tags` is consumed as metadata only, never by the C++ comparison/TF-IDF engine.

## Global Constraints

- Scoped to **article -> tags only** (per the design spec's Topic Tagging section) — no topic-to-topic similarity/clustering, no `tags`/`tags_full`/`topic_similarity` tables (those are explicitly out of scope, same as V2's dropped `expand_degree`).
- **Verified correction to the design spec's assumed format:** the spec describes Keywords extraction as `Keywords: term1; term2; term3` (semicolon-separated, single line). Checking real extracted text from all 280 PDFs currently in `articles/` shows ScienceDirect's actual layout is **one keyword per line**, immediately followed by an `abstract` / `a b s t r a c t` header line — there is no semicolon anywhere. This plan's regex matches the verified real shape, not the spec's assumed one.
- **Verified reliability:** 277/280 articles (98.9%) have a cleanly-extractable Keywords line list. The 3 without one are "Journal Pre-proof" cover-page-only PDFs that still contain a plain `Abstract` header later in the text — confirmed by direct extraction, not assumed.
- **"Top TF-IDF terms from Abstract" fallback (spec wording) is implemented as local term-frequency ranking, not real TF-IDF** — the corpus-wide `tf_idf` SQL table doesn't exist yet at this pipeline stage (it's built by Phase 2's C++ engine, which runs *after* topic tagging per the architecture diagram). This is a deliberate, documented simplification, not an oversight.
- Datamuse expansion reuses V2's exact mechanism (`recommend.hpp`'s `get_related_topics`: `GET https://api.datamuse.com/words?ml=<seed>&max=<n>`), seeded from real per-article keywords instead of V2's random Wikipedia topics, per the design spec.
- Every unique (lowercased) keyword is fetched from Datamuse **at most once per run**, via an on-disk JSON cache at `data/datamuse_cache.json` — verified necessary, since the 280-article corpus shares heavy domain vocabulary (e.g. "control", "model predictive control" appear across dozens of articles).
- No `ThreadPoolExecutor`/parallel fetching (V2 had this for bulk Wikipedia scraping at much higher volume) — sequential calls with caching are sufficient at this corpus size; adding concurrency here would be unjustified complexity (YAGNI).
- `article_tags.tag` is always stored lowercased and stripped, regardless of source, so cross-article tag matching is case-insensitive by construction.
- `tag_all_articles` only processes `file_id`s with no existing `article_tags` rows — a per-file idempotency check, distinct from (and not a substitute for) Phase 4's planned incremental-rebuild semantics for `comparison`/`relation_distance_filtered`.

---

## Ground-truth fixtures used by this plan

All values below were produced by actually running extraction code against real PDFs in `articles/` and real calls to the live Datamuse API during planning (not estimated).

**Corpus size:** 280 PDFs currently in `articles/` (grown from 99 at spec time, 194 at Phase 2 time — re-verify count, but expect growth, not shrinkage).

**Keyword extraction fixtures** (exact, verified output of `extract_intrinsic_keywords`):

| File | `file_id` | Keywords |
|---|---|---|
| `A-closed-loop-control-strategy-for-automotive-EHB-base_2026_Control-Engineer.pdf` | `10.1016/j.conengprac.2026.106951` | `["EHB systems", "Hysteresis characteristics", "RC operator", "RBF neural network", "Closed-loop control", "Stability proof"]` (6) |
| `A-data-driven-approach-for-tailored-fatigue-modeling-_2026_Computers---Indus.pdf` | `10.1016/j.cie.2026.111997` | `["Physical fatigue", "Industrial engineering", "Wearable sensors", "Human factors", "Fuzzy model", "Fatigue-recovery model", "Data-driven approach", "Production planning and control", "Industry 5.0"]` (9) |
| `Dynamic-modeling--linearization-and-characteristic-anal_2026_Chinese-Journal.pdf` | `10.1016/j.cja.2026.104140` | `[]` — no Keywords field (a "Journal Pre-proof" cover page only) |

**Abstract-marker variants seen:** `"a b s t r a c t"` / `"A B S T R A C T"` (spaced-letter header, immediately after a Keywords list) and plain `"Abstract"` (normal header, used as the fallback path in pre-proof PDFs lacking a Keywords box). One regex with `\s*` between each letter matches both.

**Fallback fixture** (no-Keywords file above): `extract_abstract_text` finds the header at the article's real abstract (not at the start of the document — this PDF's first ~2KB is legal/cover-page boilerplate), returning text starting `"This study investigates on the Drag-Free and Attitude Control System (DFACS) for the Taiji\nspace-based..."`. `fallback_keywords_from_abstract(..., top_n=5)` on that text returns exactly `["model", "system", "dynam", "linear", "analysi"]` (stemmed, via the existing `tokenize()`).

**Known extraction limitation (documented, not solved):** a small number of multi-line-wrapped keywords split across two list entries (e.g. one real fixture yields `"Discrete-time nonlinear model predictive"` and `"control"` as two separate keywords instead of one phrase). Each fragment still carries real topical signal post-Datamuse-expansion, so this is accepted as-is rather than adding wrapped-line-rejoining logic for a rare case.

**Datamuse API** (`https://api.datamuse.com/words`, no API key required — confirmed reachable): `?ml=control&max=5` returns `[{"word":"operate",...},{"word":"command",...},{"word":"ascendancy",...},{"word":"dominance",...},{"word":"mastery",...}]`. **Known limitation, verified, not solved:** acronym-heavy domain keywords common in this corpus (e.g. `"EHB systems"`) return low-signal generic words from Datamuse, since it doesn't recognize the acronym — expansion quality is inherently uneven across keywords, not a bug in this plan's code.

**`requests` is not yet a project dependency** — installed during planning (`pip install requests` in the `StudyAssistant` conda env resolved `requests==2.34.2`) and must be added to `src/env.yml`.

---

## Task 1: `article_tags` schema, config, and the `requests` dependency

**Files:**
- Modify: `src/config.py`
- Modify: `src/db.py`
- Modify: `src/env.yml`
- Test: `tests/test_db.py`

**Interfaces:**
- Produces: `config.DATAMUSE_CACHE_PATH`; `db.py`'s `SCHEMA` gains an `article_tags(file_id, tag, source)` table, created by the existing `get_connection()`.

- [ ] **Step 1: Write the failing test**

Add to `tests/test_db.py` (alongside the existing `_column_names` helper and table tests):

```python
def test_creates_article_tags_table(tmp_path):
    conn = get_connection(tmp_path / "test.db")
    assert _column_names(conn, "article_tags") == {"file_id", "tag", "source"}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `conda run -n StudyAssistant python -m pytest tests/test_db.py::test_creates_article_tags_table -v`
Expected: FAIL — no such table.

- [ ] **Step 3: Add `DATAMUSE_CACHE_PATH` to `src/config.py`**

Append after the existing `TOKEN_FREQ_DIR` line:

```python
DATAMUSE_CACHE_PATH = DATA_DIR / "datamuse_cache.json"
```

- [ ] **Step 4: Add the `article_tags` table to `src/db.py`'s `SCHEMA`**

Insert before the closing `"""` of `SCHEMA`:

```python
CREATE TABLE IF NOT EXISTS article_tags (
    file_id TEXT NOT NULL REFERENCES file_info(file_id),
    tag TEXT NOT NULL,
    source TEXT NOT NULL CHECK(source IN ('intrinsic', 'expanded')),
    PRIMARY KEY (file_id, tag, source)
);
```

- [ ] **Step 5: Add `requests` to `src/env.yml`**

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
      - requests==2.34.2
```

- [ ] **Step 6: Run test to verify it passes**

Run: `conda run -n StudyAssistant python -m pytest tests/test_db.py -v`
Expected: PASS (all `test_db.py` tests, including the new one).

- [ ] **Step 7: Commit**

```bash
git add src/config.py src/db.py src/env.yml tests/test_db.py
git commit -m "Add article_tags schema, DATAMUSE_CACHE_PATH config, requests dependency"
```

---

## Task 2: Intrinsic keyword extraction with abstract-derived fallback

**Files:**
- Create: `src/modules/topic_tags.py`
- Test: `tests/test_topic_tags.py`

**Interfaces:**
- Consumes: `modules.word_freq.tokenize` (Phase 1), `modules.extract_text.extract_article` (Phase 1, for fixtures only).
- Produces: `extract_intrinsic_keywords(full_text: str, max_keywords: int = 12) -> list[str]`; `extract_abstract_text(full_text: str, max_chars: int = 2000) -> str | None`; `fallback_keywords_from_abstract(abstract_text: str, top_n: int = 5) -> list[str]` — all consumed by Task 4's `tag_article`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_topic_tags.py`:

```python
from config import ARTICLES_DIR
from modules.extract_text import extract_article
from modules.topic_tags import (
    extract_abstract_text,
    extract_intrinsic_keywords,
    fallback_keywords_from_abstract,
)


def test_extract_intrinsic_keywords_six_keyword_fixture():
    article = extract_article(
        ARTICLES_DIR / "A-closed-loop-control-strategy-for-automotive-EHB-base_2026_Control-Engineer.pdf"
    )
    assert extract_intrinsic_keywords(article.full_text) == [
        "EHB systems",
        "Hysteresis characteristics",
        "RC operator",
        "RBF neural network",
        "Closed-loop control",
        "Stability proof",
    ]


def test_extract_intrinsic_keywords_nine_keyword_fixture():
    article = extract_article(
        ARTICLES_DIR / "A-data-driven-approach-for-tailored-fatigue-modeling-_2026_Computers---Indus.pdf"
    )
    assert extract_intrinsic_keywords(article.full_text) == [
        "Physical fatigue",
        "Industrial engineering",
        "Wearable sensors",
        "Human factors",
        "Fuzzy model",
        "Fatigue-recovery model",
        "Data-driven approach",
        "Production planning and control",
        "Industry 5.0",
    ]


def test_extract_intrinsic_keywords_returns_empty_when_no_keywords_field():
    article = extract_article(
        ARTICLES_DIR / "Dynamic-modeling--linearization-and-characteristic-anal_2026_Chinese-Journal.pdf"
    )
    assert extract_intrinsic_keywords(article.full_text) == []


def test_extract_abstract_text_finds_plain_abstract_header():
    article = extract_article(
        ARTICLES_DIR / "Dynamic-modeling--linearization-and-characteristic-anal_2026_Chinese-Journal.pdf"
    )
    abstract = extract_abstract_text(article.full_text)
    assert abstract is not None
    assert abstract.startswith(
        "This study investigates on the Drag-Free and Attitude Control System"
    )


def test_extract_abstract_text_returns_none_when_absent():
    assert extract_abstract_text("no abstract header anywhere in this text") is None


def test_fallback_keywords_from_abstract_ranks_by_frequency():
    article = extract_article(
        ARTICLES_DIR / "Dynamic-modeling--linearization-and-characteristic-anal_2026_Chinese-Journal.pdf"
    )
    abstract = extract_abstract_text(article.full_text)
    assert fallback_keywords_from_abstract(abstract, top_n=5) == [
        "model",
        "system",
        "dynam",
        "linear",
        "analysi",
    ]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `conda run -n StudyAssistant python -m pytest tests/test_topic_tags.py -v`
Expected: FAIL — `modules.topic_tags` doesn't exist.

- [ ] **Step 3: Write `src/modules/topic_tags.py`**

```python
import re

from modules.word_freq import tokenize

_KEYWORDS_HEADER_RE = re.compile(r"[Kk]eywords?\s*[:\-]?\s*\n")
_ABSTRACT_HEADER_RE = re.compile(r"\n\s*[Aa]\s*[Bb]\s*[Ss]\s*[Tt]\s*[Rr]\s*[Aa]\s*[Cc]\s*[Tt]\s*\n")


def extract_intrinsic_keywords(full_text: str, max_keywords: int = 12) -> list[str]:
    """Extracts the article's own Keywords field.

    ScienceDirect PDFs list one keyword per line, immediately followed by
    an "abstract"/"a b s t r a c t" header line -- verified against all 280
    real PDFs in articles/, not the semicolon-separated "term1; term2"
    shape the design spec assumed before checking real extracted text.
    """
    match = _KEYWORDS_HEADER_RE.search(full_text)
    if match is None:
        return []

    keywords: list[str] = []
    for line in full_text[match.end():].split("\n"):
        stripped = line.strip()
        if not stripped or stripped.replace(" ", "").lower() == "abstract":
            break
        keywords.append(stripped)
        if len(keywords) >= max_keywords:
            break
    return keywords


def extract_abstract_text(full_text: str, max_chars: int = 2000) -> str | None:
    """Extracts the text immediately following the Abstract header, if present."""
    match = _ABSTRACT_HEADER_RE.search(full_text)
    if match is None:
        return None
    return full_text[match.end():match.end() + max_chars].strip()


def fallback_keywords_from_abstract(abstract_text: str, top_n: int = 5) -> list[str]:
    """Ranks the abstract's tokenize() output by frequency as a stand-in for
    "top TF-IDF terms" -- the corpus-wide tf_idf SQL table doesn't exist yet
    at this pipeline stage (Phase 2's C++ engine builds it after topic
    tagging runs), so this uses local term frequency only."""
    freq = tokenize(abstract_text)
    ranked = sorted(freq.items(), key=lambda item: (-item[1], item[0]))
    return [word for word, _ in ranked[:top_n]]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `conda run -n StudyAssistant python -m pytest tests/test_topic_tags.py -v`
Expected: PASS (6 passed).

- [ ] **Step 5: Commit**

```bash
git add src/modules/topic_tags.py tests/test_topic_tags.py
git commit -m "Add intrinsic keyword extraction with abstract-derived fallback"
```

---

## Task 3: Datamuse client with disk-backed cache

**Files:**
- Create: `src/modules/datamuse.py`
- Test: `tests/test_datamuse.py`

**Interfaces:**
- Produces: `load_cache(cache_path: Path) -> dict[str, list[str]]`; `save_cache(cache: dict[str, list[str]], cache_path: Path) -> None`; `expand_keyword(keyword: str, cache: dict[str, list[str]], limit: int = 8, request_delay: float = 0.1) -> list[str]` — all consumed by Task 4's `tag_article`/`tag_all_articles`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_datamuse.py`:

```python
import json

from modules.datamuse import expand_keyword, load_cache, save_cache


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


def test_expand_keyword_calls_datamuse_and_populates_cache(monkeypatch):
    calls = []

    def fake_get(url, params, timeout):
        calls.append(params)
        return _FakeResponse([{"word": "operate"}, {"word": "command"}])

    monkeypatch.setattr("modules.datamuse.requests.get", fake_get)
    monkeypatch.setattr("modules.datamuse.time.sleep", lambda seconds: None)

    cache = {}
    result = expand_keyword("control", cache, limit=8)

    assert result == ["operate", "command"]
    assert cache["control"] == ["operate", "command"]
    assert calls == [{"ml": "control", "max": 8}]


def test_expand_keyword_uses_cache_without_calling_network(monkeypatch):
    def fake_get(*args, **kwargs):
        raise AssertionError("should not call network when cache hit")

    monkeypatch.setattr("modules.datamuse.requests.get", fake_get)

    cache = {"control": ["operate", "command"]}
    result = expand_keyword("Control", cache)  # different case, same normalized key

    assert result == ["operate", "command"]


def test_expand_keyword_returns_empty_list_on_request_failure(monkeypatch):
    import requests

    def fake_get(*args, **kwargs):
        raise requests.RequestException("boom")

    monkeypatch.setattr("modules.datamuse.requests.get", fake_get)
    monkeypatch.setattr("modules.datamuse.time.sleep", lambda seconds: None)

    cache = {}
    result = expand_keyword("control", cache)

    assert result == []
    assert cache["control"] == []


def test_load_and_save_cache_round_trip(tmp_path):
    cache_path = tmp_path / "datamuse_cache.json"
    save_cache({"control": ["operate", "command"]}, cache_path)

    loaded = load_cache(cache_path)

    assert loaded == {"control": ["operate", "command"]}
    assert json.loads(cache_path.read_text(encoding="utf-8")) == {
        "control": ["operate", "command"]
    }


def test_load_cache_returns_empty_dict_when_file_missing(tmp_path):
    assert load_cache(tmp_path / "missing.json") == {}


def test_expand_keyword_real_network_smoke_test():
    """One real call against the live Datamuse API to verify the actual
    integration works end-to-end, not just the mocked call shape. Asserts
    structure (non-empty list of strings) rather than exact words/scores --
    unlike this corpus's deterministic local PDF/NLTK pipeline, a live
    third-party API's index isn't ours to pin exact values against."""
    cache = {}
    result = expand_keyword("control", cache, limit=5)

    assert len(result) >= 1
    assert all(isinstance(word, str) and word for word in result)
    assert cache["control"] == result
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `conda run -n StudyAssistant python -m pytest tests/test_datamuse.py -v`
Expected: FAIL — `modules.datamuse` doesn't exist.

- [ ] **Step 3: Write `src/modules/datamuse.py`**

```python
import json
import time
from pathlib import Path

import requests

DATAMUSE_URL = "https://api.datamuse.com/words"


def load_cache(cache_path: Path) -> dict[str, list[str]]:
    if not cache_path.exists():
        return {}
    return json.loads(cache_path.read_text(encoding="utf-8"))


def save_cache(cache: dict[str, list[str]], cache_path: Path) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(
        json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def expand_keyword(
    keyword: str,
    cache: dict[str, list[str]],
    limit: int = 8,
    request_delay: float = 0.1,
) -> list[str]:
    """Returns Datamuse's "means like" related words for a keyword, reusing
    `cache` (keyed by lowercased keyword) across calls so articles sharing
    common domain vocabulary -- verified common in this corpus, e.g. "model
    predictive control" -- never re-hit the network for the same term."""
    key = keyword.strip().lower()
    if key in cache:
        return cache[key]

    try:
        response = requests.get(DATAMUSE_URL, params={"ml": key, "max": limit}, timeout=10)
        response.raise_for_status()
        related = [item["word"] for item in response.json()]
    except requests.RequestException:
        related = []

    cache[key] = related
    time.sleep(request_delay)
    return related
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `conda run -n StudyAssistant python -m pytest tests/test_datamuse.py -v`
Expected: PASS (6 passed). The real-network smoke test makes an actual HTTPS call — expect a second or two, not a hang; if it fails, check network connectivity before assuming a code bug.

- [ ] **Step 5: Commit**

```bash
git add src/modules/datamuse.py tests/test_datamuse.py
git commit -m "Add Datamuse client with disk-backed cache for keyword expansion"
```

---

## Task 4: Tagging orchestration (`tag_article`, `tag_all_articles`)

**Files:**
- Create: `src/tag_topics.py`
- Test: `tests/test_tag_topics.py`

**Interfaces:**
- Consumes: `extract_intrinsic_keywords`/`extract_abstract_text`/`fallback_keywords_from_abstract` (Task 2), `expand_keyword`/`load_cache`/`save_cache` (Task 3), `db.get_connection`, `config.DATAMUSE_CACHE_PATH`/`config.DB_PATH`.
- Produces: `tag_article(file_id: str, full_text: str, conn: sqlite3.Connection, cache: dict[str, list[str]]) -> None`; `tag_all_articles(conn: sqlite3.Connection, cache_path: Path = DATAMUSE_CACHE_PATH) -> int` (returns count of newly-tagged articles); a `__main__` entrypoint mirroring `src/ingest.py`'s.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_tag_topics.py`:

```python
from config import ARTICLES_DIR
from db import get_connection
from ingest import ingest_pdf
from tag_topics import tag_all_articles, tag_article

FIXTURE_WITH_KEYWORDS = (
    ARTICLES_DIR / "A-closed-loop-control-strategy-for-automotive-EHB-base_2026_Control-Engineer.pdf"
)
FIXTURE_NO_KEYWORDS = (
    ARTICLES_DIR / "Dynamic-modeling--linearization-and-characteristic-anal_2026_Chinese-Journal.pdf"
)


def _fake_expand_keyword(keyword, cache, limit=8, request_delay=0.0):
    cache[keyword] = [f"{keyword}-related"]
    return cache[keyword]


def test_tag_article_writes_intrinsic_and_expanded_tags(tmp_path, monkeypatch):
    monkeypatch.setattr("tag_topics.expand_keyword", _fake_expand_keyword)

    conn = get_connection(tmp_path / "test.db")
    file_id = ingest_pdf(FIXTURE_WITH_KEYWORDS, conn, token_freq_dir=tmp_path / "token_freq")
    full_text = conn.execute(
        "SELECT full_text FROM article_text WHERE file_id = ?", (file_id,)
    ).fetchone()[0]

    tag_article(file_id, full_text, conn, cache={})

    intrinsic = {
        row[0]
        for row in conn.execute(
            "SELECT tag FROM article_tags WHERE file_id = ? AND source = 'intrinsic'",
            (file_id,),
        ).fetchall()
    }
    assert intrinsic == {
        "ehb systems",
        "hysteresis characteristics",
        "rc operator",
        "rbf neural network",
        "closed-loop control",
        "stability proof",
    }

    expanded = {
        row[0]
        for row in conn.execute(
            "SELECT tag FROM article_tags WHERE file_id = ? AND source = 'expanded'",
            (file_id,),
        ).fetchall()
    }
    assert expanded == {f"{kw}-related" for kw in intrinsic}


def test_tag_article_falls_back_when_no_keywords_field(tmp_path, monkeypatch):
    monkeypatch.setattr("tag_topics.expand_keyword", _fake_expand_keyword)

    conn = get_connection(tmp_path / "test.db")
    file_id = ingest_pdf(FIXTURE_NO_KEYWORDS, conn, token_freq_dir=tmp_path / "token_freq")
    full_text = conn.execute(
        "SELECT full_text FROM article_text WHERE file_id = ?", (file_id,)
    ).fetchone()[0]

    tag_article(file_id, full_text, conn, cache={})

    intrinsic = {
        row[0]
        for row in conn.execute(
            "SELECT tag FROM article_tags WHERE file_id = ? AND source = 'intrinsic'",
            (file_id,),
        ).fetchall()
    }
    assert intrinsic == {"model", "system", "dynam", "linear", "analysi"}


def test_tag_all_articles_skips_already_tagged_files(tmp_path, monkeypatch):
    monkeypatch.setattr("tag_topics.expand_keyword", _fake_expand_keyword)

    conn = get_connection(tmp_path / "test.db")
    ingest_pdf(FIXTURE_WITH_KEYWORDS, conn, token_freq_dir=tmp_path / "token_freq")

    first_count = tag_all_articles(conn, cache_path=tmp_path / "cache.json")
    second_count = tag_all_articles(conn, cache_path=tmp_path / "cache.json")

    assert first_count == 1
    assert second_count == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `conda run -n StudyAssistant python -m pytest tests/test_tag_topics.py -v`
Expected: FAIL — `tag_topics` doesn't exist.

- [ ] **Step 3: Write `src/tag_topics.py`**

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `conda run -n StudyAssistant python -m pytest tests/test_tag_topics.py -v`
Expected: PASS (3 passed).

- [ ] **Step 5: Commit**

```bash
git add src/tag_topics.py tests/test_tag_topics.py
git commit -m "Add topic-tagging orchestration: tag_article, tag_all_articles, CLI entrypoint"
```

---

## Task 5: End-to-end real-corpus test with real Datamuse calls

**Files:**
- Test: `tests/test_end_to_end_topic_tagging.py`

**Interfaces:**
- Consumes: every function from Tasks 2–4, against real PDFs and the live Datamuse API (no mocking) — this is the one test in this plan that exercises the genuine, non-deterministic external dependency end-to-end, mirroring how Phase 2's Task 6 validated composition across all its stages on the real 8-article fixture corpus.

- [ ] **Step 1: Write the failing test**

Create `tests/test_end_to_end_topic_tagging.py`:

```python
from config import ARTICLES_DIR
from db import get_connection
from ingest import ingest_pdf
from tag_topics import tag_all_articles

FIXTURE_FILES = [
    "Bifurcation-analysis-for-a-COVID-19-cell-model-with-pa_2026_Alexandria-Engin.pdf",
    "Reduced-order-echo-state-networks-for-model-predictive_2026_European-Journal.pdf",
    "A-closed-loop-control-strategy-for-automotive-EHB-base_2026_Control-Engineer.pdf",
    "A-dual-coupled-hysteresis-model-and-hybrid-control-strategy-for-_2026_Measur.pdf",
    "A-fractional-order-mathematical-model-for-malaria-transmission-i_2026_Frankl.pdf",
    "A-frequency-interval-criterion-for-modeling-secondary_2026_Journal-of-Sound-.pdf",
    "A-hybrid-calibrated-improved-dynamic-wake-meandering-model-for_2026_Ocean-En.pdf",
    "A-hybrid-hierarchical-synergistic-control-framework-for-i_2026_Advanced-Engi.pdf",
]


def test_full_topic_tagging_pipeline_on_real_corpus_with_real_datamuse(tmp_path):
    conn = get_connection(tmp_path / "studyapp.db")
    for filename in FIXTURE_FILES:
        ingest_pdf(ARTICLES_DIR / filename, conn, token_freq_dir=tmp_path / "token_freq")

    tagged_count = tag_all_articles(conn, cache_path=tmp_path / "cache.json")
    assert tagged_count == 8

    intrinsic_count = conn.execute(
        "SELECT COUNT(*) FROM article_tags WHERE source = 'intrinsic'"
    ).fetchone()[0]
    assert intrinsic_count >= 8 * 3  # every fixture has at least 3 verified intrinsic keywords

    expanded_count = conn.execute(
        "SELECT COUNT(*) FROM article_tags WHERE source = 'expanded'"
    ).fetchone()[0]
    assert expanded_count > 0

    conengprac_tags = {
        row[0]
        for row in conn.execute(
            "SELECT tag FROM article_tags WHERE file_id = ? AND source = 'intrinsic'",
            ("10.1016/j.conengprac.2026.106951",),
        ).fetchall()
    }
    assert conengprac_tags == {
        "ehb systems",
        "hysteresis characteristics",
        "rc operator",
        "rbf neural network",
        "closed-loop control",
        "stability proof",
    }
```

- [ ] **Step 2: Run the test to verify it fails, then passes**

Run: `conda run -n StudyAssistant python -m pytest tests/test_end_to_end_topic_tagging.py -v`
Expected before Tasks 1–4: FAIL (missing modules). After: PASS (1 passed) — ingests 8 real PDFs and makes real Datamuse calls for each unique keyword across them, so expect the slowest single test in the suite (multiple real HTTPS round-trips), not a hang.

- [ ] **Step 3: Run the full test suite together**

Run: `conda run -n StudyAssistant python -m pytest tests/ -v`
Expected: PASS — Phase 1 + Phase 2's existing tests plus every Phase 3 test from Tasks 1–5.

- [ ] **Step 4: Commit**

```bash
git add tests/test_end_to_end_topic_tagging.py
git commit -m "Add end-to-end topic-tagging test: real 8-article corpus through real Datamuse API"
```

---

## Self-Review

**Spec coverage:**
- "Extract the article's own Keywords field" → Task 2's `extract_intrinsic_keywords`, verified against 280 real PDFs (regex corrected from the spec's assumed semicolon format to the real newline-separated shape). ✓
- "Fallback to top TF-IDF terms from the Abstract if no clean Keywords line is found" → Task 2's `extract_abstract_text` + `fallback_keywords_from_abstract`, with the TF-IDF-vs-local-frequency simplification explicitly documented (Global Constraints). ✓
- "Expand each article's intrinsic keywords via Datamuse... seeded from real per-article keywords instead of random Wikipedia topics" → Task 3's `expand_keyword` (same `ml=`/`api.datamuse.com` mechanism as V2's `get_related_topics`, verified by reading V2's `src/modules/word_freq.py`), wired to real keywords in Task 4. ✓
- "New `article_tags` table — intrinsic + expanded keyword tags per article" → Task 1's schema, exact shape from the design spec's consolidated schema table. ✓
- "Scoped to article -> tags only (no topic-to-topic similarity/clustering)" → no `tags`/`tags_full`/`topic_similarity` table anywhere in this plan; confirmed out of scope in Global Constraints. ✓
- Phase 2's plan explicitly deferred "topic tagging... planned as Phase 3" — this plan is that deferred work. ✓

**Verified-not-assumed claims:** every regex, fixture value, and API response shape in this plan was produced by actually running extraction code against real PDFs in `articles/` and real HTTPS calls to Datamuse during planning (see Ground-truth fixtures) — including discovering that the design spec's assumed Keywords format (semicolon-separated) doesn't match any real PDF in the corpus, and that 3/280 PDFs are pre-proof covers lacking a Keywords field but still containing a real Abstract.

**Placeholder scan:** none — all regexes, fixture values, and code are concrete and have been executed for real, not estimated.

**Type/interface consistency:** `extract_intrinsic_keywords`/`extract_abstract_text`/`fallback_keywords_from_abstract` (Task 2) are declared once and consumed identically by Task 4's `tag_article`. `expand_keyword`/`load_cache`/`save_cache` (Task 3) are declared once and consumed identically by Task 4. `tag_article`/`tag_all_articles` (Task 4) are consumed identically by Task 5's end-to-end test.

## Out of scope for this phase

- **Topic-to-topic similarity/expansion** (V2's `topicSimilarity`/`expand_degree`) — explicitly out of scope per the design spec; no such table in this plan's schema.
- **Incremental-rebuild semantics for `comparison`/`relation_distance_filtered`, orchestration/`.bat` replacement tying Python ingest + tagging + C++ stages into one entrypoint, and the known `ingest_pdf` file_id-only idempotency gap** — all Phase 4, already deferred by Phase 1's and Phase 2's plans. This plan's own `tag_all_articles` per-file "skip already-tagged" check is a narrow idempotency convenience for this stage only, not a substitute for Phase 4's broader incremental design.
- **Using `article_tags` in retrieval scoring** — per the design spec, topic tagging is article-facing metadata only; the C++ comparison/TF-IDF engine (Phase 2) never reads this table.
- **Rejoining multi-line-wrapped keyword fragments** (documented limitation in Ground-truth fixtures) — rare in the real corpus, and each fragment still carries usable topical signal; not worth the added complexity.
