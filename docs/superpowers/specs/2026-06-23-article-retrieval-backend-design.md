# StudyApp V3 — Article Retrieval Backend Design

## Background

V2 (`StudyApp_V2_May18_2024`) is a working PyQt5 desktop app + C++/Python pipeline
built around reference books, using SQLite (not ChromaDB — despite docs and
`env.yml` claiming otherwise, a full audit confirmed zero ChromaDB/LangChain usage
anywhere in the actual code).

V3 (`StudyApp_V3_Dec28_2024`) was found, on audit, to be an **older snapshot of V2**,
not a forward-looking redesign — it predates V2's `recommend.hpp`, topic tagging,
`.env` support, MD5-based file IDs, and the entire `app/` GUI. Neither V3's
`PROMPT.txt` (sample essay text, not a spec) nor its `README.md` (a pre-V2-era
planning doc still talking about "PDF files" and "reading entries") mentions
articles or ScienceDirect anywhere. The only place the article-pivot goal was
written down was one sentence in `articles/README.md`.

This spec defines a **clean-slate redesign** of V3, using V2/V3's code as
reference material only (not as a base to copy from), tuned specifically for
academic articles instead of reference books.

## Goals

- A **retrieval backend/library** (no GUI) that other tools — primarily the
  Controller-Toolbox project's case-study research — can call to find relevant
  articles from a shared corpus at `../articles/`.
- Two retrieval modes:
  1. **Item-to-item similarity**: given an article, return the most similar others.
  2. **Prompt-based ranking**: given a free-text query, return the most relevant articles.
- Scale target: corpus growing toward **~10,000 articles** (currently 99, real
  control-engineering/MPC/robotics papers already in `articles/`).
- Keep the Python/C++ split V2 used (Python: orchestration, extraction,
  tokenization, topic tagging; C++: relational distance, TF-IDF, pairwise
  comparison), bridged via file/SQLite handoff — no pybind11. This is an explicit
  choice to keep the proven, simpler-to-reason-about pattern at scale, not an
  oversight.

## Inputs (ground truth, not assumed)

`articles/` already contains 99 real PDFs (control engineering / MPC / robotics
domain — consistent with Controller-Toolbox's case-study needs). Confirmed
characteristics:
- PDF files only, no sidecar metadata (no JSON/CSV with title, authors, keywords, etc.).
- Filenames are **truncated and sanitized** (e.g.
  `ReGA-PINN--A-physics-informed-neural-network-approach-for-dyna_2026_Ocean-En.pdf`)
  — useful as a human-readable label, but not a reliable structured data source.
  Title, authors, abstract, and keywords must be extracted from the PDF content
  itself, not inferred from the filename.

## Architecture

```
articles/*.pdf
   │
   ▼ (Python) extract — PyMuPDF (fitz), one row per article (no book-style
   │            multi-chunk sliding window — articles are short enough that
   │            chunking isn't needed by default)
   ▼ (Python) tokenize — NLTK + stopword filtering + Porter stemming
   │            (unchanged from V2 — not book/article-specific)
   ▼ (Python) topic tagging — extract intrinsic Keywords field (regex/heuristic
   │            on extracted text; Abstract fallback if no clean Keywords line),
   │            expand via Datamuse for cross-article topic clustering
   │
   │  [Python → C++ handoff via SQLite + JSON token-frequency files]
   ▼
   ▼ (C++) relational distance + TF-IDF — same metrics as V2, but thresholds
   │            (e.g. V2's hardcoded `frequency >= 30` filter) become config
   │            parameters scaled to article-length documents instead of
   │            whole-book token counts
   ▼ (C++) pairwise comparison — single canonical engine (see "Removed from V2"
                below for why there's only one, not two)
```

## Two retrieval entry points

1. **Item-to-item similarity** — V2 had two parallel, never-reconciled
   implementations (Python's fast-approximate `item_matrix`, capped to the
   top-1000 tokens; C++'s exhaustive `comparison`, the one V2's GUI actually
   trusted). V3 **standardizes on one engine**: the C++ exhaustive comparison.
   The Python fast-approximate path is dropped — its reason to exist (avoiding
   slow exhaustive computation) is much less relevant once incremental builds
   are in place (see below), and a single engine removes an entire class of
   "two tables that disagree" bugs V2 had.
2. **Prompt-based ranking** — tokenize a free-text query the same way as
   documents, score articles by relevance against the precomputed TF-IDF table,
   return ranked results. Reuses the TF-IDF table already built for item-to-item
   comparison; no separate index needed. Mirrors V2's `processPrompt`.

## Incremental builds (critical at this scale)

V2's real-world timing: a couple of hours for a from-scratch full rebuild,
a few minutes for an incremental add of new files, ~11 seconds for prompt-based
ranking. V3 must preserve and formalize this asymmetry — at 10,000 articles, a
full O(n²) rebuild will almost certainly take meaningfully longer than V2's
book-corpus experience (pair count grows quadratically with article count, even
though each article's token vector is much smaller than a book's). Incremental
updates are the realistic day-to-day operating mode, not a nice-to-have:

- When new articles are added to an existing corpus, only new-vs-existing and
  new-vs-new pairs are computed — never re-pairing the whole existing corpus
  (formalizing V2's `skim_files` pattern).
- A full rebuild must be resumable if interrupted (carrying forward V2's
  `low_similarity.txt`-style checkpoint pattern), not restart from zero.
- The actual full-rebuild and incremental-rebuild timings at 10,000 articles are
  **unverified** — this needs empirical validation once the corpus grows
  meaningfully past 99, not assumed from V2's book-corpus numbers.

## Topic tagging

Scoped to **article → tags** only (no topic-to-topic similarity/clustering —
deliberately left out of scope; V2 had this via `expand_degree`/iterative
expansion, but nothing has asked for it and it adds a topic-similarity graph
for a capability not currently needed):

1. Extract the article's own Keywords field (heuristic pattern match on
   extracted PDF text, e.g. `Keywords: term1; term2; term3`).
2. Fallback to top TF-IDF terms from the Abstract if no clean Keywords line is found.
3. Expand each article's intrinsic keywords via Datamuse (related-words API)
   for broader cross-article topic clustering — same mechanism V2 used, seeded
   from real per-article keywords instead of random Wikipedia topics.

## Database schema (consolidated from V2's 11 tables)

| Table | Change vs. V2 |
|---|---|
| `file_info` | Kept, but ID generation moves off "MD5 of file *path*" (V2's scheme — renaming/moving a file silently churns its ID) to a content-hash or DOI-based ID where extractable. |
| `article_text` | Renamed from `pdf_chunks` — one row per article instead of many chunk rows, since per-chunk storage existed in V2 to support book-length documents and GUI content search, neither of which applies here. |
| `relation_distance_filtered`, `tf_idf` | Kept; thresholds become config parameters instead of hardcoded constants. |
| `comparison` | The single canonical item-to-item table (V2's `item_matrix` dropped). |
| `article_tags` | New — intrinsic + expanded keyword tags per article. |
| ~~`item_matrix`~~ | Dropped (Python fast-approximate path removed). |
| ~~`tags`/`tags_full`/`topic_similarity`~~ | Dropped (topic-to-topic graph out of scope). |
| ~~`item_matrix_filtered`~~ | Dropped (was dead code in V2 — never populated). |

## Explicitly out of scope / removed from V2

- PyQt5 GUI (`app/`) — V3 is backend/library only.
- Markdown notes-export workflow.
- Ollama chat feature (`ideation.py`) — was already fully disconnected from V2's pipeline.
- ChromaDB/LangChain/LangGraph dependencies — confirmed dead code in V2, never used.
- Docker setup — not needed for a library consumed in-process by another local project.
- Topic-to-topic similarity/expansion (see Topic tagging above).
- Python fast-approximate `item_matrix` engine (see retrieval entry points above).

## V2 bugs/fragility this redesign avoids by construction

- Broken Python `tf_idf.py` (queried a nonexistent table) — moot, that whole path is dropped.
- Inverted success/failure messages in `conda_activate.bat`/`conda_deactivate.bat` — moot, orchestration is written fresh, not copied.
- 4+ inconsistent hardcoded `D:\READING LIST`-style paths across files — fixed via one config source instead of per-language defaults that can drift independently.
- MD5-of-path file IDs churning on rename/move — fixed via content-hash/DOI-based IDs.
- Docs describing a fictional schema that never matched the code — avoided by writing docs after the implementation exists, not as an aspirational "build report."

## Testing & validation

No labeled "correct recommendations" dataset exists, so automated tests focus on
what's mechanically verifiable:
- Extraction correctness (a given PDF produces expected text/keyword fields) —
  using the real 99 papers already in `articles/` as fixtures.
- Tokenization correctness.
- Schema/pipeline integration across the full extract → tokenize → distance →
  TF-IDF → comparison flow on a fixture subset.
- Incremental-vs-full-rebuild equivalence: adding articles incrementally must
  produce the same `comparison` scores a full rebuild would.

"Good recommendations" itself requires a manual spot-check against the 99 real
papers once the pipeline runs end-to-end — there's no ground-truth relevance
labeling, so this is a known gap that can't be closed at design time.

## Open risks / unknowns (flagged, not guessed at)

- Exact threshold values (relational-distance frequency cutoff, TF-IDF
  parameters) need empirical tuning against the real 99-paper corpus — the spec
  defines them as config-driven, not what their values should be.
- Reliability of the Keywords-field extraction heuristic against real
  ScienceDirect PDF layouts is unverified until run against the actual 99 PDFs.
- Full-rebuild and incremental-rebuild timing at scale (thousands of articles)
  is unverified — V2's timing data is from a different corpus shape (fewer,
  much longer documents) and may not extrapolate linearly.
