# Python Modules

The Python side handles data preparation and orchestration: it extracts text from
PDFs, topic-tags them, populates `data/studyapp.db`, and drives the C++ engine.
All sources are in [../src/](../src/).

## Entry points

| Module | Role |
|--------|------|
| [pipeline.py](../src/pipeline.py) | Orchestrates a full build: `ingest → tag → C++ (relational distance → TF-IDF → comparison)`. `--full` forces a full rebuild; default is incremental. This is what `setup.bat`/`setup.sh` invoke. |
| [query.py](../src/query.py) | Query time: reads a prompt file (default `PROMPT.txt`), tokenizes it, runs the engine's `--process-prompt`, and prints a ranked, readable article list. This is what `run.bat`/`run.sh` invoke. Options: `--top N`, `--all`, `--db`. |
| [setup_nltk.py](../src/setup_nltk.py) | Downloads the NLTK corpora the tokenizer needs: `punkt`, `punkt_tab`, `stopwords`. Run once before the first build. |

Each of `ingest.py`, `tag_topics.py`, and `pipeline.py` is also runnable directly
as a script (`python src/<name>.py`) for running a single stage.

## Core modules

| Module | Role |
|--------|------|
| [config.py](../src/config.py) | Central paths: `ARTICLES_DIR` (the sibling `../articles/` corpus), `DB_PATH`, `TOKEN_FREQ_DIR`, `DATAMUSE_CACHE_PATH`. |
| [db.py](../src/db.py) | SQLite schema and `get_connection()`. Tables: `file_info`, `article_text`, `article_tags`, `relation_distance_filtered`, `tf_idf`, `comparison`, `comparison_seen`. |
| [ingest.py](../src/ingest.py) | Walks `*.pdf` in the corpus, extracts text, writes a per-article token-frequency JSON, and upserts `file_info` + `article_text`. Skips unchanged files (content SHA-256) and purges stale rows for changed/moved files. |
| [tag_topics.py](../src/tag_topics.py) | For each untagged article: pulls intrinsic keywords, then expands them via Datamuse, writing `intrinsic` and `expanded` rows into `article_tags`. |

## `modules/` helpers

| Module | Role |
|--------|------|
| [extract_text.py](../src/modules/extract_text.py) | PyMuPDF (`fitz`) text extraction. Derives `file_id` from the page-1 DOI when present (else `sha256:<hash>`), NFKC-normalizes text, records SHA-256 and page count. |
| [word_freq.py](../src/modules/word_freq.py) | `tokenize()`: lowercase, strip non-word chars, NLTK word-tokenize, drop stopwords (NLTK list + extras) and non-alpha/garbage tokens, Porter-stem, and count. Returns `{token: count}`. |
| [tokenize_prompt.py](../src/modules/tokenize_prompt.py) | Thin pass-through to `word_freq.tokenize()` so query text is tokenized identically to article text (used to build `--process-prompt` query files). |
| [topic_tags.py](../src/modules/topic_tags.py) | Keyword extraction: `extract_intrinsic_keywords()` reads the article's own Keywords field; falls back to top abstract terms (`extract_abstract_text` + `fallback_keywords_from_abstract`) when absent. |
| [datamuse.py](../src/modules/datamuse.py) | `expand_keyword()`: Datamuse "means-like" lookups with a JSON cache (`data/datamuse_cache.json`). Network failures degrade to no expansion rather than raising. |

## Data flow

```
PDF ─► extract_text.extract_article ─► word_freq.tokenize ─► data/token_freq/*.json
                    │                                              │
                    └─────────────► file_info / article_text ◄─────┘   (ingest.py)
                                          │
              topic_tags + datamuse ─► article_tags                    (tag_topics.py)
                                          │
                                          ▼
                              C++ retrieval_engine                     (see CPP.md)
```
