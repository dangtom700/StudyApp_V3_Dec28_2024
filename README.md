# StudyApp V3

A content-based article retrieval engine for a corpus of academic PDFs. A Python
stage ingests and topic-tags the PDFs into a SQLite database; a C++ engine then
does the heavy numeric work (relational distance, TF-IDF, pairwise comparison,
and prompt ranking). You build the index once, then query it from the command
line with a free-text prompt.

## Overview

```
../articles/*.pdf
      │
      ▼  (Python: src/pipeline.py)
  ingest ──► tag topics ──► data/studyapp.db  ─┐
                                               │  (C++: build/retrieval_engine.exe)
                                               ├─► relational distance
                                               ├─► TF-IDF
                                               └─► pairwise comparison
      │
      ▼  query time
  prompt ──► retrieval_engine --process-prompt ──► ranked article list
```

- The Python side lives in [src/](src/); the C++ engine sources are in [src/cpp/](src/cpp/).
- All state is a single SQLite file, `data/studyapp.db` (schema in [src/db.py](src/db.py)).
- Builds are **incremental** by default — re-running only processes new/changed PDFs.

## Prerequisites

1. **Conda environment** `StudyAssistant` (Python 3.12 + PyMuPDF, NLTK, requests,
   pytest), defined in [src/env.yml](src/env.yml):
   ```powershell
   conda env create -f src/env.yml
   ```
2. **C++ toolchain — MSYS2 UCRT64.** The build expects it at `C:\msys64\ucrt64`
   (MinGW g++ + SQLite3). This path is hardcoded in [setup.bat](setup.bat),
   [setup.sh](setup.sh), and [src/pipeline.py](src/pipeline.py) — see
   [Notes & gotchas](#notes--gotchas) if yours differs.
3. **CMake** on your `PATH`.
4. **The corpus.** PDFs must live in `../articles/` *relative to this repo root*
   (i.e. a sibling `articles/` folder, `C:\Users\dangs\Desktop\project\articles\`).
   This location is set by `ARTICLES_DIR` in [src/config.py](src/config.py).
   Only `*.pdf` files are ingested.

## Quick start

From the repo root:

```powershell
.\setup.bat            # incremental build (default)
.\setup.bat --full     # force a full rebuild from scratch
```

(`./setup.sh` is the bash equivalent.) The script builds the C++ engine, downloads
the required NLTK corpora, and runs the full pipeline. On a real corpus this is
**slow and mostly silent for long stretches — that is normal, not a hang.**

## What the build produces

Everything lands under [data/](data/):

| Path | Contents |
|------|----------|
| `data/studyapp.db` | SQLite DB: file info, article text, topic tags, relational distances, TF-IDF, pairwise comparison |
| `data/token_freq/*.json` | Per-article `{token: count}` frequency maps |
| `data/datamuse_cache.json` | Cached Datamuse "means-like" keyword expansions |

## Querying the corpus

Once the index is built (`setup.bat`), querying is two steps — no terminal typing
of the prompt itself, so it can be as long as an essay:

1. **Edit [PROMPT.txt](PROMPT.txt)** and paste your query into it. The whole file
   is the prompt; lines starting with `#` are ignored (handy for notes).
2. **Run it:**
   ```powershell
   .\run.bat            # ranks the whole corpus against PROMPT.txt
   .\run.bat --top 10   # show only the top 10 (default 20; --all for everything)
   ```
   (`./run.sh` is the bash equivalent.)

`run.bat` calls [src/query.py](src/query.py), which tokenizes the prompt the same
way articles are tokenized, runs the engine's `--process-prompt`, and prints a
ranked, readable list:

```
Prompt: PROMPT.txt

Rank    Score  Article
----  -------  ------------------------------------------------
   1   0.6388  Reduced-order-echo-state-networks-for-model-predictive_2026_European-Journal.pdf
   2   0.5975  Self-loop-physics-informed-neural-network-for-model-pre_2026_Journal-of-Proc.pdf
   ...
```

To query a different file, pass it as an argument: `run.bat some-other-prompt.txt`.

<details>
<summary>Calling the engine directly (what <code>query.py</code> wraps)</summary>

```powershell
# Tokenize a prompt into a query file, then rank against it. The UCRT64 bin dir
# must be on PATH so the engine finds SQLite3.dll (query.py does this for you).
$env:PATH = 'C:\msys64\ucrt64\bin;' + $env:PATH
.\build\retrieval_engine.exe --process-prompt data\studyapp.db query.json
```

Raw engine output is TSV, `<file_id>\t<score>`, best first. `file_id` is the
article's DOI when one is found on page 1, otherwise `sha256:<hash>`.
</details>

## Running the tests

```powershell
conda run -n StudyAssistant pytest
```

`pyproject.toml` puts `src/` on the import path and points pytest at `tests/`.
The C++ tests build and drive the engine against real PDF fixtures from
`../articles/`.

## Project layout

```
setup.bat / setup.sh    One-time build: compile engine + setup NLTK + run pipeline
run.bat / run.sh        Query: rank the corpus against PROMPT.txt
PROMPT.txt              Your query text (edit this, then run.bat)
CMakeLists.txt          Build definition for the C++ retrieval_engine
src/
  config.py             Central paths (corpus, DB, data dir)
  db.py                 SQLite schema + connection
  ingest.py             PDF → text/token-freq → DB
  tag_topics.py         Keyword extraction + Datamuse expansion → article_tags
  pipeline.py           Orchestrates ingest + tag + C++ stages (setup.bat runs this)
  query.py              Tokenize PROMPT.txt → engine --process-prompt → ranked list
  setup_nltk.py         Downloads NLTK corpora (punkt, punkt_tab, stopwords)
  modules/              extract_text, word_freq, tokenize_prompt, topic_tags, datamuse
  cpp/                  Retrieval engine (see docs/CPP.md)
docs/
  Python.md             Python module reference
  CPP.md                C++ engine reference
tests/                  pytest suite (Python + C++-driven)
data/                   Generated artifacts (DB, token freq, cache)
```

See [docs/Python.md](docs/Python.md) and [docs/CPP.md](docs/CPP.md) for
module-level detail.

## Notes & gotchas

- **Hardcoded MSYS2 path.** `C:\msys64\ucrt64` appears in [setup.bat](setup.bat),
  [setup.sh](setup.sh), and `UCRT64_BIN` in [src/pipeline.py](src/pipeline.py). If
  MSYS2 is installed elsewhere, edit those three spots (build prefix + runtime
  `PATH`).
- **Long silent runs are expected** on a full corpus — ingestion and tagging
  print little until they finish.
- **Datamuse tagging needs network access.** Topic expansion calls the Datamuse
  API; results are cached in `data/datamuse_cache.json`, and a failed request
  degrades gracefully (no expanded tags) rather than aborting the build.
- **Incremental vs. full.** Re-running `setup.bat` skips unchanged PDFs (matched by
  content SHA-256); use `--full` to recompute everything.
