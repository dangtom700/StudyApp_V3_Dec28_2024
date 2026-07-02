# C++ Retrieval Engine

The C++ side is a single command-line executable, `retrieval_engine`, that does
the numeric heavy lifting Python is too slow for. It reads from and writes to the
same SQLite database the Python pipeline builds (`data/studyapp.db`). Sources
live in [../src/cpp/](../src/cpp/) and are compiled by the top-level
[CMakeLists.txt](../CMakeLists.txt).

## Building

The engine links against SQLite3 from MSYS2 UCRT64:

```powershell
cmake -B build -DCMAKE_PREFIX_PATH=C:/msys64/ucrt64 -G "MinGW Makefiles" .
cmake --build build
```

This produces `build/retrieval_engine.exe`. When you run it directly, the UCRT64
`bin` directory must be on `PATH` so `SQLite3.dll` resolves (the Python
[pipeline](../src/pipeline.py) prepends it automatically):

```powershell
$env:PATH = 'C:\msys64\ucrt64\bin;' + $env:PATH
```

## Command-line interface

```
retrieval_engine <subcommand> <db_path> [options]
```

Dispatch lives in [main.cpp](../src/cpp/main.cpp).

| Subcommand | Purpose | Options |
|------------|---------|---------|
| `--compute-relational-distance <db>` | Per-article token weighting written to `relation_distance_filtered` | `--max-token-length` (18), `--min-token-frequency` (1), `--persist-frequency-threshold` (3), `--incremental` |
| `--compute-tfidf <db>` | Corpus-wide TF-IDF written to `tf_idf` | — |
| `--compute-comparison <db>` | Pairwise article distances written to `comparison` | `--comparison-score-threshold` (0.15), `--incremental` |
| `--process-prompt <db> <query.json>` | Ranks articles against a token-frequency query; prints `file_id\tscore` (TSV, best first) to stdout | `--max-token-length` (18), `--min-token-frequency` (1) |

Values in parentheses are defaults (see each module's `Config` struct). The first
three subcommands are what the build pipeline runs, in that order; `--incremental`
restricts work to articles not yet processed. `--process-prompt` is the
query-time entry point and is the only subcommand that reads a query file and
writes to stdout instead of the DB.

## Translation units

| File | Role |
|------|------|
| [main.cpp](../src/cpp/main.cpp) | Argument parsing and subcommand dispatch |
| [relational_distance.cpp](../src/cpp/relational_distance.cpp) / `.hpp` | `--compute-relational-distance`; per-article token weights |
| [tfidf.cpp](../src/cpp/tfidf.cpp) / `.hpp` | `--compute-tfidf`; corpus-wide term weighting |
| [comparison.cpp](../src/cpp/comparison.cpp) / `.hpp` | `--compute-comparison`; pairwise article distances |
| [prompt_ranking.cpp](../src/cpp/prompt_ranking.cpp) / `.hpp` | `--process-prompt`; ranks articles against a query |

## Header-only helpers

| File | Role |
|------|------|
| [sql_utils.hpp](../src/cpp/sql_utils.hpp) | Thin SQLite wrappers: `open`, `prepare`, `execute` (throw `std::runtime_error` on failure) |
| [token_freq_reader.hpp](../src/cpp/token_freq_reader.hpp) | Minimal parser for the flat `{"token": count}` JSON that the Python side emits (`data/token_freq/*.json` and query files) |

The token-freq JSON is deliberately simple — keys are always alpha-only tokens
produced by [word_freq.py](../src/modules/word_freq.py)'s `tokenize()`, so the
reader never needs general JSON string-escape handling.
