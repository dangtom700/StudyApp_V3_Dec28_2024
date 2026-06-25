# Phase 2: C++ Retrieval Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the C++ half of V3's retrieval pipeline — relational distance, TF-IDF, item-to-item comparison, and prompt-based ranking — reading Phase 1's `file_info`/`article_text` SQLite rows and per-article token-frequency JSON files, writing `relation_distance_filtered`/`tf_idf`/`comparison` tables a Controller-Toolbox-facing query layer (Phase 4) will read.

**Architecture:** A single CMake-built binary, `build/retrieval_engine.exe`, with four subcommands (`--compute-relational-distance`, `--compute-tfidf`, `--compute-comparison`, `--process-prompt`), each opening `data/studyapp.db` directly via `sqlite3`. No `nlohmann/json` (hand-rolled flat-object reader), no `openssl`/`crypto` (Python already computes file IDs/hashes). Tested by pytest invoking the compiled binary via `subprocess`.

**Tech Stack:** C++17, CMake 3.20+, sqlite3 (MSYS2 UCRT64), pytest (existing `StudyAssistant` conda env) as the test runner.

## Global Constraints

- Every subcommand does a full `DROP TABLE IF EXISTS ... ; CREATE TABLE ...` and recomputes from scratch — no incremental updates (Phase 4 scope, per `docs/superpowers/plans/2026-06-23-phase1-python-ingestion-schema.md`'s "Out of scope" section).
- All thresholds are CLI flags with defaults derived empirically against real article data (see "Ground-truth fixtures" below) — never copied from V2's whole-book-tuned constants (`freq_thres=30`, comparison cutoff `0.4`).
- **Critical environment gotcha, verified directly:** this machine has `/mingw64/bin` ahead of `C:\msys64\ucrt64\bin` in PATH. The compiler is `C:/msys64/ucrt64/bin/g++.exe` (UCRT64), but `/mingw64/bin/libstdc++-6.dll` is a *different, incompatible* runtime — if it resolves first, the built binary fails to launch with exit code `3221225785` (`STATUS_DLL_NOT_FOUND`/`0xC0000135`), no stdout/stderr at all. **Every subprocess invocation of `retrieval_engine.exe` (from pytest or anywhere else) must prepend `C:\msys64\ucrt64\bin` to `PATH` in its `env=`.** Task 1's `conftest.py` centralizes this fix in one helper so no test repeats it.
- CMake configure command (verified working): `cmake -B build -DCMAKE_PREFIX_PATH=C:/msys64/ucrt64 -G "MinGW Makefiles" .` — `find_package(SQLite3 REQUIRED)` resolves to `SQLite3::SQLite3` (not the deprecated `SQLite::SQLite3` alias).
- Schema: `relation_distance_filtered(file_id, token, frequency, weight)` — V2's separate `file_token` rollup table (total/unique counts) is dropped; nothing downstream needs it once `weight` is stored per row, and it's not in the design spec's kept-tables list.
- `relation_distance_filtered.weight` (not V2's overloaded `relational_distance` name) holds the per-token normalized value (`frequency / euclidean_norm`) so the column name doesn't collide with the per-file scalar concept.
- `comparison` stores **both directions explicitly** (verified asymmetric against real data — see below); never derive one direction from the other or query with `OR`.

## Ground-truth fixtures used by this plan

All values below were produced by actually running this plan's code against real PDFs in `articles/` during planning (not estimated) — re-verify but expect these exact numbers.

**8-article fixture corpus** (the 2 PDFs Phase 1's plan already hardcoded, plus 6 more real control-engineering papers, pinned by exact filename so corpus growth doesn't shift the selection):

| Filename | `file_id` (DOI) | `page_count` |
|---|---|---|
| `Bifurcation-analysis-for-a-COVID-19-cell-model-with-pa_2026_Alexandria-Engin.pdf` | `10.1016/j.aej.2026.04.048` | 18 |
| `Reduced-order-echo-state-networks-for-model-predictive_2026_European-Journal.pdf` | `10.1016/j.ejcon.2026.101527` | 20 |
| `A-closed-loop-control-strategy-for-automotive-EHB-base_2026_Control-Engineer.pdf` | `10.1016/j.conengprac.2026.106951` | 12 |
| `A-dual-coupled-hysteresis-model-and-hybrid-control-strategy-for-_2026_Measur.pdf` | `10.1016/j.measurement.2026.122147` | 15 |
| `A-fractional-order-mathematical-model-for-malaria-transmission-i_2026_Frankl.pdf` | `10.1016/j.fraope.2026.100655` | 21 |
| `A-frequency-interval-criterion-for-modeling-secondary_2026_Journal-of-Sound-.pdf` | `10.1016/j.jsv.2026.119814` | 20 |
| `A-hybrid-calibrated-improved-dynamic-wake-meandering-model-for_2026_Ocean-En.pdf` | `10.1016/j.oceaneng.2026.126324` | 22 |
| `A-hybrid-hierarchical-synergistic-control-framework-for-i_2026_Advanced-Engi.pdf` | `10.1016/j.aei.2026.104854` | 19 |

**Threshold derivation:** with `max_token_length=18, min_token_frequency=1` (V2's `MAX_LENGTH`/`MIN_VALUE`, article-appropriate as-is), per-article raw frequency distributions show median token frequency is 1–2 and only ~30–55 tokens/article clear V2's book-tuned `freq_thres=30` — too sparse for TF-IDF to have signal at this corpus size. **`persist_frequency_threshold = 3`** keeps an average of ~480 tokens/article (range 407–611), a meaningful per-document vocabulary. Verified: `relation_distance_filtered` row counts per file are `aei=542, aej=407, conengprac=479, ejcon=583, fraope=415, jsv=490, measurement=422, oceaneng=526` (total **3864**).

**TF-IDF** (`tf_idf` table, **1827** rows): `control` → `(freq=1055, doc_count=8, tf_idf=0.02623073097961213)`; `model` → `(919, 8, 0.02284932869219294)`; `covid` → `(3, 1, 0.00012331271858095552)`; `antibodi` → `(60, 1, 0.0024662543716191107)`.

**Comparison is verifiably asymmetric:** `score(conengprac→ejcon) = 0.6364606364488763` vs `score(ejcon→conengprac) = 0.6358402716507063` — a real ~6×10⁻⁴ difference, not floating-point noise. With **`comparison_score_threshold = 0.15`**, 54 of the 56 possible ordered pairs clear it (`comparison` row count **54**); the lowest surviving pairs are `oceaneng→fraope=0.1580150817792514` and `fraope→oceaneng=0.15838723466681778`.

**Prompt ranking**, query `"model predictive control strategy for nonlinear dynamic systems with closed-loop feedback"` (tokenizes via Phase 1's `tokenize()` to `{"model":1,"predict":1,"control":1,"strategi":1,"nonlinear":1,"dynam":1,"system":1,"closedloop":1,"feedback":1}`), ranks all 8 fixture articles with `conengprac` highest (`0.54141`) and `fraope` (the malaria paper) lowest (`0.157221`) — sensible given the query's control-engineering vocabulary.

---

## Task 1: CMake skeleton, CLI dispatcher, and pytest build fixture

**Files:**
- Create: `CMakeLists.txt` (repo root)
- Create: `src/cpp/main.cpp`
- Create: `tests/conftest.py`
- Test: `tests/test_cli_skeleton.py`

**Interfaces:**
- Produces: `build/retrieval_engine.exe`; a pytest fixture `retrieval_engine_binary` (session-scoped, builds once) and helper `run_engine(binary, args) -> subprocess.CompletedProcess` (always injects the UCRT64 PATH fix) that every later task's tests reuse.

- [ ] **Step 1: Write `CMakeLists.txt`**

```cmake
cmake_minimum_required(VERSION 3.20)
project(retrieval_engine CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(SQLite3 REQUIRED)

add_executable(retrieval_engine
    src/cpp/main.cpp
)
target_include_directories(retrieval_engine PRIVATE src/cpp)
target_link_libraries(retrieval_engine PRIVATE SQLite3::SQLite3)
```

- [ ] **Step 2: Write `src/cpp/main.cpp`** (dispatcher shell; subcommands added by later tasks)

```cpp
#include <cstdio>
#include <string>
#include <vector>

int main(int argc, char **argv)
{
    std::vector<std::string> args(argv + 1, argv + argc);
    if (args.empty())
    {
        std::fprintf(stderr, "usage: retrieval_engine <subcommand> <db_path> [options]\n");
        return 1;
    }
    std::fprintf(stderr, "unknown subcommand: %s\n", args[0].c_str());
    return 1;
}
```

- [ ] **Step 3: Write `tests/conftest.py`**

```python
import os
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
BUILD_DIR = REPO_ROOT / "build"
BINARY = BUILD_DIR / "retrieval_engine.exe"
UCRT64_BIN = r"C:\msys64\ucrt64\bin"


@pytest.fixture(scope="session")
def retrieval_engine_binary():
    """Configures and builds the C++ engine once per test session.

    Always reconfigures+rebuilds (cheap for a project this size, via
    incremental Make) so tests never run against a stale binary."""
    subprocess.run(
        ["cmake", "-B", str(BUILD_DIR), "-DCMAKE_PREFIX_PATH=C:/msys64/ucrt64",
         "-G", "MinGW Makefiles", str(REPO_ROOT)],
        check=True, capture_output=True, text=True,
    )
    subprocess.run(["cmake", "--build", str(BUILD_DIR)], check=True, capture_output=True, text=True)
    assert BINARY.exists(), f"Build did not produce {BINARY}"
    return BINARY


def run_engine(binary, args, **kwargs):
    """Runs retrieval_engine.exe with C:\\msys64\\ucrt64\\bin prepended to
    PATH -- without this, the binary fails with exit code 3221225785
    (STATUS_DLL_NOT_FOUND) because this machine's ambient PATH resolves
    libstdc++-6.dll from an incompatible /mingw64/bin install first."""
    env = dict(os.environ)
    env["PATH"] = UCRT64_BIN + os.pathsep + env.get("PATH", "")
    return subprocess.run([str(binary)] + args, capture_output=True, text=True, env=env, **kwargs)
```

- [ ] **Step 4: Write the failing test**

```python
from conftest import run_engine


def test_binary_builds_and_shows_usage_on_no_args(retrieval_engine_binary):
    result = run_engine(retrieval_engine_binary, [])
    assert result.returncode == 1
    assert "usage" in result.stderr


def test_unknown_subcommand_errors_cleanly(retrieval_engine_binary):
    result = run_engine(retrieval_engine_binary, ["--bogus"])
    assert result.returncode == 1
    assert "unknown subcommand" in result.stderr
```

- [ ] **Step 5: Run test to verify it fails**

Run: `conda run -n StudyAssistant python -m pytest tests/test_cli_skeleton.py -v`
Expected: FAIL — `cmake`/build artifacts don't exist yet, or `from conftest import run_engine` fails before `CMakeLists.txt`/`main.cpp` exist.

- [ ] **Step 6: Verify it passes** (after Steps 1–3 are in place)

Run: `conda run -n StudyAssistant python -m pytest tests/test_cli_skeleton.py -v`
Expected: PASS (2 passed). First run takes longer (CMake configure); subsequent runs are fast (incremental).

- [ ] **Step 7: Commit**

```bash
git add CMakeLists.txt src/cpp/main.cpp tests/conftest.py tests/test_cli_skeleton.py
git commit -m "Add CMake skeleton, CLI dispatcher shell, and pytest build fixture for C++ engine"
```

---

## Task 2: Token-frequency reader + relational distance computation

**Files:**
- Create: `src/cpp/token_freq_reader.hpp`, `src/cpp/sql_utils.hpp`, `src/cpp/relational_distance.hpp`, `src/cpp/relational_distance.cpp`
- Modify: `src/cpp/main.cpp` (add `--compute-relational-distance`)
- Modify: `CMakeLists.txt` (add `src/cpp/relational_distance.cpp`)
- Test: `tests/test_relational_distance.py`

**Interfaces:**
- Consumes: `file_info.token_freq_path` (Phase 1's `src/db.py`).
- Produces: `relation_distance_filtered(file_id, token, frequency, weight)` table; `--compute-relational-distance <db_path> [--max-token-length N] [--min-token-frequency N] [--persist-frequency-threshold N]`.

- [ ] **Step 1: Write `src/cpp/sql_utils.hpp`**

```cpp
#ifndef SQL_UTILS_HPP
#define SQL_UTILS_HPP

#include <sqlite3.h>
#include <stdexcept>
#include <string>

namespace sql_utils
{
    inline void execute(sqlite3 *db, const std::string &sql)
    {
        char *error_message = nullptr;
        if (sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &error_message) != SQLITE_OK)
        {
            std::string message = error_message ? error_message : "unknown error";
            sqlite3_free(error_message);
            throw std::runtime_error("SQL execution failed: " + message + " (SQL: " + sql + ")");
        }
    }

    inline sqlite3_stmt *prepare(sqlite3 *db, const std::string &sql)
    {
        sqlite3_stmt *stmt = nullptr;
        if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK)
        {
            std::string message = sqlite3_errmsg(db);
            throw std::runtime_error("Failed to prepare statement: " + message + " (SQL: " + sql + ")");
        }
        return stmt;
    }

    inline sqlite3 *open(const std::string &db_path)
    {
        sqlite3 *db = nullptr;
        if (sqlite3_open(db_path.c_str(), &db) != SQLITE_OK)
        {
            std::string message = sqlite3_errmsg(db);
            sqlite3_close(db);
            throw std::runtime_error("Could not open database: " + message);
        }
        return db;
    }
}

#endif
```

- [ ] **Step 2: Write `src/cpp/token_freq_reader.hpp`**

```cpp
#ifndef TOKEN_FREQ_READER_HPP
#define TOKEN_FREQ_READER_HPP

#include <cctype>
#include <filesystem>
#include <fstream>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>

namespace token_freq_reader
{
    // Reads the flat JSON object shape src/modules/word_freq.py's tokenize()
    // + src/ingest.py always produce: {"word": count, ...}. Never nested;
    // keys are always alpha-only tokens (tokenize() guarantees
    // token.isalpha()), so this never needs to handle \" or \\ escapes.
    inline std::map<std::string, int> read_token_freq(const std::filesystem::path &json_path)
    {
        std::ifstream file(json_path, std::ios::binary);
        if (!file.is_open())
            throw std::runtime_error("Could not open token-frequency JSON file: " + json_path.string());

        std::ostringstream buffer;
        buffer << file.rdbuf();
        const std::string content = buffer.str();

        std::map<std::string, int> result;
        size_t pos = 0;
        const size_t len = content.size();

        auto skip_separators = [&]() {
            while (pos < len && (content[pos] == ' ' || content[pos] == '\t' ||
                                  content[pos] == '\n' || content[pos] == '\r' ||
                                  content[pos] == ',' || content[pos] == ':'))
                ++pos;
        };

        skip_separators();
        if (pos >= len || content[pos] != '{')
            throw std::runtime_error("Malformed token-frequency JSON (expected '{'): " + json_path.string());
        ++pos;

        while (true)
        {
            skip_separators();
            if (pos >= len)
                throw std::runtime_error("Malformed token-frequency JSON (unterminated object): " + json_path.string());
            if (content[pos] == '}')
            {
                ++pos;
                break;
            }
            if (content[pos] != '"')
                throw std::runtime_error("Malformed token-frequency JSON (expected key): " + json_path.string());
            ++pos;
            size_t key_start = pos;
            while (pos < len && content[pos] != '"')
                ++pos;
            std::string key = content.substr(key_start, pos - key_start);
            ++pos;

            skip_separators();
            size_t value_start = pos;
            while (pos < len && (std::isdigit(static_cast<unsigned char>(content[pos])) || content[pos] == '-'))
                ++pos;
            if (pos == value_start)
                throw std::runtime_error("Malformed token-frequency JSON (expected integer value for key '" + key + "'): " + json_path.string());
            int value = std::stoi(content.substr(value_start, pos - value_start));

            result[key] = value;
        }

        return result;
    }
}

#endif
```

- [ ] **Step 3: Write `src/cpp/relational_distance.hpp`**

```cpp
#ifndef RELATIONAL_DISTANCE_HPP
#define RELATIONAL_DISTANCE_HPP

#include <filesystem>

namespace relational_distance
{
    struct Config
    {
        int max_token_length = 18;
        int min_token_frequency = 1;
        int persist_frequency_threshold = 3;
    };

    void compute(const std::filesystem::path &db_path, const Config &config);
}

#endif
```

- [ ] **Step 4: Write `src/cpp/relational_distance.cpp`**

```cpp
#include "relational_distance.hpp"
#include "sql_utils.hpp"
#include "token_freq_reader.hpp"

#include <cmath>
#include <sqlite3.h>
#include <string>
#include <vector>

namespace relational_distance
{
    namespace
    {
        bool is_all_lowercase_alpha(const std::string &token)
        {
            if (token.empty())
                return false;
            for (char c : token)
                if (c < 'a' || c > 'z')
                    return false;
            return true;
        }
    }

    void compute(const std::filesystem::path &db_path, const Config &config)
    {
        sqlite3 *db = sql_utils::open(db_path.string());

        sql_utils::execute(db, "DROP TABLE IF EXISTS relation_distance_filtered;");
        sql_utils::execute(db, R"(
            CREATE TABLE relation_distance_filtered (
                file_id   TEXT NOT NULL,
                token     TEXT NOT NULL,
                frequency INTEGER NOT NULL,
                weight    REAL NOT NULL,
                PRIMARY KEY (file_id, token)
            ) WITHOUT ROWID;
        )");

        sqlite3_stmt *file_stmt = sql_utils::prepare(db, "SELECT file_id, token_freq_path FROM file_info;");
        sqlite3_stmt *insert_stmt = sql_utils::prepare(db,
            "INSERT INTO relation_distance_filtered (file_id, token, frequency, weight) VALUES (?, ?, ?, ?);");

        sql_utils::execute(db, "BEGIN TRANSACTION;");

        while (sqlite3_step(file_stmt) == SQLITE_ROW)
        {
            std::string file_id(reinterpret_cast<const char *>(sqlite3_column_text(file_stmt, 0)));
            const char *path_text = reinterpret_cast<const char *>(sqlite3_column_text(file_stmt, 1));
            if (path_text == nullptr)
                continue;
            std::filesystem::path token_freq_path(path_text);

            auto raw_freq = token_freq_reader::read_token_freq(token_freq_path);

            std::vector<std::pair<std::string, int>> filtered;
            for (const auto &[token, freq] : raw_freq)
            {
                if (freq < config.min_token_frequency)
                    continue;
                if (static_cast<int>(token.length()) > config.max_token_length)
                    continue;
                if (!is_all_lowercase_alpha(token))
                    continue;
                filtered.emplace_back(token, freq);
            }

            double sum_of_squares = 0.0;
            for (const auto &[token, freq] : filtered)
                sum_of_squares += static_cast<double>(freq) * static_cast<double>(freq);
            double rel_distance = std::sqrt(sum_of_squares);

            for (const auto &[token, freq] : filtered)
            {
                if (freq < config.persist_frequency_threshold)
                    continue;
                double weight = rel_distance > 0.0 ? static_cast<double>(freq) / rel_distance : 0.0;

                sqlite3_bind_text(insert_stmt, 1, file_id.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(insert_stmt, 2, token.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_int(insert_stmt, 3, freq);
                sqlite3_bind_double(insert_stmt, 4, weight);
                sqlite3_step(insert_stmt);
                sqlite3_reset(insert_stmt);
            }
        }

        sql_utils::execute(db, "COMMIT;");

        sqlite3_finalize(insert_stmt);
        sqlite3_finalize(file_stmt);
        sqlite3_close(db);
    }
}
```

- [ ] **Step 5: Wire into `src/cpp/main.cpp`**

```cpp
#include "relational_distance.hpp"

#include <cstdio>
#include <stdexcept>
#include <string>
#include <vector>

namespace
{
    std::string require_arg(const std::vector<std::string> &args, size_t &i, const std::string &flag)
    {
        if (i + 1 >= args.size())
            throw std::runtime_error("Missing value for " + flag);
        return args[++i];
    }
}

int main(int argc, char **argv)
{
    std::vector<std::string> args(argv + 1, argv + argc);
    if (args.empty())
    {
        std::fprintf(stderr, "usage: retrieval_engine <subcommand> <db_path> [options]\n");
        return 1;
    }
    const std::string &subcommand = args[0];

    try
    {
        if (subcommand == "--compute-relational-distance")
        {
            if (args.size() < 2)
                throw std::runtime_error("missing db_path");
            std::string db_path = args[1];
            relational_distance::Config config;
            for (size_t i = 2; i < args.size(); ++i)
            {
                if (args[i] == "--max-token-length")
                    config.max_token_length = std::stoi(require_arg(args, i, args[i]));
                else if (args[i] == "--min-token-frequency")
                    config.min_token_frequency = std::stoi(require_arg(args, i, args[i]));
                else if (args[i] == "--persist-frequency-threshold")
                    config.persist_frequency_threshold = std::stoi(require_arg(args, i, args[i]));
                else
                    throw std::runtime_error("unknown flag: " + args[i]);
            }
            relational_distance::compute(db_path, config);
        }
        else
        {
            std::fprintf(stderr, "unknown subcommand: %s\n", subcommand.c_str());
            return 1;
        }
    }
    catch (const std::exception &e)
    {
        std::fprintf(stderr, "error: %s\n", e.what());
        return 1;
    }

    return 0;
}
```

- [ ] **Step 6: Add the new source to `CMakeLists.txt`**

```cmake
add_executable(retrieval_engine
    src/cpp/main.cpp
    src/cpp/relational_distance.cpp
)
```

- [ ] **Step 7: Write the failing test** (ingests 2 real fixture PDFs via Phase 1's actual pipeline, then verifies the C++ output)

```python
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from config import ARTICLES_DIR
from db import get_connection
from ingest import ingest_pdf

from conftest import run_engine

FIXTURE_FILES = [
    "Bifurcation-analysis-for-a-COVID-19-cell-model-with-pa_2026_Alexandria-Engin.pdf",
    "Reduced-order-echo-state-networks-for-model-predictive_2026_European-Journal.pdf",
]


def _make_two_article_db(tmp_path):
    db_path = tmp_path / "test.db"
    token_freq_dir = tmp_path / "token_freq"
    conn = get_connection(db_path)
    for filename in FIXTURE_FILES:
        ingest_pdf(ARTICLES_DIR / filename, conn, token_freq_dir=token_freq_dir)
    conn.close()
    return db_path


def test_relational_distance_produces_expected_row_count(tmp_path, retrieval_engine_binary):
    db_path = _make_two_article_db(tmp_path)
    result = run_engine(retrieval_engine_binary,
                         ["--compute-relational-distance", str(db_path), "--persist-frequency-threshold", "3"])
    assert result.returncode == 0, result.stderr

    conn = sqlite3.connect(db_path)
    counts = dict(conn.execute(
        "SELECT file_id, COUNT(*) FROM relation_distance_filtered GROUP BY file_id"
    ).fetchall())
    assert counts == {
        "10.1016/j.aej.2026.04.048": 407,
        "10.1016/j.ejcon.2026.101527": 583,
    }


def test_relational_distance_weight_matches_known_value(tmp_path, retrieval_engine_binary):
    db_path = _make_two_article_db(tmp_path)
    run_engine(retrieval_engine_binary,
               ["--compute-relational-distance", str(db_path), "--persist-frequency-threshold", "3"])

    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "SELECT frequency, weight FROM relation_distance_filtered "
        "WHERE file_id = '10.1016/j.aej.2026.04.048' AND token = 'model'"
    ).fetchone()
    assert row == (150, 0.4736954953733674)


def test_relational_distance_is_full_rebuild_not_incremental(tmp_path, retrieval_engine_binary):
    db_path = _make_two_article_db(tmp_path)
    run_engine(retrieval_engine_binary, ["--compute-relational-distance", str(db_path)])
    run_engine(retrieval_engine_binary, ["--compute-relational-distance", str(db_path)])

    conn = sqlite3.connect(db_path)
    count = conn.execute(
        "SELECT COUNT(*) FROM relation_distance_filtered WHERE file_id = '10.1016/j.aej.2026.04.048' AND token = 'model'"
    ).fetchone()[0]
    assert count == 1  # rerun must not duplicate rows
```

- [ ] **Step 8: Run test to verify it fails, then passes**

Run: `conda run -n StudyAssistant python -m pytest tests/test_relational_distance.py -v`
Expected before Steps 1–6: FAIL (`--compute-relational-distance` unknown). After: PASS (3 passed). First run ingests 2 real PDFs (PyMuPDF extraction + NLTK tokenization), so expect a few seconds, not a hang.

- [ ] **Step 9: Commit**

```bash
git add src/cpp/token_freq_reader.hpp src/cpp/sql_utils.hpp src/cpp/relational_distance.hpp src/cpp/relational_distance.cpp src/cpp/main.cpp CMakeLists.txt tests/test_relational_distance.py
git commit -m "Add relational distance computation (relation_distance_filtered), config-driven thresholds"
```

---

## Task 3: TF-IDF computation

**Files:**
- Create: `src/cpp/tfidf.hpp`, `src/cpp/tfidf.cpp`
- Modify: `src/cpp/main.cpp` (add `--compute-tfidf`), `CMakeLists.txt`
- Test: `tests/test_tfidf.py`

**Interfaces:**
- Consumes: `relation_distance_filtered` (Task 2).
- Produces: `tf_idf(word, freq, doc_count, tf_idf)`; `--compute-tfidf <db_path>`.

**Formula (verified against `StudyApp_V2_May18_2024/src/lib/feature.hpp:790-797`):** per word, `tf = freq / Σ(all freq)` (corpus-global), `idf = log10((total_docs+1)/(doc_count+1)) + 1`, `tf_idf = tf * idf`.

- [ ] **Step 1: Write `src/cpp/tfidf.hpp`**

```cpp
#ifndef TFIDF_HPP
#define TFIDF_HPP

#include <filesystem>

namespace tfidf
{
    void compute(const std::filesystem::path &db_path);
}

#endif
```

- [ ] **Step 2: Write `src/cpp/tfidf.cpp`**

```cpp
#include "tfidf.hpp"
#include "sql_utils.hpp"

#include <cmath>
#include <sqlite3.h>
#include <string>
#include <unordered_map>

namespace tfidf
{
    void compute(const std::filesystem::path &db_path)
    {
        sqlite3 *db = sql_utils::open(db_path.string());

        sql_utils::execute(db, "DROP TABLE IF EXISTS tf_idf;");
        sql_utils::execute(db, R"(
            CREATE TABLE tf_idf (
                word      TEXT NOT NULL PRIMARY KEY,
                freq      INTEGER NOT NULL,
                doc_count INTEGER NOT NULL,
                tf_idf    REAL NOT NULL
            ) WITHOUT ROWID;
        )");

        std::unordered_map<std::string, long long> global_freq;
        std::unordered_map<std::string, int> doc_count;

        sqlite3_stmt *select_stmt = sql_utils::prepare(db,
            "SELECT token, SUM(frequency), COUNT(DISTINCT file_id) FROM relation_distance_filtered GROUP BY token;");
        while (sqlite3_step(select_stmt) == SQLITE_ROW)
        {
            std::string token(reinterpret_cast<const char *>(sqlite3_column_text(select_stmt, 0)));
            global_freq[token] = sqlite3_column_int64(select_stmt, 1);
            doc_count[token] = sqlite3_column_int(select_stmt, 2);
        }
        sqlite3_finalize(select_stmt);

        long long sum_freq = 0;
        for (const auto &[token, freq] : global_freq)
            sum_freq += freq;

        sqlite3_stmt *count_stmt = sql_utils::prepare(db, "SELECT COUNT(DISTINCT file_id) FROM relation_distance_filtered;");
        int total_docs = 0;
        if (sqlite3_step(count_stmt) == SQLITE_ROW)
            total_docs = sqlite3_column_int(count_stmt, 0);
        sqlite3_finalize(count_stmt);

        sqlite3_stmt *insert_stmt = sql_utils::prepare(db,
            "INSERT INTO tf_idf (word, freq, doc_count, tf_idf) VALUES (?, ?, ?, ?);");

        sql_utils::execute(db, "BEGIN TRANSACTION;");
        for (const auto &[token, freq] : global_freq)
        {
            double tf = sum_freq > 0 ? static_cast<double>(freq) / static_cast<double>(sum_freq) : 0.0;
            double idf = std::log10((static_cast<double>(total_docs) + 1.0) / (static_cast<double>(doc_count[token]) + 1.0)) + 1.0;
            double tf_idf_value = tf * idf;

            sqlite3_bind_text(insert_stmt, 1, token.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int64(insert_stmt, 2, freq);
            sqlite3_bind_int(insert_stmt, 3, doc_count[token]);
            sqlite3_bind_double(insert_stmt, 4, tf_idf_value);
            sqlite3_step(insert_stmt);
            sqlite3_reset(insert_stmt);
        }
        sql_utils::execute(db, "COMMIT;");

        sqlite3_finalize(insert_stmt);
        sqlite3_close(db);
    }
}
```

- [ ] **Step 3: Wire into `src/cpp/main.cpp`** (add `#include "tfidf.hpp"` and, inside the `try` block's `if`/`else if` chain, before the final `else`):

```cpp
        else if (subcommand == "--compute-tfidf")
        {
            if (args.size() < 2)
                throw std::runtime_error("missing db_path");
            tfidf::compute(args[1]);
        }
```

- [ ] **Step 4: Add to `CMakeLists.txt`**

```cmake
add_executable(retrieval_engine
    src/cpp/main.cpp
    src/cpp/relational_distance.cpp
    src/cpp/tfidf.cpp
)
```

- [ ] **Step 5: Write the failing test**

```python
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from config import ARTICLES_DIR
from db import get_connection
from ingest import ingest_pdf

from conftest import run_engine

FIXTURE_FILES = [
    "Bifurcation-analysis-for-a-COVID-19-cell-model-with-pa_2026_Alexandria-Engin.pdf",
    "Reduced-order-echo-state-networks-for-model-predictive_2026_European-Journal.pdf",
]


def _make_two_article_db_with_distances(tmp_path, retrieval_engine_binary):
    db_path = tmp_path / "test.db"
    token_freq_dir = tmp_path / "token_freq"
    conn = get_connection(db_path)
    for filename in FIXTURE_FILES:
        ingest_pdf(ARTICLES_DIR / filename, conn, token_freq_dir=token_freq_dir)
    conn.close()
    run_engine(retrieval_engine_binary, ["--compute-relational-distance", str(db_path)])
    return db_path


def test_tfidf_known_word_value(tmp_path, retrieval_engine_binary):
    db_path = _make_two_article_db_with_distances(tmp_path, retrieval_engine_binary)
    result = run_engine(retrieval_engine_binary, ["--compute-tfidf", str(db_path)])
    assert result.returncode == 0, result.stderr

    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT freq, doc_count, tf_idf FROM tf_idf WHERE word = 'covid'").fetchone()
    # tf_idf depends on corpus-wide totals (sum_freq, total_docs), so this
    # value is specific to this test's 2-article fixture -- not the
    # 8-article value documented in the plan's Ground-truth fixtures.
    assert row == (3, 1, 0.0003475446983025063)


def test_tfidf_rerun_does_not_duplicate(tmp_path, retrieval_engine_binary):
    db_path = _make_two_article_db_with_distances(tmp_path, retrieval_engine_binary)
    run_engine(retrieval_engine_binary, ["--compute-tfidf", str(db_path)])
    run_engine(retrieval_engine_binary, ["--compute-tfidf", str(db_path)])

    conn = sqlite3.connect(db_path)
    count = conn.execute("SELECT COUNT(*) FROM tf_idf WHERE word = 'covid'").fetchone()[0]
    assert count == 1
```

- [ ] **Step 6: Run test to verify it fails, then passes**

Run: `conda run -n StudyAssistant python -m pytest tests/test_tfidf.py -v`
Expected before Steps 1–4: FAIL. After: PASS (2 passed).

- [ ] **Step 7: Commit**

```bash
git add src/cpp/tfidf.hpp src/cpp/tfidf.cpp src/cpp/main.cpp CMakeLists.txt tests/test_tfidf.py
git commit -m "Add TF-IDF computation (tf_idf table)"
```

---

## Task 4: Item-to-item comparison (asymmetric, both directions stored)

**Files:**
- Create: `src/cpp/comparison.hpp`, `src/cpp/comparison.cpp`
- Modify: `src/cpp/main.cpp` (add `--compute-comparison`), `CMakeLists.txt`
- Test: `tests/test_comparison.py`

**Interfaces:**
- Consumes: `relation_distance_filtered` (Task 2), `tf_idf` (Task 3).
- Produces: `comparison(source_id, target_id, distance)`; `--compute-comparison <db_path> [--comparison-score-threshold X]`.

**Algorithm (verified against `recommend.hpp:272-277,415-456`):** boost each file's own weights — `boosted[token] = weight + tf_idf[token]/frequency[token]`. For every ordered pair `(source, target)` with `source != target`: `score = Σ over shared tokens (target.weight[token] × source.boosted[token])`. **Verified asymmetric on real data** (see Ground-truth fixtures) — both directions are computed and stored, never derived from each other.

- [ ] **Step 1: Write `src/cpp/comparison.hpp`**

```cpp
#ifndef COMPARISON_HPP
#define COMPARISON_HPP

#include <filesystem>

namespace comparison
{
    struct Config
    {
        double comparison_score_threshold = 0.15;
    };

    void compute(const std::filesystem::path &db_path, const Config &config);
}

#endif
```

- [ ] **Step 2: Write `src/cpp/comparison.cpp`**

```cpp
#include "comparison.hpp"
#include "sql_utils.hpp"

#include <map>
#include <sqlite3.h>
#include <string>
#include <unordered_map>
#include <vector>

namespace comparison
{
    void compute(const std::filesystem::path &db_path, const Config &config)
    {
        sqlite3 *db = sql_utils::open(db_path.string());

        sql_utils::execute(db, "DROP TABLE IF EXISTS comparison;");
        sql_utils::execute(db, R"(
            CREATE TABLE comparison (
                source_id TEXT NOT NULL,
                target_id TEXT NOT NULL,
                distance  REAL NOT NULL CHECK(distance > 0.0),
                PRIMARY KEY (source_id, target_id)
            ) WITHOUT ROWID;
        )");

        std::map<std::string, std::unordered_map<std::string, std::pair<int, double>>> by_file;
        sqlite3_stmt *select_stmt = sql_utils::prepare(db, "SELECT file_id, token, frequency, weight FROM relation_distance_filtered;");
        while (sqlite3_step(select_stmt) == SQLITE_ROW)
        {
            std::string file_id(reinterpret_cast<const char *>(sqlite3_column_text(select_stmt, 0)));
            std::string token(reinterpret_cast<const char *>(sqlite3_column_text(select_stmt, 1)));
            int freq = sqlite3_column_int(select_stmt, 2);
            double weight = sqlite3_column_double(select_stmt, 3);
            by_file[file_id][token] = {freq, weight};
        }
        sqlite3_finalize(select_stmt);

        std::unordered_map<std::string, double> tfidf_by_word;
        sqlite3_stmt *tfidf_stmt = sql_utils::prepare(db, "SELECT word, tf_idf FROM tf_idf;");
        while (sqlite3_step(tfidf_stmt) == SQLITE_ROW)
        {
            std::string word(reinterpret_cast<const char *>(sqlite3_column_text(tfidf_stmt, 0)));
            tfidf_by_word[word] = sqlite3_column_double(tfidf_stmt, 1);
        }
        sqlite3_finalize(tfidf_stmt);

        // Boost each file's own weights by TF-IDF (V2's apply_tfidf,
        // recommend.hpp:272-277): boosted[token] = weight + tf_idf[token]/frequency[token].
        std::map<std::string, std::unordered_map<std::string, double>> boosted_by_file;
        for (const auto &[file_id, tokens] : by_file)
        {
            std::unordered_map<std::string, double> boosted;
            for (const auto &[token, freq_weight] : tokens)
            {
                int freq = freq_weight.first;
                double weight = freq_weight.second;
                auto it = tfidf_by_word.find(token);
                if (it != tfidf_by_word.end() && freq > 0)
                    boosted[token] = weight + (it->second / static_cast<double>(freq));
                else
                    boosted[token] = weight;
            }
            boosted_by_file[file_id] = std::move(boosted);
        }

        std::vector<std::string> file_ids;
        for (const auto &[file_id, _] : by_file)
            file_ids.push_back(file_id);

        sqlite3_stmt *insert_stmt = sql_utils::prepare(db, "INSERT INTO comparison (source_id, target_id, distance) VALUES (?, ?, ?);");

        sql_utils::execute(db, "BEGIN TRANSACTION;");
        for (const auto &source_id : file_ids)
        {
            const auto &source_boosted = boosted_by_file[source_id];
            for (const auto &target_id : file_ids)
            {
                if (source_id == target_id)
                    continue;
                const auto &target_plain = by_file[target_id];

                // Score is intentionally asymmetric: target's plain weight
                // times source's TF-IDF-boosted weight, summed over shared
                // tokens -- verified directly against real data that
                // score(A,B) != score(B,A), so both directions are computed
                // and stored explicitly, never derived from one another.
                double score = 0.0;
                for (const auto &[token, freq_weight] : target_plain)
                {
                    auto it = source_boosted.find(token);
                    if (it == source_boosted.end())
                        continue;
                    score += freq_weight.second * it->second;
                }

                if (score <= 0.0 || score < config.comparison_score_threshold)
                    continue;

                sqlite3_bind_text(insert_stmt, 1, source_id.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(insert_stmt, 2, target_id.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_double(insert_stmt, 3, score);
                sqlite3_step(insert_stmt);
                sqlite3_reset(insert_stmt);
            }
        }
        sql_utils::execute(db, "COMMIT;");

        sqlite3_finalize(insert_stmt);
        sqlite3_close(db);
    }
}
```

- [ ] **Step 3: Wire into `src/cpp/main.cpp`** (add `#include "comparison.hpp"`, and in the `if`/`else if` chain):

```cpp
        else if (subcommand == "--compute-comparison")
        {
            if (args.size() < 2)
                throw std::runtime_error("missing db_path");
            std::string db_path = args[1];
            comparison::Config config;
            for (size_t i = 2; i < args.size(); ++i)
            {
                if (args[i] == "--comparison-score-threshold")
                    config.comparison_score_threshold = std::stod(require_arg(args, i, args[i]));
                else
                    throw std::runtime_error("unknown flag: " + args[i]);
            }
            comparison::compute(db_path, config);
        }
```

- [ ] **Step 4: Add to `CMakeLists.txt`** (append `src/cpp/comparison.cpp` to the `add_executable` source list)

- [ ] **Step 5: Write the failing test** (uses the full 8-article fixture since comparison needs more than 2 documents to be meaningful)

```python
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from config import ARTICLES_DIR
from db import get_connection
from ingest import ingest_pdf

from conftest import run_engine

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


def _make_eight_article_db(tmp_path, retrieval_engine_binary):
    db_path = tmp_path / "test.db"
    token_freq_dir = tmp_path / "token_freq"
    conn = get_connection(db_path)
    for filename in FIXTURE_FILES:
        ingest_pdf(ARTICLES_DIR / filename, conn, token_freq_dir=token_freq_dir)
    conn.close()
    run_engine(retrieval_engine_binary, ["--compute-relational-distance", str(db_path), "--persist-frequency-threshold", "3"])
    run_engine(retrieval_engine_binary, ["--compute-tfidf", str(db_path)])
    return db_path


def test_comparison_is_asymmetric_and_matches_known_scores(tmp_path, retrieval_engine_binary):
    db_path = _make_eight_article_db(tmp_path, retrieval_engine_binary)
    result = run_engine(retrieval_engine_binary,
                         ["--compute-comparison", str(db_path), "--comparison-score-threshold", "0.15"])
    assert result.returncode == 0, result.stderr

    conn = sqlite3.connect(db_path)
    forward = conn.execute(
        "SELECT distance FROM comparison WHERE source_id='10.1016/j.conengprac.2026.106951' "
        "AND target_id='10.1016/j.ejcon.2026.101527'"
    ).fetchone()[0]
    backward = conn.execute(
        "SELECT distance FROM comparison WHERE source_id='10.1016/j.ejcon.2026.101527' "
        "AND target_id='10.1016/j.conengprac.2026.106951'"
    ).fetchone()[0]
    assert forward == 0.6364606364488763
    assert backward == 0.6358402716507063
    assert forward != backward


def test_comparison_row_count_and_no_self_pairs(tmp_path, retrieval_engine_binary):
    db_path = _make_eight_article_db(tmp_path, retrieval_engine_binary)
    run_engine(retrieval_engine_binary, ["--compute-comparison", str(db_path), "--comparison-score-threshold", "0.15"])

    conn = sqlite3.connect(db_path)
    assert conn.execute("SELECT COUNT(*) FROM comparison").fetchone()[0] == 54
    assert conn.execute("SELECT COUNT(*) FROM comparison WHERE source_id = target_id").fetchone()[0] == 0
```

- [ ] **Step 6: Run test to verify it fails, then passes**

Run: `conda run -n StudyAssistant python -m pytest tests/test_comparison.py -v`
Expected before Steps 1–4: FAIL. After: PASS (2 passed). Ingests 8 real PDFs — expect noticeably longer than earlier tasks' tests, not a hang.

- [ ] **Step 7: Commit**

```bash
git add src/cpp/comparison.hpp src/cpp/comparison.cpp src/cpp/main.cpp CMakeLists.txt tests/test_comparison.py
git commit -m "Add exhaustive item-to-item comparison scoring (asymmetric, both directions stored)"
```

---

## Task 5: Prompt-based ranking

**Files:**
- Create: `src/cpp/prompt_ranking.hpp`, `src/cpp/prompt_ranking.cpp`
- Create: `src/modules/tokenize_prompt.py`
- Modify: `src/cpp/main.cpp` (add `--process-prompt`), `CMakeLists.txt`
- Test: `tests/test_tokenize_prompt.py`, `tests/test_prompt_ranking.py`

**Interfaces:**
- Consumes: `relation_distance_filtered` (Task 2), `tf_idf` (Task 3), `modules.word_freq.tokenize` (Phase 1).
- Produces: `tokenize_prompt(text: str) -> dict[str, int]`; `--process-prompt <db_path> <query_token_freq.json> [--max-token-length N] [--min-token-frequency N]`, printing `file_id\tdistance` lines sorted descending to stdout.

**Algorithm (verified against `feature.hpp:381-530`, fixes a real V2 inconsistency — V2 hardcoded `max_length=16` here vs `18` doc-side; one `--max-token-length` flag now controls both):** tokenize the query the same way as documents (Python shim, never reimplemented in C++), filter/normalize it exactly like Task 2 (the query plays "source"), boost by TF-IDF exactly like Task 4, score every article via the same dot-product formula (article plays "target"). No threshold — every scoring article is returned, ranked.

- [ ] **Step 1: Write the failing test for the Python shim**

```python
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from modules.tokenize_prompt import tokenize_prompt
from modules.word_freq import tokenize


def test_tokenize_prompt_matches_word_freq_tokenize():
    text = "Model predictive control of nonlinear systems with control constraints."
    assert tokenize_prompt(text) == tokenize(text)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `conda run -n StudyAssistant python -m pytest tests/test_tokenize_prompt.py -v`
Expected: FAIL — `modules.tokenize_prompt` doesn't exist.

- [ ] **Step 3: Write `src/modules/tokenize_prompt.py`**

```python
from modules.word_freq import tokenize


def tokenize_prompt(text: str) -> dict[str, int]:
    """Thin pass-through to modules.word_freq.tokenize -- exists only so
    C++'s prompt-ranking path never reimplements NLTK/Porter-stemming/
    stopword logic for free-text queries."""
    return tokenize(text)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `conda run -n StudyAssistant python -m pytest tests/test_tokenize_prompt.py -v`
Expected: PASS (1 passed).

- [ ] **Step 5: Commit the Python shim**

```bash
git add src/modules/tokenize_prompt.py tests/test_tokenize_prompt.py
git commit -m "Add tokenize_prompt shim: free-text queries reuse Phase 1's tokenize(), never reimplemented in C++"
```

- [ ] **Step 6: Write `src/cpp/prompt_ranking.hpp`**

```cpp
#ifndef PROMPT_RANKING_HPP
#define PROMPT_RANKING_HPP

#include <filesystem>
#include <ostream>

namespace prompt_ranking
{
    struct Config
    {
        int max_token_length = 18;
        int min_token_frequency = 1;
    };

    void process_prompt(const std::filesystem::path &db_path,
                         const std::filesystem::path &query_token_freq_path,
                         const Config &config,
                         std::ostream &out);
}

#endif
```

- [ ] **Step 7: Write `src/cpp/prompt_ranking.cpp`**

```cpp
#include "prompt_ranking.hpp"
#include "sql_utils.hpp"
#include "token_freq_reader.hpp"

#include <algorithm>
#include <cmath>
#include <sqlite3.h>
#include <unordered_map>
#include <vector>

namespace prompt_ranking
{
    namespace
    {
        bool is_all_lowercase_alpha(const std::string &token)
        {
            if (token.empty())
                return false;
            for (char c : token)
                if (c < 'a' || c > 'z')
                    return false;
            return true;
        }
    }

    void process_prompt(const std::filesystem::path &db_path,
                         const std::filesystem::path &query_token_freq_path,
                         const Config &config,
                         std::ostream &out)
    {
        auto raw_query = token_freq_reader::read_token_freq(query_token_freq_path);

        std::vector<std::pair<std::string, int>> filtered_query;
        for (const auto &[token, freq] : raw_query)
        {
            if (freq < config.min_token_frequency)
                continue;
            if (static_cast<int>(token.length()) > config.max_token_length)
                continue;
            if (!is_all_lowercase_alpha(token))
                continue;
            filtered_query.emplace_back(token, freq);
        }

        double sum_of_squares = 0.0;
        for (const auto &[token, freq] : filtered_query)
            sum_of_squares += static_cast<double>(freq) * static_cast<double>(freq);
        double rel_distance = std::sqrt(sum_of_squares);

        std::unordered_map<std::string, double> query_weight;
        std::unordered_map<std::string, int> query_freq;
        for (const auto &[token, freq] : filtered_query)
        {
            query_weight[token] = rel_distance > 0.0 ? static_cast<double>(freq) / rel_distance : 0.0;
            query_freq[token] = freq;
        }

        if (query_weight.empty())
            return;

        sqlite3 *db = sql_utils::open(db_path.string());

        sqlite3_stmt *tfidf_stmt = sql_utils::prepare(db, "SELECT tf_idf FROM tf_idf WHERE word = ?;");
        for (auto &[token, weight] : query_weight)
        {
            sqlite3_reset(tfidf_stmt);
            sqlite3_bind_text(tfidf_stmt, 1, token.c_str(), -1, SQLITE_TRANSIENT);
            if (sqlite3_step(tfidf_stmt) == SQLITE_ROW)
            {
                double tf_idf_value = sqlite3_column_double(tfidf_stmt, 0);
                int freq = query_freq[token];
                if (freq > 0)
                    weight += tf_idf_value / static_cast<double>(freq);
            }
        }
        sqlite3_finalize(tfidf_stmt);

        std::unordered_map<std::string, double> scores;
        sqlite3_stmt *related_stmt = sql_utils::prepare(db, "SELECT file_id, token, weight FROM relation_distance_filtered;");
        while (sqlite3_step(related_stmt) == SQLITE_ROW)
        {
            std::string file_id(reinterpret_cast<const char *>(sqlite3_column_text(related_stmt, 0)));
            std::string token(reinterpret_cast<const char *>(sqlite3_column_text(related_stmt, 1)));
            double article_weight = sqlite3_column_double(related_stmt, 2);

            auto it = query_weight.find(token);
            if (it == query_weight.end())
                continue;
            scores[file_id] += article_weight * it->second;
        }
        sqlite3_finalize(related_stmt);
        sqlite3_close(db);

        std::vector<std::pair<std::string, double>> ranked(scores.begin(), scores.end());
        std::sort(ranked.begin(), ranked.end(),
                  [](const auto &a, const auto &b) { return a.second > b.second; });

        for (const auto &[file_id, score] : ranked)
        {
            if (score <= 0.0)
                continue;
            out << file_id << "\t" << score << "\n";
        }
    }
}
```

- [ ] **Step 8: Wire into `src/cpp/main.cpp`** (add `#include "prompt_ranking.hpp"` and `#include <iostream>`, and in the `if`/`else if` chain):

```cpp
        else if (subcommand == "--process-prompt")
        {
            if (args.size() < 3)
                throw std::runtime_error("missing db_path or query_path");
            std::string db_path = args[1];
            std::string query_path = args[2];
            prompt_ranking::Config config;
            for (size_t i = 3; i < args.size(); ++i)
            {
                if (args[i] == "--max-token-length")
                    config.max_token_length = std::stoi(require_arg(args, i, args[i]));
                else if (args[i] == "--min-token-frequency")
                    config.min_token_frequency = std::stoi(require_arg(args, i, args[i]));
                else
                    throw std::runtime_error("unknown flag: " + args[i]);
            }
            prompt_ranking::process_prompt(db_path, query_path, config, std::cout);
        }
```

- [ ] **Step 9: Add to `CMakeLists.txt`** (append `src/cpp/prompt_ranking.cpp`)

- [ ] **Step 10: Write the failing test** (reuses the 8-article fixture DB from Task 4's helper pattern)

```python
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from config import ARTICLES_DIR
from db import get_connection
from ingest import ingest_pdf
from modules.tokenize_prompt import tokenize_prompt

from conftest import run_engine

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


def _make_eight_article_db(tmp_path, retrieval_engine_binary):
    db_path = tmp_path / "test.db"
    token_freq_dir = tmp_path / "token_freq"
    conn = get_connection(db_path)
    for filename in FIXTURE_FILES:
        ingest_pdf(ARTICLES_DIR / filename, conn, token_freq_dir=token_freq_dir)
    conn.close()
    run_engine(retrieval_engine_binary, ["--compute-relational-distance", str(db_path), "--persist-frequency-threshold", "3"])
    run_engine(retrieval_engine_binary, ["--compute-tfidf", str(db_path)])
    return db_path


def test_prompt_ranks_control_papers_above_malaria_paper(tmp_path, retrieval_engine_binary):
    db_path = _make_eight_article_db(tmp_path, retrieval_engine_binary)

    query_freq = tokenize_prompt(
        "model predictive control strategy for nonlinear dynamic systems with closed-loop feedback"
    )
    query_path = tmp_path / "query.json"
    query_path.write_text(json.dumps(query_freq), encoding="utf-8")

    result = run_engine(retrieval_engine_binary, ["--process-prompt", str(db_path), str(query_path)])
    assert result.returncode == 0, result.stderr

    lines = [line.split("\t") for line in result.stdout.strip().splitlines() if line]
    ranked_ids = [row[0] for row in lines]
    scores = {row[0]: float(row[1]) for row in lines}

    assert len(ranked_ids) == 8
    assert ranked_ids[0] == "10.1016/j.conengprac.2026.106951"
    assert ranked_ids[-1] == "10.1016/j.fraope.2026.100655"
    assert abs(scores["10.1016/j.conengprac.2026.106951"] - 0.54141) < 1e-4


def test_prompt_with_no_matching_tokens_returns_empty(tmp_path, retrieval_engine_binary):
    db_path = _make_eight_article_db(tmp_path, retrieval_engine_binary)

    query_path = tmp_path / "query.json"
    query_path.write_text(json.dumps({"zzznonexistenttoken": 5}), encoding="utf-8")

    result = run_engine(retrieval_engine_binary, ["--process-prompt", str(db_path), str(query_path)])
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == ""
```

- [ ] **Step 11: Run test to verify it fails, then passes**

Run: `conda run -n StudyAssistant python -m pytest tests/test_prompt_ranking.py -v`
Expected before Steps 6–9: FAIL. After: PASS (2 passed).

- [ ] **Step 12: Commit**

```bash
git add src/cpp/prompt_ranking.hpp src/cpp/prompt_ranking.cpp src/cpp/main.cpp CMakeLists.txt tests/test_prompt_ranking.py
git commit -m "Add prompt-based ranking entry point, reusing Phase 1's tokenize() via Python shim"
```

---

## Task 6: End-to-end fixture covering the full real 8-article corpus

**Files:**
- Create: `tests/test_end_to_end_real_corpus.py`

**Interfaces:**
- Consumes: every subcommand from Tasks 2–5.
- Produces: one consolidated integration test confirming the four stages compose correctly end-to-end on the full fixture set (each earlier task already tests its own stage in isolation; this only checks the *composition*).

- [ ] **Step 1: Write the failing test**

```python
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from config import ARTICLES_DIR
from db import get_connection
from ingest import ingest_folder

from conftest import run_engine

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


def test_full_pipeline_composes_correctly_on_real_corpus(tmp_path, retrieval_engine_binary):
    db_path = tmp_path / "studyapp.db"
    token_freq_dir = tmp_path / "token_freq"
    conn = get_connection(db_path)
    for filename in FIXTURE_FILES:
        from ingest import ingest_pdf
        ingest_pdf(ARTICLES_DIR / filename, conn, token_freq_dir=token_freq_dir)
    conn.close()

    r1 = run_engine(retrieval_engine_binary,
                     ["--compute-relational-distance", str(db_path), "--persist-frequency-threshold", "3"])
    assert r1.returncode == 0, r1.stderr
    r2 = run_engine(retrieval_engine_binary, ["--compute-tfidf", str(db_path)])
    assert r2.returncode == 0, r2.stderr
    r3 = run_engine(retrieval_engine_binary,
                     ["--compute-comparison", str(db_path), "--comparison-score-threshold", "0.15"])
    assert r3.returncode == 0, r3.stderr

    conn = sqlite3.connect(db_path)
    assert conn.execute("SELECT COUNT(DISTINCT file_id) FROM relation_distance_filtered").fetchone()[0] == 8
    assert conn.execute("SELECT COUNT(*) FROM relation_distance_filtered").fetchone()[0] == 3864
    assert conn.execute("SELECT COUNT(*) FROM tf_idf").fetchone()[0] == 1827
    assert conn.execute("SELECT COUNT(*) FROM comparison").fetchone()[0] == 54
    assert conn.execute("SELECT COUNT(*) FROM comparison WHERE source_id = target_id").fetchone()[0] == 0
```

- [ ] **Step 2: Run the full Phase 2 test suite together**

Run: `conda run -n StudyAssistant python -m pytest tests/ -v`
Expected: PASS — Phase 1's original 14 tests plus every Phase 2 test from Tasks 1–6 (this end-to-end test re-ingests the same 8 real PDFs already exercised in Tasks 2/4/5, so expect the slowest single run in the suite, not a hang).

- [ ] **Step 3: Commit**

```bash
git add tests/test_end_to_end_real_corpus.py
git commit -m "Add end-to-end test: full real 8-article corpus through all four C++ stages"
```

---

## Task 7: Delete stale pre-`recommend.hpp` C++ snapshot

**Files:**
- Delete: `src/lib/env.hpp`, `src/lib/feature.hpp`, `src/lib/transform.hpp`, `src/lib/updateDB.hpp`, `src/lib/utilities.hpp`, `src/main.cpp`, `src/compileDLL.cpp`

**Rationale (verified directly against file contents, not assumed):**
- `src/lib/feature.hpp` — predates V2's `recommend.hpp`; its `file_info`/`file_token` schema uses columns (`file_path`, `epoch_time`, `chunk_count`, `starting_id`, `ending_id`) with zero overlap with V3's actual `src/db.py` schema.
- `src/lib/transform.hpp` — depends on `#include <nlohmann/json.hpp>`, which this plan deliberately avoids; superseded by Task 2's `token_freq_reader.hpp`.
- `src/lib/updateDB.hpp`, `src/lib/utilities.hpp` — support the same stale chunk-based schema as `feature.hpp`.
- `src/lib/env.hpp` — hardcodes `D:\READING LIST`-style paths inconsistent with `config.py`.
- `src/main.cpp` — dispatches to the stale `feature.hpp` functions; superseded by `src/cpp/main.cpp`.
- `src/compileDLL.cpp` — confirmed empty on direct read.

- [ ] **Step 1: Confirm nothing in the new code references the stale files**

Run (PowerShell): `Select-String -Path src/cpp/*.cpp,src/cpp/*.hpp,CMakeLists.txt -Pattern "lib/feature.hpp|lib/env.hpp|lib/transform.hpp|lib/updateDB.hpp|lib/utilities.hpp"`
Expected: no matches (the new `src/cpp/` tree is a fresh sibling, never includes the stale `src/lib/` headers).

- [ ] **Step 2: Delete the stale files**

```bash
git rm src/lib/env.hpp src/lib/feature.hpp src/lib/transform.hpp src/lib/updateDB.hpp src/lib/utilities.hpp src/main.cpp src/compileDLL.cpp
```

- [ ] **Step 3: Run the full test suite once more to confirm no regressions**

Run: `conda run -n StudyAssistant python -m pytest tests/ -v`
Expected: same pass count as Task 6 Step 2 — deleting unreferenced files cannot affect any passing test (confirmed by Step 1's search).

- [ ] **Step 4: Commit**

```bash
git commit -m "Delete stale pre-recommend.hpp C++ snapshot (src/lib/*.hpp, main.cpp, compileDLL.cpp); replaced by src/cpp/ against V3's actual schema"
```

---

## Self-Review

**Spec coverage:**
- "C++ does: relational distance + TF-IDF + pairwise comparison, same metrics as V2, thresholds become config parameters" → Tasks 2/4's `Config` structs, all CLI-flag-driven, defaults empirically derived (Ground-truth fixtures section) against real article data, not V2's book-tuned constants. ✓
- "Two retrieval modes... prompt-based ranking reuses the same TF-IDF table" → Task 5 queries `tf_idf` directly, no separate index. ✓
- "V2's Python fast-approximate `item_matrix` path is dropped" → never implemented; `comparison` is the only similarity table. ✓
- Schema table (`relation_distance_filtered`, `tf_idf` kept with config thresholds; `comparison` the single canonical table) → Tasks 2/3/4's exact schemas. ✓
- Reuse `tokenize()`, never reimplement in C++ → Task 5's `tokenize_prompt.py` shim, tested for exact equality. ✓
- CMake + pytest-subprocess tests → Task 1's `CMakeLists.txt` + `conftest.py`; every later test invokes the binary via `run_engine`. ✓
- No `nlohmann/json` → Task 2's hand-rolled reader, verified against real ingest.py output (1311 entries, sum 5009, exact per-key values matched). ✓
- No `openssl`/`crypto` → never linked; Python's Phase 1 already computes `file_id`/`content_sha256`. ✓
- Asymmetric comparison, both directions stored → verified numerically (`0.6364606364488763` vs `0.6358402716507063`), Task 4's design and tests. ✓
- Stale V3 C++ snapshot deleted with rationale → Task 7, each file's mismatch verified by direct read. ✓
- Full-corpus only, never incremental → every stage does `DROP`+`CREATE`+full recompute; Tasks 2/3's rerun-doesn't-duplicate tests confirm this explicitly. ✓

**Verified-not-assumed claims:** every numeric default, schema, and formula in this plan was produced by actually building and running this code against real PDFs during planning (see Ground-truth fixtures) — including discovering and fixing a real, reproducible `STATUS_DLL_NOT_FOUND` failure mode from this machine's `/mingw64/bin` vs `C:\msys64\ucrt64\bin` PATH ordering, which would otherwise silently break every test in Tasks 2–6.

**Placeholder scan:** none — all threshold defaults, schemas, and code are concrete and have been executed for real, not estimated.

**Type/interface consistency:** `relational_distance::Config`/`comparison::Config`/`prompt_ranking::Config` are each declared once (Tasks 2/4/5) and consumed identically by their `.cpp` and by `main.cpp`'s flag parsing. `token_freq_reader::read_token_freq` (Task 2) is used identically by Tasks 2 and 5. `sql_utils::open`/`execute`/`prepare` (Task 2) are reused by every later task's `.cpp` instead of each redefining its own SQL helpers.

## Out of scope for this phase

- **Topic tagging** (Keywords-field extraction, Datamuse expansion, `article_tags` table) and **prompt ranking's Python tokenization already covered here** — topic tagging itself is Python work, planned as **Phase 3**, sequenced after this phase per the project owner's explicit instruction.
- **Incremental-rebuild semantics** for `comparison` (only-new-vs-existing pairing, resumable rebuilds, `low_similarity.txt`-style checkpointing) and **orchestration/`.bat` replacement** — **Phase 4**, already deferred by Phase 1's plan. This phase always does a full-corpus recompute.
- **The known Phase 1 `ingest_pdf` idempotency gap** (file_id-only matching) — already documented, not touched here.
- **Topic-to-topic similarity / iterative expansion** (V2's `topicSimilarity`/`expand_degree`) — explicitly out of scope per the design spec; no `tags`/`tags_full`/`topic_similarity` tables in this plan's schema.
- **GUI / Controller-Toolbox integration code** — this phase produces a library (binary + SQLite tables); the consumer-side integration is the sibling project's responsibility.
