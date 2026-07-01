# Phase 4: Incremental Rebuilds, Orchestration Entrypoint, and Run Scripts

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the C++ engine's `comparison`/`relation_distance_filtered` stages incremental (only new files trigger recomputation), fix `ingest_pdf`'s content-hash idempotency gap, wire everything into a single Python orchestration entrypoint, add `run.bat`/`run.sh`, and run the pipeline for real against the full 280-PDF corpus to produce `data/studyapp.db` for the first time — completing the first prototype.

**Architecture:** No new modules beyond `src/build.py` (orchestration) — this phase modifies the 3 existing C++ engine stages to support an opt-in `--incremental` flag, adds a schema-completeness fix to `src/db.py`, and fixes a content-hash gap in `src/ingest.py`. Default CLI behavior of the C++ binary is unchanged; incrementality is strictly additive.

**Tech Stack:** Same as Phases 1-3 (Python 3.12 / C++17 / SQLite, MinGW/MSYS2 toolchain on Windows). No new dependencies.

## Global Constraints

- Default (no `--incremental` flag) behavior of `--compute-relational-distance` and `--compute-comparison` must remain byte-identical to today (full drop-and-recompute) — all 44 existing tests call these subcommands with no flag and must keep passing unmodified.
- `tf_idf` is always fully recomputed on every build call (cheap, O(total tokens)) — it does not get an `--incremental` flag.
- Incremental `comparison` only skips pairs where **both** source and target are already-seen (tracked via a new `comparison_seen` marker table) — old-vs-old pairs are left untouched, not recomputed. This is a deliberate, documented deviation from literal "incremental == full rebuild for every pair," justified because achieving literal equivalence for old pairs would require full O(n²) recompute on every add, defeating the purpose. See design rationale in the plan-mode conversation (2026-07-01) that produced this plan.
- Full-rebuild checkpoint/resume (V2's `low_similarity.txt` pattern) is explicitly out of scope for this phase (YAGNI at 280-article scale; full rebuild is sub-second) — deferred until corpus growth actually makes it necessary.
- `.bat`/`.sh` scripts invoke Python via `conda run -n StudyAssistant ...`, not `conda activate` (matches this repo's already-established convention from the Phase 3 plan doc's test commands; `conda activate` in non-interactive scripts is exactly the V2 fragility pattern the design spec calls out and this redesign avoids).
- Implementation is inline/direct in this session (TDD per task: failing test → implementation → passing test → commit), not subagent-per-task dispatch, per explicit user request.

---

## Task 1: Schema completeness in `db.py`

**Files:**
- Modify: `src/db.py`
- Test: `tests/test_db.py`

**Interfaces:**
- Produces: `relation_distance_filtered`, `tf_idf`, `comparison`, `comparison_seen` tables always present after `get_connection()`, matching the exact `CREATE TABLE` bodies already used by `src/cpp/relational_distance.cpp`, `src/cpp/tfidf.cpp`, `src/cpp/comparison.cpp`.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_db.py`:

```python
def test_creates_relation_distance_filtered_table(tmp_path):
    conn = get_connection(tmp_path / "test.db")
    assert _column_names(conn, "relation_distance_filtered") == {"file_id", "token", "frequency", "weight"}


def test_creates_tf_idf_table(tmp_path):
    conn = get_connection(tmp_path / "test.db")
    assert _column_names(conn, "tf_idf") == {"word", "freq", "doc_count", "tf_idf"}


def test_creates_comparison_table(tmp_path):
    conn = get_connection(tmp_path / "test.db")
    assert _column_names(conn, "comparison") == {"source_id", "target_id", "distance"}


def test_creates_comparison_seen_table(tmp_path):
    conn = get_connection(tmp_path / "test.db")
    assert _column_names(conn, "comparison_seen") == {"file_id"}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `conda run -n StudyAssistant python -m pytest tests/test_db.py -v -k "relation_distance_filtered or tf_idf or comparison"`
Expected: FAIL — no such table (for all 4 new tests).

- [ ] **Step 3: Add the tables to `SCHEMA` in `src/db.py`**

Append to the `SCHEMA` string, after the existing `article_tags` table:

```python
CREATE TABLE IF NOT EXISTS relation_distance_filtered (
    file_id   TEXT NOT NULL,
    token     TEXT NOT NULL,
    frequency INTEGER NOT NULL,
    weight    REAL NOT NULL,
    PRIMARY KEY (file_id, token)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS tf_idf (
    word      TEXT NOT NULL PRIMARY KEY,
    freq      INTEGER NOT NULL,
    doc_count INTEGER NOT NULL,
    tf_idf    REAL NOT NULL
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS comparison (
    source_id TEXT NOT NULL,
    target_id TEXT NOT NULL,
    distance  REAL NOT NULL CHECK(distance > 0.0),
    PRIMARY KEY (source_id, target_id)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS comparison_seen (
    file_id TEXT PRIMARY KEY REFERENCES file_info(file_id)
);
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `conda run -n StudyAssistant python -m pytest tests/test_db.py -v`
Expected: PASS (all tests including pre-existing ones).

- [ ] **Step 5: Commit**

```bash
git add src/db.py tests/test_db.py
git commit -m "Add relation_distance_filtered/tf_idf/comparison/comparison_seen to Python schema"
```

---

## Task 2: Fix `ingest_pdf` content-hash idempotency gap

**Files:**
- Modify: `src/ingest.py`
- Test: `tests/test_ingest.py`

**Interfaces:**
- Consumes: Task 1's `relation_distance_filtered`/`comparison`/`comparison_seen` tables (must exist for `_purge_file`'s deletes to succeed even before the C++ engine has ever run).
- Produces: `_purge_file(conn: sqlite3.Connection, file_id: str) -> None` — cascading delete helper, used again nowhere else in this plan but kept as a named function for testability and clarity.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_ingest.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `conda run -n StudyAssistant python -m pytest tests/test_ingest.py -v -k content_change or orphan`
Expected: FAIL — current code returns early on any existing `file_id` row regardless of content, and never checks `source_path` for a stale row under a different `file_id`.

- [ ] **Step 3: Implement `_purge_file` and rewrite `ingest_pdf` in `src/ingest.py`**

```python
def _purge_file(conn: sqlite3.Connection, file_id: str) -> None:
    conn.execute("DELETE FROM article_tags WHERE file_id = ?", (file_id,))
    conn.execute("DELETE FROM relation_distance_filtered WHERE file_id = ?", (file_id,))
    conn.execute("DELETE FROM comparison WHERE source_id = ? OR target_id = ?", (file_id, file_id))
    conn.execute("DELETE FROM comparison_seen WHERE file_id = ?", (file_id,))
    conn.execute("DELETE FROM article_text WHERE file_id = ?", (file_id,))
    conn.execute("DELETE FROM file_info WHERE file_id = ?", (file_id,))


def ingest_pdf(
    pdf_path: Path, conn: sqlite3.Connection, token_freq_dir: Path = TOKEN_FREQ_DIR
) -> str:
    article = extract_article(pdf_path)

    existing = conn.execute(
        "SELECT content_sha256 FROM file_info WHERE file_id = ?", (article.file_id,)
    ).fetchone()
    if existing is not None and existing[0] == article.content_sha256:
        return article.file_id
    if existing is not None:
        _purge_file(conn, article.file_id)

    orphan = conn.execute(
        "SELECT file_id FROM file_info WHERE source_path = ? AND file_id != ?",
        (str(pdf_path), article.file_id),
    ).fetchone()
    if orphan is not None:
        _purge_file(conn, orphan[0])

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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `conda run -n StudyAssistant python -m pytest tests/test_ingest.py -v`
Expected: PASS (all tests including pre-existing idempotency test).

- [ ] **Step 5: Commit**

```bash
git add src/ingest.py tests/test_ingest.py
git commit -m "Fix ingest_pdf content-hash idempotency gap and orphaned-row cleanup"
```

---

## Task 3: `--incremental` flag for `relational_distance.cpp`

**Files:**
- Modify: `src/cpp/relational_distance.hpp`
- Modify: `src/cpp/relational_distance.cpp`
- Modify: `src/cpp/main.cpp`
- Test: `tests/test_relational_distance.py`

**Interfaces:**
- Produces: `relational_distance::Config.incremental` (bool, default `false`); CLI flag `--incremental` on `--compute-relational-distance`.

- [ ] **Step 1: Write the failing test**

Add to `tests/test_relational_distance.py`:

```python
def test_relational_distance_incremental_skips_existing_files(tmp_path, retrieval_engine_binary):
    db_path = _make_two_article_db(tmp_path)
    run_engine(retrieval_engine_binary, ["--compute-relational-distance", str(db_path)])

    conn = sqlite3.connect(db_path)
    before = conn.execute(
        "SELECT frequency, weight FROM relation_distance_filtered "
        "WHERE file_id = '10.1016/j.aej.2026.04.048' AND token = 'model'"
    ).fetchone()
    conn.close()

    token_freq_dir = tmp_path / "token_freq"
    conn = get_connection(db_path)
    ingest_pdf(
        ARTICLES_DIR / "A-closed-loop-control-strategy-for-automotive-EHB-base_2026_Control-Engineer.pdf",
        conn,
        token_freq_dir=token_freq_dir,
    )
    conn.close()

    run_engine(retrieval_engine_binary, ["--compute-relational-distance", str(db_path), "--incremental"])

    conn = sqlite3.connect(db_path)
    after = conn.execute(
        "SELECT frequency, weight FROM relation_distance_filtered "
        "WHERE file_id = '10.1016/j.aej.2026.04.048' AND token = 'model'"
    ).fetchone()
    new_file_count = conn.execute(
        "SELECT COUNT(*) FROM relation_distance_filtered WHERE file_id = '10.1016/j.conengprac.2026.106951'"
    ).fetchone()[0]

    assert after == before
    assert new_file_count > 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `conda run -n StudyAssistant python -m pytest tests/test_relational_distance.py::test_relational_distance_incremental_skips_existing_files -v`
Expected: FAIL — `--incremental` is an unrecognized flag (`unknown flag: --incremental`), or (before the flag parsing exists) the table gets dropped and only 3rd file exists incorrectly.

- [ ] **Step 3: Add `incremental` to `Config` in `relational_distance.hpp`**

```cpp
struct Config
{
    int max_token_length = 18;
    int min_token_frequency = 1;
    int persist_frequency_threshold = 3;
    bool incremental = false;
};
```

- [ ] **Step 4: Branch on `incremental` in `relational_distance.cpp`'s `compute()`**

Replace the table-creation and file-selection block (currently lines 29-46) with:

```cpp
        if (config.incremental)
        {
            sql_utils::execute(db, R"(
                CREATE TABLE IF NOT EXISTS relation_distance_filtered (
                    file_id   TEXT NOT NULL,
                    token     TEXT NOT NULL,
                    frequency INTEGER NOT NULL,
                    weight    REAL NOT NULL,
                    PRIMARY KEY (file_id, token)
                ) WITHOUT ROWID;
            )");
        }
        else
        {
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
        }

        std::string file_query = "SELECT file_id, token_freq_path FROM file_info;";
        if (config.incremental)
            file_query = "SELECT file_id, token_freq_path FROM file_info "
                         "WHERE file_id NOT IN (SELECT DISTINCT file_id FROM relation_distance_filtered);";

        sqlite3_stmt *file_stmt = sql_utils::prepare(db, file_query);
```

(The rest of `compute()` — reading token_freq JSON, filtering, computing weight, inserting — is unchanged.)

- [ ] **Step 5: Parse `--incremental` in `main.cpp`**

In the `--compute-relational-distance` branch's flag loop, add:

```cpp
                else if (args[i] == "--incremental")
                    config.incremental = true;
```

- [ ] **Step 6: Rebuild and run test to verify it passes**

Run: `conda run -n StudyAssistant python -m pytest tests/test_relational_distance.py -v`
Expected: PASS (all tests including the pre-existing `test_relational_distance_is_full_rebuild_not_incremental`, which still calls the subcommand with no flag and is unaffected).

- [ ] **Step 7: Commit**

```bash
git add src/cpp/relational_distance.hpp src/cpp/relational_distance.cpp src/cpp/main.cpp tests/test_relational_distance.py
git commit -m "Add --incremental flag to relational-distance stage"
```

---

## Task 4: `--incremental` flag + `comparison_seen` marker for `comparison.cpp`

**Files:**
- Modify: `src/cpp/comparison.hpp`
- Modify: `src/cpp/comparison.cpp`
- Modify: `src/cpp/main.cpp`
- Test: `tests/test_comparison.py`

**Interfaces:**
- Consumes: Task 3's incremental `relational_distance` stage (a realistic incremental workflow runs both stages with `--incremental`).
- Produces: `comparison::Config.incremental` (bool, default `false`); CLI flag `--incremental` on `--compute-comparison`; `comparison_seen(file_id)` table populated as a side effect of every `compute()` call (both modes).

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_comparison.py`:

```python
def test_comparison_incremental_skips_old_old_pairs(tmp_path, retrieval_engine_binary):
    db_path = tmp_path / "test.db"
    token_freq_dir = tmp_path / "token_freq"
    conn = get_connection(db_path)
    ingest_pdf(ARTICLES_DIR / FIXTURE_FILES[0], conn, token_freq_dir=token_freq_dir)
    ingest_pdf(ARTICLES_DIR / FIXTURE_FILES[1], conn, token_freq_dir=token_freq_dir)
    conn.close()

    run_engine(retrieval_engine_binary, ["--compute-relational-distance", str(db_path)])
    run_engine(retrieval_engine_binary, ["--compute-tfidf", str(db_path)])
    run_engine(retrieval_engine_binary,
               ["--compute-comparison", str(db_path), "--incremental", "--comparison-score-threshold", "0.0"])

    conn = sqlite3.connect(db_path)
    before = conn.execute(
        "SELECT distance FROM comparison WHERE source_id='10.1016/j.aej.2026.04.048' "
        "AND target_id='10.1016/j.ejcon.2026.101527'"
    ).fetchone()[0]
    conn.close()

    conn = get_connection(db_path)
    ingest_pdf(ARTICLES_DIR / FIXTURE_FILES[2], conn, token_freq_dir=token_freq_dir)
    ingest_pdf(ARTICLES_DIR / FIXTURE_FILES[3], conn, token_freq_dir=token_freq_dir)
    conn.close()

    run_engine(retrieval_engine_binary, ["--compute-relational-distance", str(db_path), "--incremental"])
    run_engine(retrieval_engine_binary, ["--compute-tfidf", str(db_path)])
    run_engine(retrieval_engine_binary,
               ["--compute-comparison", str(db_path), "--incremental", "--comparison-score-threshold", "0.0"])

    conn = sqlite3.connect(db_path)
    after = conn.execute(
        "SELECT distance FROM comparison WHERE source_id='10.1016/j.aej.2026.04.048' "
        "AND target_id='10.1016/j.ejcon.2026.101527'"
    ).fetchone()[0]
    new_pair_count = conn.execute(
        "SELECT COUNT(*) FROM comparison WHERE source_id = '10.1016/j.conengprac.2026.106951'"
    ).fetchone()[0]

    assert after == before
    assert new_pair_count > 0


def test_comparison_incremental_on_empty_db_equals_full(tmp_path, retrieval_engine_binary):
    db_path = _make_eight_article_db(tmp_path, retrieval_engine_binary)
    result = run_engine(retrieval_engine_binary,
                         ["--compute-comparison", str(db_path), "--incremental", "--comparison-score-threshold", "0.15"])
    assert result.returncode == 0, result.stderr

    conn = sqlite3.connect(db_path)
    assert conn.execute("SELECT COUNT(*) FROM comparison").fetchone()[0] == 54
    forward = conn.execute(
        "SELECT distance FROM comparison WHERE source_id='10.1016/j.conengprac.2026.106951' "
        "AND target_id='10.1016/j.ejcon.2026.101527'"
    ).fetchone()[0]
    assert forward == 0.6364606364488763
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `conda run -n StudyAssistant python -m pytest tests/test_comparison.py -v -k incremental`
Expected: FAIL — `unknown flag: --incremental`.

- [ ] **Step 3: Add `incremental` to `Config` in `comparison.hpp`**

```cpp
struct Config
{
    double comparison_score_threshold = 0.15;
    bool incremental = false;
};
```

- [ ] **Step 4: Rewrite `compute()` in `comparison.cpp`**

```cpp
#include "comparison.hpp"
#include "sql_utils.hpp"

#include <map>
#include <set>
#include <sqlite3.h>
#include <string>
#include <unordered_map>
#include <vector>

namespace comparison
{
    void compute(const std::filesystem::path &db_path, const Config &config)
    {
        sqlite3 *db = sql_utils::open(db_path.string());

        if (config.incremental)
        {
            sql_utils::execute(db, R"(
                CREATE TABLE IF NOT EXISTS comparison (
                    source_id TEXT NOT NULL,
                    target_id TEXT NOT NULL,
                    distance  REAL NOT NULL CHECK(distance > 0.0),
                    PRIMARY KEY (source_id, target_id)
                ) WITHOUT ROWID;
            )");
        }
        else
        {
            sql_utils::execute(db, "DROP TABLE IF EXISTS comparison;");
            sql_utils::execute(db, R"(
                CREATE TABLE comparison (
                    source_id TEXT NOT NULL,
                    target_id TEXT NOT NULL,
                    distance  REAL NOT NULL CHECK(distance > 0.0),
                    PRIMARY KEY (source_id, target_id)
                ) WITHOUT ROWID;
            )");
        }
        sql_utils::execute(db, R"(
            CREATE TABLE IF NOT EXISTS comparison_seen (
                file_id TEXT PRIMARY KEY
            );
        )");
        if (!config.incremental)
            sql_utils::execute(db, "DELETE FROM comparison_seen;");

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

        std::set<std::string> seen_ids;
        if (config.incremental)
        {
            sqlite3_stmt *seen_stmt = sql_utils::prepare(db, "SELECT file_id FROM comparison_seen;");
            while (sqlite3_step(seen_stmt) == SQLITE_ROW)
                seen_ids.insert(std::string(reinterpret_cast<const char *>(sqlite3_column_text(seen_stmt, 0))));
            sqlite3_finalize(seen_stmt);
        }

        std::vector<std::string> file_ids;
        for (const auto &[file_id, _] : by_file)
            file_ids.push_back(file_id);

        std::vector<std::string> new_ids;
        for (const auto &file_id : file_ids)
            if (seen_ids.find(file_id) == seen_ids.end())
                new_ids.push_back(file_id);

        if (config.incremental && new_ids.empty())
        {
            sqlite3_close(db);
            return;
        }
        std::set<std::string> new_id_set(new_ids.begin(), new_ids.end());

        std::unordered_map<std::string, double> tfidf_by_word;
        sqlite3_stmt *tfidf_stmt = sql_utils::prepare(db, "SELECT word, tf_idf FROM tf_idf;");
        while (sqlite3_step(tfidf_stmt) == SQLITE_ROW)
        {
            std::string word(reinterpret_cast<const char *>(sqlite3_column_text(tfidf_stmt, 0)));
            tfidf_by_word[word] = sqlite3_column_double(tfidf_stmt, 1);
        }
        sqlite3_finalize(tfidf_stmt);

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

        sqlite3_stmt *insert_stmt = sql_utils::prepare(db, "INSERT OR REPLACE INTO comparison (source_id, target_id, distance) VALUES (?, ?, ?);");

        sql_utils::execute(db, "BEGIN TRANSACTION;");
        for (const auto &source_id : file_ids)
        {
            const auto &source_boosted = boosted_by_file[source_id];
            for (const auto &target_id : file_ids)
            {
                if (source_id == target_id)
                    continue;
                if (config.incremental
                    && new_id_set.find(source_id) == new_id_set.end()
                    && new_id_set.find(target_id) == new_id_set.end())
                    continue;

                const auto &target_plain = by_file[target_id];

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

        sqlite3_stmt *mark_seen_stmt = sql_utils::prepare(db, "INSERT OR IGNORE INTO comparison_seen (file_id) VALUES (?);");
        for (const auto &file_id : file_ids)
        {
            sqlite3_bind_text(mark_seen_stmt, 1, file_id.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_step(mark_seen_stmt);
            sqlite3_reset(mark_seen_stmt);
        }
        sqlite3_finalize(mark_seen_stmt);

        sql_utils::execute(db, "COMMIT;");

        sqlite3_finalize(insert_stmt);
        sqlite3_close(db);
    }
}
```

Note: in non-incremental mode, `new_ids` ends up equal to `file_ids` (since `comparison_seen` was just cleared), so the `new_id_set` skip-condition never triggers — every pair is computed exactly as before.

- [ ] **Step 5: Parse `--incremental` in `main.cpp`**

In the `--compute-comparison` branch's flag loop, add:

```cpp
                else if (args[i] == "--incremental")
                    config.incremental = true;
```

- [ ] **Step 6: Rebuild and run tests to verify they pass**

Run: `conda run -n StudyAssistant python -m pytest tests/test_comparison.py -v`
Expected: PASS (all tests, including pre-existing ones using the default full-rebuild path).

- [ ] **Step 7: Commit**

```bash
git add src/cpp/comparison.hpp src/cpp/comparison.cpp src/cpp/main.cpp tests/test_comparison.py
git commit -m "Add --incremental flag and comparison_seen marker table to comparison stage"
```

---

## Task 5: Orchestration entrypoint `src/build.py`

> **Correction during implementation:** the module was actually named `src/pipeline.py` (test file `tests/test_pipeline.py`), not `src/build.py`/`tests/test_build.py` as originally planned below. Reason: a plain top-level module named `build` collides with the repo-root `build/` CMake output directory -- under `python -m pytest`, the repo root ends up on `sys.path`, and Python resolves `import build` to that directory as a namespace package instead of the intended `src/build.py`. Discovered via a real failing test (`AttributeError: <module 'build' (namespace)> ... has no attribute 'ENGINE_BINARY'`), not guessed at. All code below is otherwise unchanged; mentally substitute `build.py`→`pipeline.py` and `build_module`→`pipeline_module`.

**Files:**
- Create: `src/pipeline.py`
- Test: `tests/test_pipeline.py`

**Interfaces:**
- Consumes: `ingest_folder` (`src/ingest.py`), `tag_all_articles` (`src/tag_topics.py`), `get_connection` (`src/db.py`), `ARTICLES_DIR`/`DB_PATH`/`REPO_ROOT` (`src/config.py`).
- Produces: `build(folder: Path = ARTICLES_DIR, db_path: Path = DB_PATH, full: bool = False) -> None`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_build.py`:

```python
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from config import ARTICLES_DIR

import build as build_module

FIXTURE_FILES = [
    "Bifurcation-analysis-for-a-COVID-19-cell-model-with-pa_2026_Alexandria-Engin.pdf",
    "Reduced-order-echo-state-networks-for-model-predictive_2026_European-Journal.pdf",
]
EXTRA_FILE = "A-closed-loop-control-strategy-for-automotive-EHB-base_2026_Control-Engineer.pdf"


def _fixture_folder(tmp_path, filenames):
    folder = tmp_path / "corpus"
    folder.mkdir()
    for filename in filenames:
        (folder / filename).write_bytes((ARTICLES_DIR / filename).read_bytes())
    return folder


def test_build_populates_all_tables(tmp_path, retrieval_engine_binary, monkeypatch):
    monkeypatch.setattr(build_module, "ENGINE_BINARY", retrieval_engine_binary)
    folder = _fixture_folder(tmp_path, FIXTURE_FILES)
    db_path = tmp_path / "studyapp.db"

    build_module.build(folder=folder, db_path=db_path, full=True)

    conn = sqlite3.connect(db_path)
    assert conn.execute("SELECT COUNT(*) FROM file_info").fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM article_tags").fetchone()[0] > 0
    assert conn.execute("SELECT COUNT(*) FROM relation_distance_filtered").fetchone()[0] > 0
    assert conn.execute("SELECT COUNT(*) FROM tf_idf").fetchone()[0] > 0
    assert conn.execute("SELECT COUNT(*) FROM comparison").fetchone()[0] > 0


def test_build_incremental_adds_new_file_without_touching_old_pairs(tmp_path, retrieval_engine_binary, monkeypatch):
    monkeypatch.setattr(build_module, "ENGINE_BINARY", retrieval_engine_binary)
    folder = _fixture_folder(tmp_path, FIXTURE_FILES)
    db_path = tmp_path / "studyapp.db"

    build_module.build(folder=folder, db_path=db_path, full=True)
    conn = sqlite3.connect(db_path)
    before = conn.execute(
        "SELECT distance FROM comparison WHERE source_id='10.1016/j.aej.2026.04.048' "
        "AND target_id='10.1016/j.ejcon.2026.101527'"
    ).fetchone()[0]
    conn.close()

    (folder / EXTRA_FILE).write_bytes((ARTICLES_DIR / EXTRA_FILE).read_bytes())
    build_module.build(folder=folder, db_path=db_path, full=False)

    conn = sqlite3.connect(db_path)
    after = conn.execute(
        "SELECT distance FROM comparison WHERE source_id='10.1016/j.aej.2026.04.048' "
        "AND target_id='10.1016/j.ejcon.2026.101527'"
    ).fetchone()[0]
    assert after == before
    assert conn.execute("SELECT COUNT(*) FROM file_info").fetchone()[0] == 3
    assert conn.execute(
        "SELECT COUNT(*) FROM comparison WHERE source_id = '10.1016/j.conengprac.2026.106951'"
    ).fetchone()[0] > 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `conda run -n StudyAssistant python -m pytest tests/test_build.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'build'`.

- [ ] **Step 3: Create `src/build.py`**

```python
import argparse
import os
import subprocess
from pathlib import Path

from config import ARTICLES_DIR, DB_PATH, REPO_ROOT
from db import get_connection
from ingest import ingest_folder
from tag_topics import tag_all_articles

ENGINE_BINARY = REPO_ROOT / "build" / "retrieval_engine.exe"
UCRT64_BIN = r"C:\msys64\ucrt64\bin"


def _run_engine(args: list[str]) -> None:
    env = dict(os.environ)
    env["PATH"] = UCRT64_BIN + os.pathsep + env.get("PATH", "")
    result = subprocess.run([str(ENGINE_BINARY), *args], env=env)
    if result.returncode != 0:
        raise RuntimeError(f"retrieval_engine {args[0]} failed with exit code {result.returncode}")


def build(folder: Path = ARTICLES_DIR, db_path: Path = DB_PATH, full: bool = False) -> None:
    if not ENGINE_BINARY.exists():
        raise FileNotFoundError(f"{ENGINE_BINARY} not found -- build it first (see run.bat/run.sh)")

    conn = get_connection(db_path)
    try:
        ingest_folder(folder, conn)
        tag_all_articles(conn)
    finally:
        conn.close()

    distance_args = ["--compute-relational-distance", str(db_path)]
    comparison_args = ["--compute-comparison", str(db_path)]
    if not full:
        distance_args.append("--incremental")
        comparison_args.append("--incremental")

    _run_engine(distance_args)
    _run_engine(["--compute-tfidf", str(db_path)])
    _run_engine(comparison_args)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the StudyApp V3 ingest + retrieval build pipeline.")
    parser.add_argument("--full", action="store_true", help="Force a full rebuild instead of an incremental one.")
    args = parser.parse_args()
    build(full=args.full)
    print(f"Build complete: {DB_PATH}")
```

Note: `ENGINE_BINARY` is a module-level `Path` used directly (not passed as a parameter) so tests can `monkeypatch.setattr(build_module, "ENGINE_BINARY", ...)` to point at the session-scoped `retrieval_engine_binary` fixture's exe without needing a real cmake build inside the test itself.

- [ ] **Step 4: Run tests to verify they pass**

Run: `conda run -n StudyAssistant python -m pytest tests/test_build.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/build.py tests/test_build.py
git commit -m "Add src/build.py orchestration entrypoint tying ingest+tag+C++ stages together"
```

---

## Task 6: Incremental-vs-full equivalence integration test

**Files:**
- Modify: `tests/test_end_to_end_real_corpus.py` (add a new test function; existing test stays as-is)

**Interfaces:**
- Consumes: `build.build` (Task 5), `run_engine`/`retrieval_engine_binary` (`tests/conftest.py`).

- [ ] **Step 1: Write the test**

Add to `tests/test_end_to_end_real_corpus.py`:

```python
def test_incremental_build_equals_full_rebuild_for_new_pairs(tmp_path, retrieval_engine_binary):
    import build as build_module
    build_module.ENGINE_BINARY = retrieval_engine_binary

    folder = tmp_path / "corpus"
    folder.mkdir()
    for filename in FIXTURE_FILES[:6]:
        (folder / filename).write_bytes((ARTICLES_DIR / filename).read_bytes())

    incremental_db = tmp_path / "incremental.db"
    build_module.build(folder=folder, db_path=incremental_db, full=True)

    conn = sqlite3.connect(incremental_db)
    old_pairs_before = dict(
        conn.execute("SELECT source_id || '|' || target_id, distance FROM comparison").fetchall()
    )
    conn.close()

    for filename in FIXTURE_FILES[6:]:
        (folder / filename).write_bytes((ARTICLES_DIR / filename).read_bytes())
    build_module.build(folder=folder, db_path=incremental_db, full=False)

    conn = sqlite3.connect(incremental_db)
    old_pairs_after = dict(
        conn.execute(
            "SELECT source_id || '|' || target_id, distance FROM comparison "
            "WHERE source_id || '|' || target_id IN ({})".format(
                ",".join("?" * len(old_pairs_before))
            ),
            list(old_pairs_before.keys()),
        ).fetchall()
    )
    conn.close()
    assert old_pairs_after == old_pairs_before

    full_db = tmp_path / "full.db"
    full_folder = tmp_path / "full_corpus"
    full_folder.mkdir()
    for filename in FIXTURE_FILES:
        (full_folder / filename).write_bytes((ARTICLES_DIR / filename).read_bytes())
    build_module.build(folder=full_folder, db_path=full_db, full=True)

    conn_inc = sqlite3.connect(incremental_db)
    conn_full = sqlite3.connect(full_db)
    new_pairs = conn_inc.execute(
        "SELECT source_id, target_id, distance FROM comparison "
        "WHERE source_id NOT IN ({0}) OR target_id NOT IN ({0})".format(
            ",".join(f"'{fid}'" for fid in [
                "10.1016/j.aej.2026.04.048", "10.1016/j.ejcon.2026.101527",
                "10.1016/j.conengprac.2026.106951",
            ])
        )
    ).fetchall()
    assert len(new_pairs) > 0
    for source_id, target_id, distance in new_pairs:
        full_distance = conn_full.execute(
            "SELECT distance FROM comparison WHERE source_id = ? AND target_id = ?",
            (source_id, target_id),
        ).fetchone()
        assert full_distance is not None
        assert full_distance[0] == distance
    conn_inc.close()
    conn_full.close()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `conda run -n StudyAssistant python -m pytest tests/test_end_to_end_real_corpus.py::test_incremental_build_equals_full_rebuild_for_new_pairs -v`
Expected: FAIL before Tasks 3-5 land; PASS once they do (this test is written last as a capstone check, run it now purely to confirm it exercises real behavior rather than trivially passing).

- [ ] **Step 3: Run full test file to verify it passes**

Run: `conda run -n StudyAssistant python -m pytest tests/test_end_to_end_real_corpus.py -v`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add tests/test_end_to_end_real_corpus.py
git commit -m "Add incremental-vs-full-rebuild equivalence test for new comparison pairs"
```

---

## Task 7: `run.bat` and `run.sh`

**Files:**
- Create: `run.bat`
- Create: `run.sh`

- [ ] **Step 1: Create `run.bat`**

```bat
@echo off
setlocal
cd /d "%~dp0"

echo Building C++ retrieval engine...
cmake -B build -DCMAKE_PREFIX_PATH=C:/msys64/ucrt64 -G "MinGW Makefiles" . || exit /b 1
cmake --build build || exit /b 1

echo Setting up NLTK corpora...
conda run -n StudyAssistant python src/setup_nltk.py || exit /b 1

echo Running StudyApp V3 build pipeline...
conda run -n StudyAssistant python src/build.py %* || exit /b 1

echo Done.
```

- [ ] **Step 2: Create `run.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

echo "Building C++ retrieval engine..."
cmake -B build -DCMAKE_PREFIX_PATH=C:/msys64/ucrt64 -G "MinGW Makefiles" .
cmake --build build

echo "Setting up NLTK corpora..."
conda run -n StudyAssistant python src/setup_nltk.py

echo "Running StudyApp V3 build pipeline..."
conda run -n StudyAssistant python src/build.py "$@"

echo "Done."
```

- [ ] **Step 3: Verify manually**

Run: `./run.bat` (or `bash run.sh`) from the repo root.
Expected: exits 0, prints `Done.`, and `data/studyapp.db` exists afterward (first real run — this doubles as Task 8).

- [ ] **Step 4: Commit**

```bash
git add run.bat run.sh
git commit -m "Add run.bat/run.sh entrypoint scripts"
```

---

## Task 8: Run the pipeline for real against the full corpus

- [ ] **Step 1: Run `run.bat` (or `run.sh`) with no arguments** against the real `../articles/` corpus (280 PDFs).

- [ ] **Step 2: Verify row counts and sanity-check output**

```bash
conda run -n StudyAssistant python -c "
import sqlite3
conn = sqlite3.connect('data/studyapp.db')
print('file_info:', conn.execute('SELECT COUNT(*) FROM file_info').fetchone()[0])
print('article_tags:', conn.execute('SELECT COUNT(*) FROM article_tags').fetchone()[0])
print('comparison:', conn.execute('SELECT COUNT(*) FROM comparison').fetchone()[0])
print('self-pairs:', conn.execute('SELECT COUNT(*) FROM comparison WHERE source_id = target_id').fetchone()[0])
"
```

Expected: `file_info` == 280 (or current corpus count), `article_tags` > 0, `comparison` > 0, `self-pairs` == 0.

- [ ] **Step 2: No commit needed** — `data/` is gitignored; this step produces local data only, not a code change.

## Out of scope for this phase (documented, not deferred silently)

- Full-rebuild checkpoint/resume (V2's `low_similarity.txt` pattern) — YAGNI at current 280-article scale (sub-second full rebuild); revisit once corpus growth makes an interrupted full rebuild a real risk.
- Cross-platform (`.sh` for actual Linux/macOS, not just Git Bash on this Windows box) — no non-Windows toolchain exists anywhere in this project yet.
