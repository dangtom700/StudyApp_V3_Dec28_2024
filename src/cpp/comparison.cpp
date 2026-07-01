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
