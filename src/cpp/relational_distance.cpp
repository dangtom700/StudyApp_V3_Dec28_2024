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
