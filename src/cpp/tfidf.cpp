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
