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
