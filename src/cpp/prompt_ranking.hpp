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
