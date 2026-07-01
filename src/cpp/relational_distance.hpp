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
        bool incremental = false;
    };

    void compute(const std::filesystem::path &db_path, const Config &config);
}

#endif
