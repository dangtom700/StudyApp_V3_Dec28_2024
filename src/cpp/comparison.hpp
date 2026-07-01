#ifndef COMPARISON_HPP
#define COMPARISON_HPP

#include <filesystem>

namespace comparison
{
    struct Config
    {
        double comparison_score_threshold = 0.15;
        bool incremental = false;
    };

    void compute(const std::filesystem::path &db_path, const Config &config);
}

#endif
