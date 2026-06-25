#include "comparison.hpp"
#include "prompt_ranking.hpp"
#include "relational_distance.hpp"
#include "tfidf.hpp"

#include <cstdio>
#include <iostream>
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
        else if (subcommand == "--compute-tfidf")
        {
            if (args.size() < 2)
                throw std::runtime_error("missing db_path");
            tfidf::compute(args[1]);
        }
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
