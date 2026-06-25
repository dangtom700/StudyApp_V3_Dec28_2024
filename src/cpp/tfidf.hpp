#ifndef TFIDF_HPP
#define TFIDF_HPP

#include <filesystem>

namespace tfidf
{
    void compute(const std::filesystem::path &db_path);
}

#endif
