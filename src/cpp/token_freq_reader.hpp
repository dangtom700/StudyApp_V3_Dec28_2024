#ifndef TOKEN_FREQ_READER_HPP
#define TOKEN_FREQ_READER_HPP

#include <cctype>
#include <filesystem>
#include <fstream>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>

namespace token_freq_reader
{
    // Reads the flat JSON object shape src/modules/word_freq.py's tokenize()
    // + src/ingest.py always produce: {"word": count, ...}. Never nested;
    // keys are always alpha-only tokens (tokenize() guarantees
    // token.isalpha()), so this never needs to handle \" or \\ escapes.
    inline std::map<std::string, int> read_token_freq(const std::filesystem::path &json_path)
    {
        std::ifstream file(json_path, std::ios::binary);
        if (!file.is_open())
            throw std::runtime_error("Could not open token-frequency JSON file: " + json_path.string());

        std::ostringstream buffer;
        buffer << file.rdbuf();
        const std::string content = buffer.str();

        std::map<std::string, int> result;
        size_t pos = 0;
        const size_t len = content.size();

        auto skip_separators = [&]() {
            while (pos < len && (content[pos] == ' ' || content[pos] == '\t' ||
                                  content[pos] == '\n' || content[pos] == '\r' ||
                                  content[pos] == ',' || content[pos] == ':'))
                ++pos;
        };

        skip_separators();
        if (pos >= len || content[pos] != '{')
            throw std::runtime_error("Malformed token-frequency JSON (expected '{'): " + json_path.string());
        ++pos;

        while (true)
        {
            skip_separators();
            if (pos >= len)
                throw std::runtime_error("Malformed token-frequency JSON (unterminated object): " + json_path.string());
            if (content[pos] == '}')
            {
                ++pos;
                break;
            }
            if (content[pos] != '"')
                throw std::runtime_error("Malformed token-frequency JSON (expected key): " + json_path.string());
            ++pos;
            size_t key_start = pos;
            while (pos < len && content[pos] != '"')
                ++pos;
            std::string key = content.substr(key_start, pos - key_start);
            ++pos;

            skip_separators();
            size_t value_start = pos;
            while (pos < len && (std::isdigit(static_cast<unsigned char>(content[pos])) || content[pos] == '-'))
                ++pos;
            if (pos == value_start)
                throw std::runtime_error("Malformed token-frequency JSON (expected integer value for key '" + key + "'): " + json_path.string());
            int value = std::stoi(content.substr(value_start, pos - value_start));

            result[key] = value;
        }

        return result;
    }
}

#endif
