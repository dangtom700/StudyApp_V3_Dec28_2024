#ifndef TRANSFORMER
#define TRANSFORMER

#include <vector>
#include <map>
#include <filesystem>
#include <fstream>
#include <set>
#include <stdexcept>
#include <cmath>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace TRANSFORMER {

    // Compute the sum of all token frequencies in a given JSON object
    int compute_sum_token_json(const std::map<std::string, int>& tokens) {
        int result = 0;
        for (const auto& [key, value] : tokens) {
            result += value;
        }
        return result;
    }

    // Filter a set of tokens by maximum length and minimum frequency
    std::vector<std::tuple<std::string, int, double>> token_filter(const std::map<std::string, int>& tokens, const int& max_length, const int& min_value, const double& relational_distance) {
        std::vector<std::tuple<std::string, int, double>> result;
        for (const std::pair<std::string, int>& token : tokens) {
            // Check if every character of token is in alphabt abcdefghijklmnopqrstuvwxyz
            bool contains_only_letters = std::all_of(token.first.begin(), token.first.end(), [](char c) { return (c >= 'a' && c <= 'z'); });

            if (!contains_only_letters) {
                continue;
            }

            // Check if token is less than or equal to max_length and contains at least min_value letters
            if(token.first.length() <= max_length && token.second >= min_value) {
                result.push_back({token.first, token.second, static_cast<double>(token.second) / relational_distance});
            }
        }
        return result;
    }

    // Count the number of unique tokens in a given JSON object
    int count_unique_tokens(const std::map<std::string, int>& tokens) {
        return tokens.size();
    }

    // Parse a given JSON file and return the contents as a map
    std::map<std::string, int> json_to_map(const std::filesystem::path& json_file) {
        std::map<std::string, int> result;
        std::ifstream file(json_file);
        if (!file.is_open()) {
            throw std::runtime_error("Could not open JSON file: " + json_file.string());
        }

        json j;
        file >> j;

        for (auto it = j.begin(); it != j.end(); ++it) {
            result[it.key()] = it.value().get<int>();
        }

        return result;
    }

    // Compute the Euclidean norm of the given map of strings to integers
    double Pythagoras(const std::map<std::string, int>& tokens) {
        double result = 0.0;
        for (const auto& [key, value] : tokens) {
            result += value * value;
        }
        return std::sqrt(static_cast<double>(result));
    }

    // Compute the relational distance of each token in the given map
    std::vector<double> compute_relational_distance(const std::map<std::string, int>& tokens, const double& relational_distance) {
        std::vector<double> result;
        for (const auto& [key, value] : tokens) {
            result.push_back(static_cast<double>(value) / relational_distance);
        }
        return result;
    }

} // namespace TRANSFORMER

#endif // TRANSFORMER
