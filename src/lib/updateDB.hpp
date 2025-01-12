#ifndef UPDATE_INFO
#define UPDATE_INFO

#include <sqlite3.h>
#include <string>
#include <filesystem>
#include <iostream>
#include <ctime>
#include <iomanip>
#include <chrono>
#include <sstream>

#include "env.hpp"
#include "utilities.hpp"

namespace UPDATE_INFO {
    
    /**
     * @brief Get the last write time of a file in epoch time format (seconds since January 1, 1970, 00:00:00 UTC)
     * 
     * @param path The path to the file
     * @return The last write time of the file in epoch time format
     */
    int get_epoch_time(const std::filesystem::path& path) {
        try {
            // Get the last write time of the file
            auto ftime = std::filesystem::last_write_time(path);

            // Convert file time to system time
            auto sctp = std::chrono::time_point_cast<std::chrono::system_clock::duration>(
                ftime - decltype(ftime)::clock::now() + std::chrono::system_clock::now()
            );

            // Get the epoch time in seconds
            auto epoch_time = std::chrono::system_clock::to_time_t(sctp);

            return static_cast<int>(epoch_time);
        } catch (const std::filesystem::filesystem_error& e) {
            std::cerr << "Error getting last write time for file: " << e.what() << std::endl;
            return -1; // Return -1 to indicate an error occurred
        }
    }

    /**
     * @brief Get the last write time of a file in a human-readable string format
     * 
     * @param path The path to the file
     * @return The last write time of the file as a string in the format "YYYY-MM-DD HH:MM:SS"
     * 
     * If an error occurs while retrieving the last write time (e.g., the file does not exist),
     * an error message is returned instead.
     */
    std::string get_last_write_time(const std::filesystem::path& path) {
        try {
            // Get the last write time of the file
            auto ftime = std::filesystem::last_write_time(path).time_since_epoch().count();

            // Convert to a system clock time point
            auto sctp = std::chrono::system_clock::from_time_t(ftime);
            std::time_t time = std::chrono::system_clock::to_time_t(sctp);

            // Convert to local time
            std::tm* localTime = std::localtime(&time);

            // Format the time into a string
            std::ostringstream oss;
            oss << std::put_time(localTime, "%Y-%m-%d %H:%M:%S");

            return oss.str();
        } catch (const std::filesystem::filesystem_error& e) {
            // Handle any errors that may occur, e.g., file does not exist
            return "Error: " + std::string(e.what());
        }
    }

    /**
     * @brief Create a unique identifier for a given file name and epoch time
     * 
     * @param path The path to the file
     * @param epoch_time The last write time of the file in epoch time format
     * @param chunk_count The number of chunks for the given file name
     * @param starting_id The starting ID for the given file name
     * @return A unique identifier in the format "encoded_file_name encoded_time encoded_chunk_count encoded_starting_id redundancy"
     * 
     * This function encodes the file name, epoch time, chunk count, and starting ID into a unique identifier. The redundancy is
     * calculated by XORing the encoded values together. The resulting string is concatenation of the encoded values and the
     * redundancy.
     */

    std::string create_unique_id(const std::filesystem::path& path, const int& epoch_time, const int& chunk_count, const int& starting_id) {
        // Calculate encoded file name
        uint64_t encoded_file_name = 0;
        for (char c : path.generic_string()) {
            encoded_file_name += static_cast<uint8_t>(c);
        }
        encoded_file_name *= std::max(1, chunk_count);  // Ensure chunk_count is at least 1
        encoded_file_name *= epoch_time;
        encoded_file_name &= 0xFFFFFFFFFFFFFFFF;  // Limit to 64 bits

        // Calculate encoded starting ID
        int mod_starting_id = (starting_id == 0) ? (epoch_time % 3600) : starting_id;
        uint32_t encoded_starting_id = static_cast<uint32_t>(mod_starting_id * ((chunk_count + 1) << 1));
        encoded_starting_id &= 0xFFFFFFFF;  // Limit to 32 bits

        // Use stringstream to generate hexadecimal representation
        std::stringstream ss;
        ss << std::hex << encoded_file_name << std::setw(8) << std::setfill('0') << encoded_starting_id;

        // Calculate redundancy value
        uint32_t redundancy = static_cast<uint32_t>(encoded_file_name ^ encoded_starting_id);

        // Append redundancy value in hexadecimal format
        ss << std::setw(8) << std::setfill('0') << redundancy;

        return ss.str();
    }

    /**
     * @brief Count the number of chunks for a given file name in the pdf_chunks table
     * 
     * @param db The database connection
     * @param file_name The file name to search for
     * @return The number of chunks if found, otherwise 0
     * 
     * This function executes a SELECT query on the pdf_chunks table, binding the given file_name to the ? placeholder.
     * If a row is returned, the chunk_count column is retrieved and returned as an int. Otherwise, 0 is returned.
     */
    int count_chunk_for_each_title(sqlite3* db, const std::string& file_name) {
        sqlite3_stmt* stmt;
        sqlite3_prepare_v2(db, "SELECT COUNT(chunk_index) FROM pdf_chunks WHERE file_name = ?;", -1, &stmt, NULL);
        sqlite3_bind_text(stmt, 1, file_name.c_str(), -1, SQLITE_STATIC);
        int chunk_count = 0;
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            chunk_count = sqlite3_column_int(stmt, 0);
        }
        sqlite3_finalize(stmt);
        return chunk_count;
    }

    /**
     * @brief Get the starting ID for a given file name from the pdf_chunks table
     * 
     * @param db The database connection
     * @param file_name The file name to search for
     * @return The starting ID if found, otherwise 0
     * 
     * This function executes a SELECT query on the pdf_chunks table, binding the given file_name to the ? placeholder.
     * If a row is returned, the starting_id column is retrieved and returned as an int. Otherwise, 0 is returned.
     */
    int get_starting_id(sqlite3* db, const std::string& file_name) {
        sqlite3_stmt* stmt;
        sqlite3_prepare_v2(db, "SELECT MIN(id) FROM pdf_chunks WHERE file_name = ?;", -1, &stmt, NULL);
        sqlite3_bind_text(stmt, 1, file_name.c_str(), -1, SQLITE_STATIC);
        int starting_id = 0;
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            starting_id = sqlite3_column_int(stmt, 0);
        }
        sqlite3_finalize(stmt);
        return starting_id;
    }

    /**
     * @brief Get the ending ID for a given file name from the pdf_chunks table
     * 
     * @param db The database connection
     * @param file_name The file name to search for
     * @return The ending ID if found, otherwise 0
     * 
     * This function executes a SELECT query on the pdf_chunks table, binding the given file_name to the ? placeholder.
     * If a row is returned, the ending_id column is retrieved and returned as an int. Otherwise, 0 is returned.
     */
    int get_ending_id(sqlite3* db, const std::string& file_name) {
        sqlite3_stmt* stmt;
        sqlite3_prepare_v2(db, "SELECT MAX(id) FROM pdf_chunks WHERE file_name = ?;", -1, &stmt, NULL);
        sqlite3_bind_text(stmt, 1, file_name.c_str(), -1, SQLITE_STATIC);
        int ending_id = 0;
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            ending_id = sqlite3_column_int(stmt, 0);
        }
        sqlite3_finalize(stmt);
        return ending_id;
    }
}

#endif // UPDATE_INFO