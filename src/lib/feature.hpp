#ifndef FEATURE_HPP
#define FEATURE_HPP

#include <filesystem>
#include <vector>
#include <map>
#include <fstream>
#include <memory> // For smart pointers
#include <sqlite3.h>

#include "utilities.hpp"
#include "env.hpp"
#include "transform.hpp"
#include "updateDB.hpp"

namespace FEATURE {
    
    /**
     * Execute a SQL query on the given database.
     *
     * @param db The database to execute the query on.
     * @param sql The SQL query to execute.
     *
     * @throws std::runtime_error if the query fails with an error message.
     */
    void execute_sql(sqlite3* db, const std::string& sql) {
        char* error_message = nullptr;
        int exit = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &error_message);
        if (exit != SQLITE_OK) {
            std::cerr << "Error executing SQL: " << error_message << std::endl;
            sqlite3_free(error_message);
            throw std::runtime_error("SQL execution failed");
        }
    }

    /**
     * Compute the relational distance of each token in the given map of strings to
     * integers and store the result in a SQLite database.
     *
     * @param filtered_files A vector of file paths to process.
     * @param show_progress If true, print progress messages to the console.
     * @param reset_table If true, reset the table before adding new data.
     * @param is_dumped If true, dump the data to a file.
     *
     * @throws std::runtime_error if the database connection or query fails.
     */
    void computeRelationalDistance(const std::vector<std::filesystem::path>& filtered_files,
                                const bool show_progress = true,
                                const bool reset_table = true,
                                const bool is_dumped = true) {
        try {
            // Set up SQLite database connection
            sqlite3* db;
            int exit = sqlite3_open(ENV_HPP::database_path.string().c_str(), &db);
            if (exit) {
                std::cerr << "Error opening SQLite database: " << sqlite3_errmsg(db) << std::endl;
                return;
            }

            // Disable synchronous mode to speed up inserts (optional)
            execute_sql(db, "PRAGMA synchronous = OFF;");

            // Create tables if reset_table is true
            if (reset_table) {
                std::string create_table_sql = R"(
                    DROP TABLE IF EXISTS file_token;
                    CREATE TABLE IF NOT EXISTS file_token (
                        file_name TEXT PRIMARY KEY,
                        total_tokens INTEGER,
                        unique_tokens INTEGER,
                        relational_distance REAL
                    );
                )";
                execute_sql(db, create_table_sql);

                create_table_sql = R"(
                    DROP TABLE IF EXISTS relation_distance;
                    CREATE TABLE IF NOT EXISTS relation_distance (
                        file_name TEXT,
                        Token TEXT,
                        frequency INTEGER,
                        relational_distance REAL,
                        PRIMARY KEY (file_name, Token)
                    );
                )";
                execute_sql(db, create_table_sql);
                std::cout << "Tables created successfully" << std::endl;
            }

            // Start a transaction to speed up multiple inserts
            execute_sql(db, "BEGIN TRANSACTION;");

            bool trigger_once = true;
            for (const std::filesystem::path& file : filtered_files) {
                if (trigger_once && is_dumped) {
                    trigger_once = false;
                    UTILITIES_HPP::Basic::reset_data_dumper(ENV_HPP::data_dumper_path);
                }

                std::map<std::string, int> json_map = TRANSFORMER::json_to_map(file);
                
                for (auto it = json_map.begin(); it != json_map.end();) {
                    const std::string& key = it->first;
                    const int value = it->second;

                    if (value < ENV_HPP::min_value || key.length() > ENV_HPP::max_length || 
                        !std::all_of(key.begin(), key.end(), [](char c) { return c >= 'a' && c <= 'z'; })) {
                        it = json_map.erase(it); // Safely erase invalid entries
                    } else {
                        ++it; // Move to the next element
                    }
                }

                DataEntry row = {
                    .path = file.stem().generic_string(),
                    .sum = TRANSFORMER::compute_sum_token_json(json_map),
                    .num_unique_tokens = TRANSFORMER::count_unique_tokens(json_map),
                    .relational_distance = TRANSFORMER::Pythagoras(json_map),
                };

                // Compute the relational distance of each token
                // Double gated to filter tokens
                row.filtered_tokens = TRANSFORMER::token_filter(json_map, ENV_HPP::max_length, ENV_HPP::min_value, row.relational_distance);

                // Dump the contents of a DataEntry to a file
                if (is_dumped) UTILITIES_HPP::Basic::data_entry_dump(row);

                // Insert the row into file_token table using a prepared statement
                std::string insert_sql = R"(
                    INSERT OR REPLACE INTO file_token (file_name, total_tokens, unique_tokens, relational_distance)
                    VALUES (?, ?, ?, ?);
                )";
                sqlite3_stmt* stmt;
                sqlite3_prepare_v2(db, insert_sql.c_str(), -1, &stmt, nullptr);
                sqlite3_bind_text(stmt, 1, row.path.c_str(), -1, SQLITE_STATIC);
                sqlite3_bind_int(stmt, 2, row.sum);
                sqlite3_bind_int(stmt, 3, row.num_unique_tokens);
                sqlite3_bind_double(stmt, 4, row.relational_distance);
                sqlite3_step(stmt);
                sqlite3_finalize(stmt);

                // Insert the filtered tokens into relation_distance table using a prepared statement
                insert_sql = R"(
                    INSERT OR REPLACE INTO relation_distance (file_name, token, frequency, relational_distance)
                    VALUES (?, ?, ?, ?);
                )";
                sqlite3_prepare_v2(db, insert_sql.c_str(), -1, &stmt, nullptr);
                for (const auto& token : row.filtered_tokens) {
                    sqlite3_bind_text(stmt, 1, row.path.c_str(), -1, SQLITE_STATIC);
                    sqlite3_bind_text(stmt, 2, std::get<0>(token).c_str(), -1, SQLITE_STATIC);
                    sqlite3_bind_int(stmt, 3, std::get<1>(token));
                    sqlite3_bind_double(stmt, 4, std::get<2>(token));
                    sqlite3_step(stmt);
                    sqlite3_reset(stmt); // Reset the statement for re-use
                }
                sqlite3_finalize(stmt);

                if (show_progress) {
                    std::cout << "Processed: " << file << std::endl;
                }
            }

            // Commit the transaction to apply all inserts
            execute_sql(db, "COMMIT TRANSACTION;");

            // Re-enable synchronous mode (optional, depending on your use case)
            execute_sql(db, "PRAGMA synchronous = FULL;");

            // Close the SQLite database connection
            sqlite3_close(db);
            std::cout << "Computing relational distance data finished" << std::endl;
        } catch (const std::exception& e) {
            std::cerr << "Error: " << e.what() << std::endl;
        }
    }


    /**
     * @brief Compute and store resource data from the given filtered files
     * 
     * @param filtered_files A vector of file paths to compute resource data from
     * @param show_progress Whether to show progress in the console
     * @param reset_table Whether to reset the resource data table, default is true
     * @param is_dumped Whether to dump resource data to a file, default is true
     * 
     * This function will compute resource data from the given filtered files and store it in a database.
     * The resource data includes the last write time, epoch time, chunk count, starting id, and ending id.
     * The function will also dump the resource data to a file if is_dumped is true.
     * If reset_table is true, the resource data table will be reset before computing the resource data.
     * If show_progress is true, the progress will be shown in the console.
     * If an error occurs, an error message will be printed to the console.
     */
    void computeResourceData(const std::vector<std::filesystem::path>& filtered_files,
                         const bool& show_progress = true,
                         const bool& reset_table = true,
                         const bool& is_dumped = true) {
        try {
            // Connect to the database
            sqlite3* db;
            int exit = sqlite3_open(ENV_HPP::database_path.string().c_str(), &db);
            if (exit != SQLITE_OK) {
                std::cerr << "Error opening database: " << sqlite3_errmsg(db) << std::endl;
                sqlite3_close(db);
                return;
            }

            // Disable synchronous mode for faster inserts
            execute_sql(db, "PRAGMA synchronous = OFF;");

            // Create or reset the table if required
            if (reset_table) {
                std::string create_table_sql = R"(
                    DROP TABLE IF EXISTS file_info;
                    CREATE TABLE IF NOT EXISTS file_info (
                        id TEXT PRIMARY KEY,
                        file_name TEXT NOT NULL,
                        file_path TEXT NOT NULL,
                        epoch_time INTEGER NOT NULL,
                        chunk_count INTEGER NOT NULL,
                        starting_id INTEGER NOT NULL,
                        ending_id INTEGER NOT NULL
                    );
                )";
                execute_sql(db, create_table_sql);
            }

            // Start a transaction for batch processing
            execute_sql(db, "BEGIN TRANSACTION;");

            // Prepare the insert statement (using "INSERT OR REPLACE" to handle both insert/update)
            std::string insert_sql = R"(
                INSERT OR REPLACE INTO file_info (id, file_name, file_path, epoch_time, chunk_count, starting_id, ending_id)
                VALUES (?, ?, ?, ?, ?, ?, ?);
            )";
            sqlite3_stmt* stmt;
            sqlite3_prepare_v2(db, insert_sql.c_str(), -1, &stmt, nullptr);

            bool trigger_once = true;
            for (const std::filesystem::path& file : filtered_files) {
                if (trigger_once && is_dumped) {
                    UTILITIES_HPP::Basic::reset_file_info_dumper(ENV_HPP::data_info_path);
                    trigger_once = false;
                }

                // Process the file
                DataInfo entry = {
                    .file_name = file.stem().generic_string(),
                    .file_path = UTILITIES_HPP::Basic::convertToBackslash(file.generic_string()),
                    .epoch_time = UPDATE_INFO::get_epoch_time(file),
                    .chunk_count = UPDATE_INFO::count_chunk_for_each_title(db, entry.file_path),
                    .starting_id = UPDATE_INFO::get_starting_id(db, entry.file_path),
                    .ending_id = UPDATE_INFO::get_ending_id(db, entry.file_path),
                };

                entry.id = UPDATE_INFO::create_unique_id(entry.file_path, entry.epoch_time, entry.chunk_count, entry.starting_id);

                // Export data info if needed
                if (is_dumped) UTILITIES_HPP::Basic::data_info_dump(entry);

                // Bind the values to the statement
                sqlite3_bind_text(stmt, 1, entry.id.c_str(), -1, SQLITE_STATIC);
                sqlite3_bind_text(stmt, 2, entry.file_name.c_str(), -1, SQLITE_STATIC);
                sqlite3_bind_text(stmt, 3, entry.file_path.c_str(), -1, SQLITE_STATIC);
                sqlite3_bind_int(stmt, 4, entry.epoch_time);
                sqlite3_bind_int(stmt, 5, entry.chunk_count);
                sqlite3_bind_int(stmt, 6, entry.starting_id);
                sqlite3_bind_int(stmt, 7, entry.ending_id);

                // Execute the statement
                if (sqlite3_step(stmt) != SQLITE_DONE) {
                    std::cerr << "Error inserting into file_info: " << sqlite3_errmsg(db) << std::endl;
                }

                // Reset the statement to use it again
                sqlite3_reset(stmt);

                if (show_progress) {
                    std::cout << "Processed: " << file << std::endl;
                }
            }

            // Finalize the prepared statement
            sqlite3_finalize(stmt);

            // Commit the transaction to apply all inserts
            execute_sql(db, "COMMIT TRANSACTION;");

            // Re-enable synchronous mode (optional, depending on use case)
            execute_sql(db, "PRAGMA synchronous = FULL;");

            // Close the database connection
            sqlite3_close(db);
            std::cout << "Computing resource data finished" << std::endl;

        } catch (const std::exception& e) {
            std::cerr << "Error: " << e.what() << std::endl;
        }
    }


    /**
     * @brief Process the prompt and compute the relational distance of the tokens in the JSON file to the titles in the database
     * 
     * This function will process the prompt and compute the relational distance of the tokens in the JSON file to the titles in the database.
     * The relational distance is computed using the Euclidean distance between each token in the JSON file and the tokens in the title.
     * The function will also print the first 25 results in descending order of relational distance.
     * If an error occurs, an error message will be printed to the console.
     */
    void processPrompt(const int& top_n = 100) {
        try {
            // Transform the JSON file into a map of processed tokens
            std::map<std::string, int> tokens = TRANSFORMER::json_to_map(ENV_HPP::buffer_json_path);
            int distance = TRANSFORMER::Pythagoras(tokens);
            std::vector<std::tuple<std::string, int, double>> filtered_tokens = TRANSFORMER::token_filter(tokens, 16, 1, distance);

            // Open database connection
            sqlite3* db;
            int exit = sqlite3_open(ENV_HPP::database_path.string().c_str(), &db);
            if (exit != SQLITE_OK) {
                std::cerr << "Error opening database: " << sqlite3_errmsg(db) << std::endl;
                return;
            }

            // Enable PRAGMAs for performance
            sqlite3_exec(db, "PRAGMA journal_mode=WAL;", nullptr, nullptr, nullptr);
            sqlite3_exec(db, "PRAGMA synchronous=OFF;", nullptr, nullptr, nullptr);
            sqlite3_exec(db, "PRAGMA temp_store=MEMORY;", nullptr, nullptr, nullptr);

            // Prepare the result vector
            std::vector<std::tuple<std::string, std::string, double>> RESULT;

            // Step 1: Load all the file_info data into memory
            std::string sql = "SELECT id, file_name FROM file_info;";
            sqlite3_stmt* stmt;
            exit = sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr);
            if (exit != SQLITE_OK) {
                std::cerr << "Error preparing statement (file_info): " << sqlite3_errmsg(db) << std::endl;
                sqlite3_close(db);
                return;
            }

            // Step 2: Load the relation_distance data for all filtered tokens in one go
            // Create a map to store relational distances by file_name and token
            std::map<std::string, std::map<std::string, double>> relation_distance_map;
            std::string token_in_clause;
            for (const auto& entry : filtered_tokens) {
                token_in_clause += "'" + std::get<0>(entry) + "',";
            }
            // Remove trailing comma
            if (!token_in_clause.empty()) token_in_clause.pop_back();

            std::string relation_distance_sql = "SELECT file_name, Token, Relational_distance FROM relation_distance WHERE Token IN (" + token_in_clause + ");";
            sqlite3_stmt* relation_stmt;
            exit = sqlite3_prepare_v2(db, relation_distance_sql.c_str(), -1, &relation_stmt, nullptr);
            if (exit != SQLITE_OK) {
                std::cerr << "Error preparing statement (relation_distance): " << sqlite3_errmsg(db) << std::endl;
                sqlite3_finalize(stmt);
                sqlite3_close(db);
                return;
            }

            // Populate the relation_distance_map
            while (sqlite3_step(relation_stmt) == SQLITE_ROW) {
                std::string file_name = reinterpret_cast<const char*>(sqlite3_column_text(relation_stmt, 0));
                std::string token = reinterpret_cast<const char*>(sqlite3_column_text(relation_stmt, 1));
                double relational_distance = sqlite3_column_double(relation_stmt, 2);
                relation_distance_map[file_name][token] = relational_distance;
            }
            sqlite3_finalize(relation_stmt); // Finalize statement after processing

            // Step 3: Process the file_info data and calculate distances using the map
            while (sqlite3_step(stmt) == SQLITE_ROW) {
                const unsigned char* id_text = sqlite3_column_text(stmt, 0);
                const unsigned char* file_name_text = sqlite3_column_text(stmt, 1);

                if (!id_text || !file_name_text) {
                    continue; // Skip the row if either value is null
                }

                std::string id = std::string(reinterpret_cast<const char*>(id_text));
                std::string file_name = std::string(reinterpret_cast<const char*>(file_name_text));
                double total_distance = 0;

                // Calculate total distance using the pre-fetched relation distances
                for (const auto& entry : filtered_tokens) {
                    const std::string& token = std::get<0>(entry);
                    const double relational_distance_weight = std::get<2>(entry);

                    // Lookup the relation distance in the map
                    if (relation_distance_map.count("title_" + id) && relation_distance_map["title_" + id].count(token)) {
                        total_distance += relational_distance_weight * relation_distance_map["title_" + id][token];
                    }
                }

                // Add the result to the RESULT vector
                RESULT.push_back({id, file_name, total_distance});
            }

            // Finalize statement and close the database
            sqlite3_finalize(stmt);
            sqlite3_close(db);

            // Sort the results by the largest relative distance
            std::sort(RESULT.begin(), RESULT.end(), [](const std::tuple<std::string, std::string, double>& a, const std::tuple<std::string, std::string, double>& b) {
                return std::get<2>(a) > std::get<2>(b);
            });

            // Print the first top results
            std::cout << "Top "<< top_n <<" Results:" << std::endl
                << "-----------------------------------------------------------------" << std::endl;
            for (int i = 0; i < top_n && i < RESULT.size(); i++) {
                std::cout << "ID: " << std::get<0>(RESULT[i]) << std::endl
                << "Distance: " << std::get<2>(RESULT[i]) << std::endl
                << "Name: [[" << std::get<1>(RESULT[i]) << ".pdf]]" << std::endl
                << "-----------------------------------------------------------------" << std::endl;
            }
        } catch (const std::exception& e) {
            std::cerr << "Error: " << e.what() << std::endl;
        }
    }


    void createGlobalTermsTable(const std::map<std::string, int>& global_terms,
                                const bool& show_progress = true, 
                                const bool& reset_table = true) {
        try {
            // Connect to the database
            sqlite3* db;
            int exit = sqlite3_open(ENV_HPP::database_path.string().c_str(), &db);
            if (exit != SQLITE_OK) {
                std::cerr << "Error opening database: " << sqlite3_errmsg(db) << std::endl;
                sqlite3_close(db);
                return;
            }
            // If reset_table is true, reset the global_terms table
            if (reset_table) {
                std::string drop_table_sql = "DROP TABLE IF EXISTS global_terms;";
                execute_sql(db, drop_table_sql);
            }

            // Prepare the SQL statement to create the global_terms table
            std::string create_table_sql = R"(
                CREATE TABLE IF NOT EXISTS global_terms (
                    term TEXT PRIMARY KEY,
                    count INTEGER,
                    frequency REAL
                );
            )";
            execute_sql(db, create_table_sql);

            // Prepare the SQL statement to insert data into the global_terms table
            std::string insert_sql = R"(
                INSERT INTO global_terms (term, count, frequency) VALUES (?, ?, ?);
            )";
            sqlite3_stmt* insert_stmt;
            exit = sqlite3_prepare_v2(db, insert_sql.c_str(), -1, &insert_stmt, nullptr);
            if (exit != SQLITE_OK) {
                std::cerr << "Error preparing statement: " << sqlite3_errmsg(db) << std::endl;
                sqlite3_finalize(insert_stmt);
                sqlite3_close(db);
                return;
            }

            // Filter the tokens by maximum length and minimum frequency
            int total_frequency = 0;
            for (const auto& entry : global_terms) {
                total_frequency += entry.second;
            }
            std::vector<std::tuple<std::string, int, double>> filtered_tokens = token_filter(global_terms, ENV_HPP::max_length, ENV_HPP::min_value, static_cast<double>(total_frequency));

            // Insert data into the global_terms table
            for (const auto& entry : filtered_tokens) {
                std::string term = std::get<0>(entry);
                int count = std::get<1>(entry);
                double frequency = std::get<2>(entry);

                sqlite3_bind_text(insert_stmt, 1, term.c_str(), -1, SQLITE_STATIC);
                sqlite3_bind_int(insert_stmt, 2, count);
                sqlite3_bind_double(insert_stmt, 3, frequency);

                exit = sqlite3_step(insert_stmt);
                if (exit != SQLITE_DONE) {
                    std::cerr << "Error inserting data: " << sqlite3_errmsg(db) << std::endl;
                    sqlite3_finalize(insert_stmt);
                    sqlite3_close(db);
                    return;
                }

                sqlite3_reset(insert_stmt);

                if (show_progress) {
                    std::cout << "Inserted " << term << " into global_terms table" << std::endl;
                }
            }

            // Finalize the statement and close the database
            sqlite3_finalize(insert_stmt);
            sqlite3_close(db);
        } catch (const std::exception& e) {
            std::cerr << "Error: " << e.what() << std::endl;
        }
    }
}

#endif // FEATURE_HPP