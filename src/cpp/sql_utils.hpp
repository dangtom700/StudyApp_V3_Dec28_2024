#ifndef SQL_UTILS_HPP
#define SQL_UTILS_HPP

#include <sqlite3.h>
#include <stdexcept>
#include <string>

namespace sql_utils
{
    inline void execute(sqlite3 *db, const std::string &sql)
    {
        char *error_message = nullptr;
        if (sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &error_message) != SQLITE_OK)
        {
            std::string message = error_message ? error_message : "unknown error";
            sqlite3_free(error_message);
            throw std::runtime_error("SQL execution failed: " + message + " (SQL: " + sql + ")");
        }
    }

    inline sqlite3_stmt *prepare(sqlite3 *db, const std::string &sql)
    {
        sqlite3_stmt *stmt = nullptr;
        if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK)
        {
            std::string message = sqlite3_errmsg(db);
            throw std::runtime_error("Failed to prepare statement: " + message + " (SQL: " + sql + ")");
        }
        return stmt;
    }

    inline sqlite3 *open(const std::string &db_path)
    {
        sqlite3 *db = nullptr;
        if (sqlite3_open(db_path.c_str(), &db) != SQLITE_OK)
        {
            std::string message = sqlite3_errmsg(db);
            sqlite3_close(db);
            throw std::runtime_error("Could not open database: " + message);
        }
        return db;
    }
}

#endif
