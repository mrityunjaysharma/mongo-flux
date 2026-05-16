#pragma once

#include <memory>
#include <string>
#include <vector>
#include <nlohmann/json.hpp>

#include "config.h"

namespace mongoflux {

/**
 * Result row from a ClickHouse query, represented as key-value pairs.
 */
using ResultRow = nlohmann::json;

/**
 * Query result set from ClickHouse.
 */
struct QueryResult {
    std::vector<std::string> columns;
    std::vector<ResultRow> rows;
    double elapsed_ms = 0.0;
    size_t rows_read = 0;
    size_t bytes_read = 0;
};

/**
 * ClickHouse HTTP client for executing queries and inserting data.
 * Uses the ClickHouse HTTP interface for compatibility and simplicity.
 */
class ClickHouseClient {
public:
    explicit ClickHouseClient(const ClickHouseConfig& config);
    ~ClickHouseClient();

    /** Execute a DDL or DML statement (CREATE TABLE, INSERT, etc.). */
    void execute(const std::string& sql);

    /** Execute a SELECT query and return results as JSON rows. */
    QueryResult query(const std::string& sql);

    /** Batch insert rows into a table. */
    void insert_batch(const std::string& database,
                      const std::string& table,
                      const std::vector<std::string>& columns,
                      const std::vector<std::vector<std::string>>& rows);

    /** Check if a table exists. */
    bool table_exists(const std::string& database, const std::string& table);

    /** Create a table from DDL. */
    void create_table(const std::string& ddl);

    /** Test connectivity. Throws on failure. */
    void ping();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace mongoflux
