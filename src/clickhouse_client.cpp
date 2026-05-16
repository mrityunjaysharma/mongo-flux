#include "mongoflux/clickhouse_client.h"

#include <curl/curl.h>
#include <sstream>
#include <stdexcept>
#include <chrono>
#include <cstdio>
#include <cctype>

namespace mongoflux {

struct ClickHouseClient::Impl {
    ClickHouseConfig config;
    std::string base_url;

    Impl(const ClickHouseConfig& cfg) : config(cfg) {
        std::ostringstream url;
        bool use_tls = (cfg.port == 8443 || cfg.port == 443);
        url << (use_tls ? "https" : "http") << "://"
            << cfg.host << ":" << cfg.port << "/";
        base_url = url.str();
    }

    static size_t write_callback(void* contents, size_t size, size_t nmemb, std::string* output) {
        size_t total = size * nmemb;
        output->append(static_cast<char*>(contents), total);
        return total;
    }

    /** URL-encode a string for safe use in query parameters. */
    static std::string url_encode(const std::string& value) {
        std::string encoded;
        encoded.reserve(value.size() * 3);
        for (unsigned char c : value) {
            if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
                encoded += static_cast<char>(c);
            } else {
                char buf[4];
                std::snprintf(buf, sizeof(buf), "%%%02X", c);
                encoded += buf;
            }
        }
        return encoded;
    }

    /** RAII wrapper for CURL handles to prevent leaks. */
    struct CurlHandle {
        CURL* handle;
        CurlHandle() : handle(curl_easy_init()) {
            if (!handle) throw std::runtime_error("Failed to initialize CURL");
        }
        ~CurlHandle() { if (handle) curl_easy_cleanup(handle); }
        CurlHandle(const CurlHandle&) = delete;
        CurlHandle& operator=(const CurlHandle&) = delete;
        operator CURL*() { return handle; }
    };

    std::string http_query(const std::string& sql, bool expect_data = false) {
        CurlHandle curl;
        std::string response;
        std::string url = base_url;

        // Add database parameter (URL-encoded)
        url += "?database=" + url_encode(config.database);

        // Add credentials (URL-encoded to handle special characters)
        if (!config.user.empty()) {
            url += "&user=" + url_encode(config.user);
        }
        if (!config.password.empty()) {
            url += "&password=" + url_encode(config.password);
        }

        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, sql.c_str());
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, static_cast<long>(sql.size()));
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
        curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, 300L);
        curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 10L);

        CURLcode res = curl_easy_perform(curl);
        long http_code = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

        if (res != CURLE_OK) {
            throw std::runtime_error(std::string("ClickHouse HTTP request failed: ") +
                                     curl_easy_strerror(res));
        }

        if (http_code != 200) {
            throw std::runtime_error("ClickHouse error (HTTP " + std::to_string(http_code) +
                                     "): " + response);
        }

        return response;
    }
};

ClickHouseClient::ClickHouseClient(const ClickHouseConfig& config)
    : impl_(std::make_unique<Impl>(config)) {}

ClickHouseClient::~ClickHouseClient() = default;

void ClickHouseClient::execute(const std::string& sql) {
    impl_->http_query(sql);
}

QueryResult ClickHouseClient::query(const std::string& sql) {
    auto start = std::chrono::steady_clock::now();

    // Request JSON format for easy parsing
    std::string query_with_format = sql + " FORMAT JSONEachRow";
    std::string response = impl_->http_query(query_with_format, true);

    auto end = std::chrono::steady_clock::now();
    double elapsed = std::chrono::duration<double, std::milli>(end - start).count();

    QueryResult result;
    result.elapsed_ms = elapsed;

    // Parse JSONEachRow: one JSON object per line
    std::istringstream stream(response);
    std::string line;
    while (std::getline(stream, line)) {
        if (line.empty()) continue;
        try {
            auto row = nlohmann::json::parse(line);
            if (result.columns.empty()) {
                for (auto it = row.begin(); it != row.end(); ++it) {
                    result.columns.push_back(it.key());
                }
            }
            result.rows.push_back(std::move(row));
        } catch (const nlohmann::json::exception&) {
            // Skip malformed lines
        }
    }

    result.rows_read = result.rows.size();
    return result;
}

void ClickHouseClient::insert_batch(
    const std::string& database,
    const std::string& table,
    const std::vector<std::string>& columns,
    const std::vector<std::vector<std::string>>& rows) {

    if (rows.empty()) return;

    std::ostringstream sql;
    sql << "INSERT INTO " << database << "." << table << " (";
    for (size_t i = 0; i < columns.size(); ++i) {
        if (i > 0) sql << ", ";
        sql << columns[i];
    }
    sql << ") VALUES ";

    for (size_t r = 0; r < rows.size(); ++r) {
        if (r > 0) sql << ", ";
        sql << "(";
        for (size_t c = 0; c < rows[r].size(); ++c) {
            if (c > 0) sql << ", ";
            sql << rows[r][c];
        }
        sql << ")";
    }

    impl_->http_query(sql.str());
}

bool ClickHouseClient::table_exists(const std::string& database, const std::string& table) {
    std::string sql = "EXISTS TABLE " + database + "." + table + " FORMAT TabSeparated";
    std::string response = impl_->http_query(sql, true);
    return !response.empty() && response[0] == '1';
}

void ClickHouseClient::create_table(const std::string& ddl) {
    // Support multi-statement DDL (e.g., local table + Distributed table for clusters)
    // ClickHouse HTTP interface executes one statement per request
    std::istringstream stream(ddl);
    std::string statement;
    while (std::getline(stream, statement, ';')) {
        // Trim whitespace
        size_t start = statement.find_first_not_of(" \t\n\r");
        if (start == std::string::npos) continue;
        std::string trimmed = statement.substr(start);
        size_t end = trimmed.find_last_not_of(" \t\n\r");
        if (end != std::string::npos) trimmed = trimmed.substr(0, end + 1);
        if (trimmed.empty()) continue;

        impl_->http_query(trimmed);
    }
}

void ClickHouseClient::ping() {
    impl_->http_query("SELECT 1 FORMAT Null");
}

} // namespace mongoflux
