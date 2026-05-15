#include "mg_clickhouse/clickhouse_client.h"

#include <curl/curl.h>
#include <sstream>
#include <stdexcept>
#include <chrono>

namespace mg_clickhouse {

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

    std::string http_query(const std::string& sql, bool expect_data = false) {
        CURL* curl = curl_easy_init();
        if (!curl) {
            throw std::runtime_error("Failed to initialize CURL");
        }

        std::string response;
        std::string url = base_url;

        // Add database parameter
        url += "?database=" + config.database;

        // Add credentials
        if (!config.user.empty()) {
            url += "&user=" + config.user;
        }
        if (!config.password.empty()) {
            url += "&password=" + config.password;
        }

        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, sql.c_str());
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, static_cast<long>(sql.size()));
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
        curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, 300L);

        CURLcode res = curl_easy_perform(curl);
        long http_code = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
        curl_easy_cleanup(curl);

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
    impl_->http_query(ddl);
}

void ClickHouseClient::ping() {
    impl_->http_query("SELECT 1 FORMAT Null");
}

} // namespace mg_clickhouse
