#include "mongoflux/routing.h"

#include <algorithm>
#include <sstream>

namespace mongoflux {

ParsedUri parse_mongo_uri(const std::string& uri) {
    ParsedUri result;

    // mongodb://[user:pass@]host[:port][/database][?params]
    std::string remaining = uri;

    // Extract scheme
    auto scheme_end = remaining.find("://");
    if (scheme_end != std::string::npos) {
        result.scheme = remaining.substr(0, scheme_end);
        remaining = remaining.substr(scheme_end + 3);
    } else {
        result.scheme = "mongodb";
    }

    // Strip credentials if present
    auto at_pos = remaining.find('@');
    if (at_pos != std::string::npos) {
        remaining = remaining.substr(at_pos + 1);
    }

    // Extract query parameters
    auto query_pos = remaining.find('?');
    if (query_pos != std::string::npos) {
        std::string query_string = remaining.substr(query_pos + 1);
        remaining = remaining.substr(0, query_pos);

        // Parse key=value pairs separated by &
        std::istringstream param_stream(query_string);
        std::string param;
        while (std::getline(param_stream, param, '&')) {
            auto eq_pos = param.find('=');
            if (eq_pos != std::string::npos) {
                std::string key = param.substr(0, eq_pos);
                std::string value = param.substr(eq_pos + 1);
                result.params[key] = value;
            } else {
                result.params[param] = "true";
            }
        }
    }

    // Extract database
    auto slash_pos = remaining.find('/');
    if (slash_pos != std::string::npos) {
        result.database = remaining.substr(slash_pos + 1);
        remaining = remaining.substr(0, slash_pos);
    }

    // Extract host and port
    auto colon_pos = remaining.find(':');
    if (colon_pos != std::string::npos) {
        result.host = remaining.substr(0, colon_pos);
        try {
            result.port = std::stoi(remaining.substr(colon_pos + 1));
        } catch (...) {
            result.port = 27017;
        }
    } else {
        result.host = remaining;
        result.port = 27017;
    }

    return result;
}

bool has_clickhouse_routing(const ParsedUri& uri, const std::string& param_name) {
    auto it = uri.params.find(param_name);
    if (it == uri.params.end()) return false;

    std::string value = it->second;
    std::transform(value.begin(), value.end(), value.begin(), ::tolower);
    return value == "true" || value == "1" || value == "yes";
}

} // namespace mongoflux
