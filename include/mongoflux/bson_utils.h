#pragma once

#include <string>
#include <vector>
#include <nlohmann/json.hpp>
#include <bsoncxx/document/view.hpp>
#include <bsoncxx/types.hpp>

#include "schema_mapping.h"

namespace mongoflux {

/**
 * Extract mapped fields from a BSON document into a JSON row.
 * Shared between oplog sync and change stream sync to avoid duplication.
 */
inline nlohmann::json extract_mapped_fields(
    const bsoncxx::document::view& doc,
    const CollectionMapping& mapping) {

    nlohmann::json row;
    for (const auto& field_map : mapping.fields) {
        auto elem = doc[field_map.mongo_field];
        if (elem) {
            switch (elem.type()) {
                case bsoncxx::type::k_int32:
                    row[field_map.ch_column] = elem.get_int32().value;
                    break;
                case bsoncxx::type::k_int64:
                    row[field_map.ch_column] = elem.get_int64().value;
                    break;
                case bsoncxx::type::k_double:
                    row[field_map.ch_column] = elem.get_double().value;
                    break;
                case bsoncxx::type::k_string:
                    row[field_map.ch_column] = std::string(elem.get_string().value);
                    break;
                case bsoncxx::type::k_bool:
                    row[field_map.ch_column] = elem.get_bool().value;
                    break;
                case bsoncxx::type::k_oid:
                    row[field_map.ch_column] = elem.get_oid().value.to_string();
                    break;
                case bsoncxx::type::k_date:
                    row[field_map.ch_column] = elem.get_date().to_int64();
                    break;
                default:
                    row[field_map.ch_column] = nullptr;
                    break;
            }
        } else {
            row[field_map.ch_column] = nullptr;
        }
    }
    return row;
}

/**
 * Escape a string value for ClickHouse SQL literal.
 */
inline std::string escape_ch_string(const std::string& val) {
    std::string escaped;
    escaped.reserve(val.size() + 2);
    escaped += '\'';
    for (char c : val) {
        if (c == '\'') escaped += "\\'";
        else if (c == '\\') escaped += "\\\\";
        else escaped += c;
    }
    escaped += '\'';
    return escaped;
}

/**
 * Convert a batch of JSON rows into ClickHouse-ready column values.
 * Returns columns and rows suitable for ClickHouseClient::insert_batch().
 */
inline void prepare_batch_for_insert(
    const std::vector<nlohmann::json>& batch,
    const CollectionMapping& mapping,
    std::vector<std::string>& out_columns,
    std::vector<std::vector<std::string>>& out_rows) {

    out_columns.clear();
    for (const auto& field : mapping.fields) {
        out_columns.push_back(field.ch_column);
    }

    out_rows.clear();
    out_rows.reserve(batch.size());

    for (const auto& row : batch) {
        std::vector<std::string> values;
        values.reserve(mapping.fields.size());
        for (const auto& field : mapping.fields) {
            auto it = row.find(field.ch_column);
            if (it == row.end() || it->is_null()) {
                values.push_back("NULL");
            } else if (it->is_string()) {
                values.push_back(escape_ch_string(it->get<std::string>()));
            } else {
                values.push_back(it->dump());
            }
        }
        out_rows.push_back(std::move(values));
    }
}

} // namespace mongoflux
