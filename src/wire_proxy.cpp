#include "mongoflux/wire_proxy.h"

#include <iostream>
#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <poll.h>

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/types.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/uri.hpp>

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;
using bsoncxx::builder::basic::make_array;

namespace mongoflux {

// MongoDB Wire Protocol constants
static constexpr int32_t OP_MSG = 2013;
static constexpr int32_t OP_QUERY = 2004;
static constexpr int32_t OP_REPLY = 1;
static constexpr int32_t HEADER_SIZE = 16;  // messageLength(4) + requestID(4) + responseTo(4) + opCode(4)

// Read exactly n bytes from fd
static bool read_exact(int fd, void* buf, size_t n) {
    size_t total = 0;
    while (total < n) {
        ssize_t r = ::read(fd, static_cast<char*>(buf) + total, n - total);
        if (r <= 0) return false;
        total += r;
    }
    return true;
}

// Write exactly n bytes to fd
static bool write_exact(int fd, const void* buf, size_t n) {
    size_t total = 0;
    while (total < n) {
        ssize_t w = ::write(fd, static_cast<const char*>(buf) + total, n - total);
        if (w <= 0) return false;
        total += w;
    }
    return true;
}

// Extract int32 from little-endian bytes
static int32_t read_le32(const uint8_t* p) {
    return static_cast<int32_t>(p[0]) |
           (static_cast<int32_t>(p[1]) << 8) |
           (static_cast<int32_t>(p[2]) << 16) |
           (static_cast<int32_t>(p[3]) << 24);
}

// Write int32 as little-endian bytes
static void write_le32(uint8_t* p, int32_t v) {
    p[0] = static_cast<uint8_t>(v & 0xFF);
    p[1] = static_cast<uint8_t>((v >> 8) & 0xFF);
    p[2] = static_cast<uint8_t>((v >> 16) & 0xFF);
    p[3] = static_cast<uint8_t>((v >> 24) & 0xFF);
}

WireProxy::WireProxy(
    const Config& config,
    const ProxyConfig& proxy_config,
    std::shared_ptr<SchemaMappingRegistry> registry,
    std::shared_ptr<ClickHouseClient> ch_client,
    std::shared_ptr<QueryTranslator> translator)
    : config_(config)
    , proxy_config_(proxy_config)
    , registry_(std::move(registry))
    , ch_client_(std::move(ch_client))
    , translator_(std::move(translator)) {}

WireProxy::~WireProxy() {
    stop();
}

void WireProxy::start() {
    if (running_.load()) return;

    // Create TCP socket
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        std::cerr << "[mongoflux/wire] Failed to create socket: " << strerror(errno) << std::endl;
        return;
    }

    int opt = 1;
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(proxy_config_.listen_port);
    ::inet_pton(AF_INET, proxy_config_.listen_bind.c_str(), &addr.sin_addr);

    if (::bind(listen_fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        std::cerr << "[mongoflux/wire] Failed to bind port " << proxy_config_.listen_port
                  << ": " << strerror(errno) << std::endl;
        ::close(listen_fd_);
        listen_fd_ = -1;
        return;
    }

    if (::listen(listen_fd_, 128) < 0) {
        std::cerr << "[mongoflux/wire] Failed to listen: " << strerror(errno) << std::endl;
        ::close(listen_fd_);
        listen_fd_ = -1;
        return;
    }

    running_ = true;
    accept_thread_ = std::thread(&WireProxy::accept_loop, this);

    std::cout << "[mongoflux/wire] Wire protocol proxy listening on "
              << proxy_config_.listen_bind << ":" << proxy_config_.listen_port << std::endl;
}

void WireProxy::stop() {
    running_ = false;
    if (listen_fd_ >= 0) {
        ::shutdown(listen_fd_, SHUT_RDWR);
        ::close(listen_fd_);
        listen_fd_ = -1;
    }
    if (accept_thread_.joinable()) accept_thread_.join();
    for (auto& t : client_threads_) {
        if (t.joinable()) t.join();
    }
    client_threads_.clear();
}

void WireProxy::accept_loop() {
    while (running_.load()) {
        struct pollfd pfd{};
        pfd.fd = listen_fd_;
        pfd.events = POLLIN;

        int ret = ::poll(&pfd, 1, 500);  // 500ms timeout for graceful shutdown
        if (ret <= 0) continue;

        struct sockaddr_in client_addr{};
        socklen_t addr_len = sizeof(client_addr);
        int client_fd = ::accept(listen_fd_, reinterpret_cast<struct sockaddr*>(&client_addr), &addr_len);
        if (client_fd < 0) continue;

        // TCP_NODELAY for low latency
        int flag = 1;
        ::setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

        // Handle client in a new thread
        client_threads_.emplace_back(&WireProxy::handle_client, this, client_fd);
    }
}

void WireProxy::handle_client(int client_fd) {
    // Each client connection: read messages in a loop
    while (running_.load()) {
        // Read message header (16 bytes)
        uint8_t header[HEADER_SIZE];
        if (!read_exact(client_fd, header, HEADER_SIZE)) break;

        int32_t msg_length = read_le32(header);
        int32_t request_id = read_le32(header + 4);
        // int32_t response_to = read_le32(header + 8);  // unused for requests
        int32_t opcode = read_le32(header + 12);

        if (msg_length < HEADER_SIZE || msg_length > 48 * 1024 * 1024) break;  // sanity check

        // Read message body
        int32_t body_size = msg_length - HEADER_SIZE;
        std::vector<uint8_t> body(body_size);
        if (!read_exact(client_fd, body.data(), body_size)) break;

        // Process based on opcode
        std::vector<uint8_t> response;
        if (opcode == OP_MSG) {
            response = process_op_msg(body, request_id);
        } else if (opcode == OP_QUERY) {
            response = process_op_query(body, request_id);
        } else {
            // Unsupported opcode — send error
            response = error_response(request_id, 13, "Unsupported opcode");
        }

        // Send response
        if (!response.empty()) {
            if (!write_exact(client_fd, response.data(), response.size())) break;
        }
    }

    ::close(client_fd);
}

std::vector<uint8_t> WireProxy::process_op_query(const std::vector<uint8_t>& body, int32_t request_id) {
    // OP_QUERY format:
    //   flags(4) + fullCollectionName(cstring) + numberToSkip(4) + numberToReturn(4) + query(BSON)
    if (body.size() < 12) return build_op_reply_error(request_id, 13, "OP_QUERY too short");

    // Skip flags (4 bytes)
    size_t offset = 4;

    // Read fullCollectionName (null-terminated string)
    std::string ns;
    while (offset < body.size() && body[offset] != 0) {
        ns += static_cast<char>(body[offset]);
        offset++;
    }
    offset++; // skip null terminator

    // Skip numberToSkip(4) + numberToReturn(4)
    offset += 8;

    if (offset >= body.size()) return build_op_reply_error(request_id, 13, "OP_QUERY truncated");

    // Parse the query BSON document
    const uint8_t* bson_data = body.data() + offset;
    int32_t bson_size = read_le32(bson_data);
    if (bson_size <= 0 || offset + bson_size > body.size()) {
        return build_op_reply_error(request_id, 13, "OP_QUERY invalid BSON");
    }

    bsoncxx::document::view doc(bson_data, bson_size);

    // The initial handshake comes as OP_QUERY to "admin.$cmd"
    // Find the command name (first key)
    auto it = doc.begin();
    if (it == doc.end()) return build_op_reply_error(request_id, 13, "Empty OP_QUERY");

    std::string command((*it).key());

    // Handle hello/ismaster via the same handler, but wrap response in OP_REPLY format
    bsoncxx::document::value reply_doc = [&]() -> bsoncxx::document::value {
        if (command == "hello" || command == "ismaster" || command == "isMaster") {
            return make_document(
                kvp("ismaster", true),
                kvp("ok", 1.0),
                kvp("maxBsonObjectSize", 16777216),
                kvp("maxMessageSizeBytes", 48000000),
                kvp("maxWriteBatchSize", 100000),
                kvp("minWireVersion", 0),
                kvp("maxWireVersion", 21),
                kvp("readOnly", false),
                kvp("connectionId", 1)
            );
        } else if (command == "ping") {
            return make_document(kvp("ok", 1.0));
        } else if (command == "buildInfo") {
            return make_document(
                kvp("version", "7.0.0"),
                kvp("versionArray", make_array(7, 0, 0, 0)),
                kvp("ok", 1.0)
            );
        } else {
            // Forward to MongoDB
            try {
                return forward_write(config_.mongo.database, command, doc);
            } catch (...) {
                return make_document(kvp("ok", 1.0));
            }
        }
    }();

    return build_op_reply(reply_doc, request_id);
}

std::vector<uint8_t> WireProxy::build_op_reply(const bsoncxx::document::value& doc, int32_t response_to) {
    // OP_REPLY format:
    //   header(16) + responseFlags(4) + cursorID(8) + startingFrom(4) + numberReturned(4) + documents(BSON)
    auto view = doc.view();
    int32_t bson_size = static_cast<int32_t>(view.length());
    int32_t msg_length = HEADER_SIZE + 4 + 8 + 4 + 4 + bson_size;

    std::vector<uint8_t> response(msg_length);
    write_le32(response.data(), msg_length);           // messageLength
    write_le32(response.data() + 4, response_to + 1);  // requestID
    write_le32(response.data() + 8, response_to);      // responseTo
    write_le32(response.data() + 12, OP_REPLY);        // opCode = 1

    // responseFlags = 0 (no errors)
    write_le32(response.data() + 16, 0);
    // cursorID = 0
    std::memset(response.data() + 20, 0, 8);
    // startingFrom = 0
    write_le32(response.data() + 28, 0);
    // numberReturned = 1
    write_le32(response.data() + 32, 1);
    // BSON document
    std::memcpy(response.data() + 36, view.data(), bson_size);

    return response;
}

std::vector<uint8_t> WireProxy::build_op_reply_error(int32_t response_to, int code, const std::string& msg) {
    auto doc = make_document(
        kvp("ok", 0.0),
        kvp("errmsg", msg),
        kvp("code", code),
        kvp("codeName", "InternalError")
    );
    return build_op_reply(doc, response_to);
}

std::vector<uint8_t> WireProxy::process_op_msg(const std::vector<uint8_t>& body, int32_t request_id) {
    if (body.size() < 5) return error_response(request_id, 13, "Message too short");

    // OP_MSG format: flagBits(4) + sections...
    // uint32_t flags = read_le32(body.data());

    // Parse all sections — kind=0 is the command body, kind=1 is document sequences
    size_t offset = 4;  // skip flagBits
    bsoncxx::document::view main_doc;
    bool has_main_doc = false;

    // We need to merge document sequences into the main command document
    // pymongo sends: section0={insert:"coll", $db:"myapp"} + section1={documents:[...]}
    std::string seq_identifier;
    std::vector<bsoncxx::document::view> seq_documents;

    while (offset < body.size()) {
        uint8_t section_kind = body[offset];
        offset++;

        if (section_kind == 0) {
            // Kind 0: single BSON document (the command)
            const uint8_t* bson_data = body.data() + offset;
            int32_t bson_size = read_le32(bson_data);
            if (bson_size <= 0 || offset + bson_size > body.size()) {
                return error_response(request_id, 13, "Invalid BSON size in section 0");
            }
            main_doc = bsoncxx::document::view(bson_data, bson_size);
            has_main_doc = true;
            offset += bson_size;

        } else if (section_kind == 1) {
            // Kind 1: document sequence
            // Format: size(4) + identifier(cstring) + documents...
            const uint8_t* seq_start = body.data() + offset;
            int32_t seq_size = read_le32(seq_start);
            if (seq_size <= 0 || offset + seq_size > body.size()) {
                return error_response(request_id, 13, "Invalid section 1 size");
            }

            size_t seq_offset = 4;  // skip size

            // Read identifier (null-terminated string, e.g., "documents", "updates", "deletes")
            seq_identifier.clear();
            while (seq_offset < static_cast<size_t>(seq_size) && seq_start[seq_offset] != 0) {
                seq_identifier += static_cast<char>(seq_start[seq_offset]);
                seq_offset++;
            }
            seq_offset++;  // skip null terminator

            // Read documents
            while (seq_offset < static_cast<size_t>(seq_size)) {
                const uint8_t* doc_data = seq_start + seq_offset;
                int32_t doc_size = read_le32(doc_data);
                if (doc_size <= 0 || seq_offset + doc_size > static_cast<size_t>(seq_size)) break;
                seq_documents.push_back(bsoncxx::document::view(doc_data, doc_size));
                seq_offset += doc_size;
            }

            offset += seq_size;
        } else {
            // Unknown section kind — skip rest
            break;
        }
    }

    if (!has_main_doc) return error_response(request_id, 13, "No command document in OP_MSG");

    // If we have document sequences, we need to merge them into the command
    // before forwarding to MongoDB. Build a new document with the sequence embedded.
    if (!seq_documents.empty() && !seq_identifier.empty()) {
        bsoncxx::builder::basic::document merged;
        for (auto& elem : main_doc) {
            merged.append(kvp(elem.key(), elem.get_value()));
        }
        bsoncxx::builder::basic::array arr;
        for (auto& d : seq_documents) {
            arr.append(d);
        }
        merged.append(kvp(seq_identifier, arr.extract()));

        auto merged_doc = merged.extract();
        auto merged_view = merged_doc.view();

        // Find command name
        auto it = merged_view.begin();
        if (it == merged_view.end()) return error_response(request_id, 13, "Empty command");
        std::string command((*it).key());

        return handle_command(command, merged_view, request_id);
    }

    // No document sequences — use main_doc directly
    auto it = main_doc.begin();
    if (it == main_doc.end()) return error_response(request_id, 13, "Empty command");
    std::string command((*it).key());

    return handle_command(command, main_doc, request_id);
}

std::vector<uint8_t> WireProxy::handle_command(
    const std::string& command,
    const bsoncxx::document::view& doc,
    int32_t request_id) {

    // --- Handshake / Admin commands ---
    if (command == "hello" || command == "ismaster" || command == "isMaster") {
        auto reply = make_document(
            kvp("ismaster", true),
            kvp("ok", 1.0),
            kvp("maxBsonObjectSize", 16777216),
            kvp("maxMessageSizeBytes", 48000000),
            kvp("maxWriteBatchSize", 100000),
            kvp("minWireVersion", 0),
            kvp("maxWireVersion", 21),
            kvp("readOnly", false),
            kvp("connectionId", 1)
        );
        return build_response(reply, request_id);
    }

    if (command == "ping") {
        return ok_response(request_id);
    }

    if (command == "buildInfo") {
        auto reply = make_document(
            kvp("version", "7.0.0"),
            kvp("versionArray", make_array(7, 0, 0, 0)),
            kvp("ok", 1.0)
        );
        return build_response(reply, request_id);
    }

    if (command == "whatsmyuri") {
        auto reply = make_document(kvp("you", "127.0.0.1:0"), kvp("ok", 1.0));
        return build_response(reply, request_id);
    }

    if (command == "getLog") {
        auto reply = make_document(
            kvp("log", make_array()),
            kvp("totalLinesWritten", 0),
            kvp("ok", 1.0)
        );
        return build_response(reply, request_id);
    }

    // Auth bypass (for development — no real auth)
    if (command == "saslStart" || command == "saslContinue") {
        auto reply = make_document(
            kvp("conversationId", 1),
            kvp("done", true),
            kvp("payload", bsoncxx::types::b_binary{bsoncxx::binary_sub_type::k_binary, 0, nullptr}),
            kvp("ok", 1.0)
        );
        return build_response(reply, request_id);
    }

    if (command == "getFreeMonitoringStatus" || command == "atlasVersion") {
        return ok_response(request_id);
    }

    if (command == "endSessions" || command == "killCursors" || command == "abortTransaction" || command == "commitTransaction") {
        return ok_response(request_id);
    }

    // --- Read commands (routed via MongoProxy) ---
    if (command == "find") {
        try {
            auto reply = execute_find(config_.mongo.database, doc);
            return build_response(reply, request_id);
        } catch (const std::exception& e) {
            return error_response(request_id, 2, std::string("find failed: ") + e.what());
        }
    }

    if (command == "aggregate") {
        try {
            auto reply = execute_aggregate(config_.mongo.database, doc);
            return build_response(reply, request_id);
        } catch (const std::exception& e) {
            return error_response(request_id, 2, std::string("aggregate failed: ") + e.what());
        }
    }

    if (command == "getMore") {
        // No cursor support yet — return empty batch
        auto reply = make_document(
            kvp("cursor", make_document(
                kvp("id", int64_t(0)),
                kvp("ns", ""),
                kvp("nextBatch", make_array())
            )),
            kvp("ok", 1.0)
        );
        return build_response(reply, request_id);
    }

    if (command == "count") {
        // Forward count to MongoDB
        try {
            auto reply = forward_write(config_.mongo.database, command, doc);
            return build_response(reply, request_id);
        } catch (const std::exception& e) {
            return error_response(request_id, 2, std::string("count failed: ") + e.what());
        }
    }

    // --- Write commands (forwarded to MongoDB primary) ---
    if (command == "insert" || command == "update" || command == "delete" ||
        command == "findAndModify" || command == "createIndexes" || command == "dropIndexes" ||
        command == "create" || command == "drop") {
        try {
            auto reply = forward_write(config_.mongo.database, command, doc);
            return build_response(reply, request_id);
        } catch (const std::exception& e) {
            return error_response(request_id, 2, std::string(command) + " failed: " + e.what());
        }
    }

    // --- Database admin commands ---
    if (command == "listCollections" || command == "listDatabases" || command == "listIndexes" ||
        command == "collStats" || command == "dbStats" || command == "serverStatus" ||
        command == "hostInfo" || command == "getCmdLineOpts" || command == "getParameter" ||
        command == "replSetGetStatus") {
        try {
            auto reply = forward_write(config_.mongo.database, command, doc);
            return build_response(reply, request_id);
        } catch (const std::exception& e) {
            return error_response(request_id, 2, std::string(command) + " failed: " + e.what());
        }
    }

    // Unknown command — try forwarding to MongoDB
    try {
        auto reply = forward_write(config_.mongo.database, command, doc);
        return build_response(reply, request_id);
    } catch (...) {
        return error_response(request_id, 59, "Unknown command: " + command);
    }
}

std::vector<uint8_t> WireProxy::build_response(const bsoncxx::document::value& doc, int32_t response_to) {
    auto view = doc.view();
    int32_t bson_size = static_cast<int32_t>(view.length());

    // OP_MSG response: header(16) + flagBits(4) + sectionKind(1) + bson
    int32_t msg_length = HEADER_SIZE + 4 + 1 + bson_size;

    std::vector<uint8_t> response(msg_length);
    write_le32(response.data(), msg_length);          // messageLength
    write_le32(response.data() + 4, response_to + 1); // requestID (unique)
    write_le32(response.data() + 8, response_to);     // responseTo
    write_le32(response.data() + 12, OP_MSG);         // opCode

    // flagBits = 0
    write_le32(response.data() + 16, 0);

    // Section kind 0 (body)
    response[20] = 0;

    // BSON document
    std::memcpy(response.data() + 21, view.data(), bson_size);

    return response;
}

std::vector<uint8_t> WireProxy::ok_response(int32_t response_to) {
    auto doc = make_document(kvp("ok", 1.0));
    return build_response(doc, response_to);
}

std::vector<uint8_t> WireProxy::error_response(int32_t response_to, int code, const std::string& msg) {
    auto doc = make_document(
        kvp("ok", 0.0),
        kvp("errmsg", msg),
        kvp("code", code),
        kvp("codeName", "InternalError")
    );
    return build_response(doc, response_to);
}

bsoncxx::document::value WireProxy::forward_write(
    const std::string& db,
    const std::string& command,
    const bsoncxx::document::view& doc) {

    mongocxx::client client{mongocxx::uri{config_.mongo.uri}};

    // Strip session/cluster metadata that pymongo adds — we're creating a fresh connection
    bsoncxx::builder::basic::document clean_doc;
    for (auto& elem : doc) {
        std::string key(elem.key());
        if (key == "$clusterTime" || key == "$db" || key == "lsid" ||
            key == "txnNumber" || key == "$readPreference" || key == "apiVersion") {
            continue;
        }
        clean_doc.append(kvp(key, elem.get_value()));
    }

    auto database = client[db];
    auto result = database.run_command(clean_doc.extract());
    return bsoncxx::document::value(result.view());
}

bsoncxx::document::value WireProxy::execute_find(
    const std::string& db,
    const bsoncxx::document::view& doc) {

    // Extract collection name
    auto coll_elem = doc["find"];
    if (!coll_elem || coll_elem.type() != bsoncxx::type::k_string) {
        return make_document(kvp("ok", 0.0), kvp("errmsg", "missing collection name"));
    }
    std::string collection(coll_elem.get_string().value);

    // Check if this collection has a mapping → route to ClickHouse
    if (proxy_config_.clickhouse_routing && registry_->has_mapping(collection)) {
        // Extract filter, projection, sort, limit, skip
        auto filter_elem = doc["filter"];
        auto proj_elem = doc["projection"];
        auto sort_elem = doc["sort"];
        auto limit_elem = doc["limit"];
        auto skip_elem = doc["skip"];

        bsoncxx::document::view filter = filter_elem && filter_elem.type() == bsoncxx::type::k_document
            ? filter_elem.get_document().value : bsoncxx::document::view{};
        bsoncxx::document::view projection = proj_elem && proj_elem.type() == bsoncxx::type::k_document
            ? proj_elem.get_document().value : bsoncxx::document::view{};
        bsoncxx::document::view sort = sort_elem && sort_elem.type() == bsoncxx::type::k_document
            ? sort_elem.get_document().value : bsoncxx::document::view{};

        int64_t limit = 0, skip = 0;
        if (limit_elem) {
            if (limit_elem.type() == bsoncxx::type::k_int32) limit = limit_elem.get_int32().value;
            else if (limit_elem.type() == bsoncxx::type::k_int64) limit = limit_elem.get_int64().value;
        }
        if (skip_elem) {
            if (skip_elem.type() == bsoncxx::type::k_int32) skip = skip_elem.get_int32().value;
            else if (skip_elem.type() == bsoncxx::type::k_int64) skip = skip_elem.get_int64().value;
        }

        // Translate to SQL and execute on ClickHouse
        std::string sql = translator_->translate_find(collection, filter, projection, sort, limit, skip);
        auto result = ch_client_->query(sql);

        // Build cursor response
        bsoncxx::builder::basic::array batch_builder;
        for (auto& row : result.rows) {
            batch_builder.append(bsoncxx::from_json(row.dump()));
        }

        return make_document(
            kvp("cursor", make_document(
                kvp("id", int64_t(0)),
                kvp("ns", db + "." + collection),
                kvp("firstBatch", batch_builder.extract())
            )),
            kvp("ok", 1.0)
        );
    }

    // No mapping — forward to MongoDB secondary
    mongocxx::client client{mongocxx::uri{config_.mongo.uri}};
    auto database = client[db];

    // Strip session metadata before forwarding
    bsoncxx::builder::basic::document clean_doc;
    for (auto& elem : doc) {
        std::string key(elem.key());
        if (key == "$clusterTime" || key == "$db" || key == "lsid" ||
            key == "txnNumber" || key == "$readPreference" || key == "apiVersion") {
            continue;
        }
        clean_doc.append(kvp(key, elem.get_value()));
    }
    auto result = database.run_command(clean_doc.extract());
    return bsoncxx::document::value(result.view());
}

bsoncxx::document::value WireProxy::execute_aggregate(
    const std::string& db,
    const bsoncxx::document::view& doc) {

    // Extract collection name
    auto coll_elem = doc["aggregate"];
    if (!coll_elem || coll_elem.type() != bsoncxx::type::k_string) {
        return make_document(kvp("ok", 0.0), kvp("errmsg", "missing collection name"));
    }
    std::string collection(coll_elem.get_string().value);

    // Check if this collection has a mapping → route to ClickHouse
    if (proxy_config_.clickhouse_routing && registry_->has_mapping(collection)) {
        // Extract pipeline
        auto pipeline_elem = doc["pipeline"];
        if (!pipeline_elem || pipeline_elem.type() != bsoncxx::type::k_array) {
            return make_document(kvp("ok", 0.0), kvp("errmsg", "missing pipeline"));
        }

        std::vector<bsoncxx::document::view> stages;
        for (auto& stage : pipeline_elem.get_array().value) {
            if (stage.type() == bsoncxx::type::k_document) {
                stages.push_back(stage.get_document().value);
            }
        }

        // Translate to SQL and execute on ClickHouse
        std::string sql = translator_->translate_aggregate(collection, stages);
        auto result = ch_client_->query(sql);

        // Build cursor response
        bsoncxx::builder::basic::array batch_builder;
        for (auto& row : result.rows) {
            batch_builder.append(bsoncxx::from_json(row.dump()));
        }

        return make_document(
            kvp("cursor", make_document(
                kvp("id", int64_t(0)),
                kvp("ns", db + "." + collection),
                kvp("firstBatch", batch_builder.extract())
            )),
            kvp("ok", 1.0)
        );
    }

    // No mapping — forward to MongoDB
    mongocxx::client client{mongocxx::uri{config_.mongo.uri}};
    auto database = client[db];

    // Strip session metadata before forwarding
    bsoncxx::builder::basic::document clean_doc;
    for (auto& elem : doc) {
        std::string key(elem.key());
        if (key == "$clusterTime" || key == "$db" || key == "lsid" ||
            key == "txnNumber" || key == "$readPreference" || key == "apiVersion") {
            continue;
        }
        clean_doc.append(kvp(key, elem.get_value()));
    }
    auto result = database.run_command(clean_doc.extract());
    return bsoncxx::document::value(result.view());
}

} // namespace mongoflux
