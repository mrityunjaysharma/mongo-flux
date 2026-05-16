#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <cstdint>

#include <bsoncxx/document/view.hpp>
#include <bsoncxx/document/value.hpp>

#include "config.h"
#include "mongo_proxy.h"
#include "schema_mapping.h"
#include "clickhouse_client.h"
#include "query_translator.h"

namespace mongoflux {

/**
 * MongoDB Wire Protocol Proxy.
 *
 * Listens on a TCP port and speaks the MongoDB wire protocol (OP_MSG).
 * Applications connect to this port with a standard MongoDB driver (pymongo, mongocxx, etc.)
 * and MongoFlux transparently routes queries:
 *   - If the connection URI contains ?clickhouse=true → translate to SQL, execute on ClickHouse
 *   - Otherwise → forward to MongoDB secondary
 *   - Writes → forward to MongoDB primary
 *
 * Implements the minimum wire protocol surface for pymongo compatibility:
 *   - OP_MSG (opcode 2013) request/response
 *   - hello / ismaster handshake
 *   - find, aggregate, getMore, killCursors
 *   - insert, update, delete (forwarded to primary)
 *   - ping, buildInfo, whatsmyuri, saslStart/saslContinue (auth bypass for dev)
 */
class WireProxy {
public:
    struct ProxyConfig {
        int listen_port = 27020;
        std::string listen_bind = "0.0.0.0";
        bool clickhouse_routing = true;  // enable ClickHouse routing by default
    };

    WireProxy(const Config& config,
              const ProxyConfig& proxy_config,
              std::shared_ptr<SchemaMappingRegistry> registry,
              std::shared_ptr<ClickHouseClient> ch_client,
              std::shared_ptr<QueryTranslator> translator);
    ~WireProxy();

    /** Start the TCP listener. Non-blocking. */
    void start();

    /** Stop the proxy and close all connections. */
    void stop();

    /** Check if proxy is running. */
    bool is_running() const { return running_.load(); }

    /** Get the port the proxy is listening on. */
    int port() const { return proxy_config_.listen_port; }

private:
    /** Accept loop — runs in its own thread. */
    void accept_loop();

    /** Handle a single client connection. */
    void handle_client(int client_fd);

    /** Process an OP_MSG request and produce a response. */
    std::vector<uint8_t> process_op_msg(const std::vector<uint8_t>& body, int32_t request_id);

    /** Process an OP_QUERY request (used by pymongo for initial handshake). */
    std::vector<uint8_t> process_op_query(const std::vector<uint8_t>& body, int32_t request_id);

    /** Handle a specific command from the OP_MSG body. */
    std::vector<uint8_t> handle_command(const std::string& command,
                                         const bsoncxx::document::view& doc,
                                         int32_t request_id);

    /** Build an OP_MSG response frame from a BSON document. */
    std::vector<uint8_t> build_response(const bsoncxx::document::value& doc, int32_t response_to);

    /** Build an OP_REPLY response frame (for OP_QUERY responses). */
    std::vector<uint8_t> build_op_reply(const bsoncxx::document::value& doc, int32_t response_to);

    /** Build an OP_REPLY error response. */
    std::vector<uint8_t> build_op_reply_error(int32_t response_to, int code, const std::string& msg);

    /** Build a simple {ok: 1} response. */
    std::vector<uint8_t> ok_response(int32_t response_to);

    /** Build an error response. */
    std::vector<uint8_t> error_response(int32_t response_to, int code, const std::string& msg);

    /** Forward a write command to MongoDB primary. */
    bsoncxx::document::value forward_write(const std::string& db,
                                            const std::string& command,
                                            const bsoncxx::document::view& doc);

    /** Execute a find via MongoProxy (routes to CH or MongoDB). */
    bsoncxx::document::value execute_find(const std::string& db,
                                           const bsoncxx::document::view& doc);

    /** Execute an aggregate via MongoProxy (routes to CH or MongoDB). */
    bsoncxx::document::value execute_aggregate(const std::string& db,
                                                const bsoncxx::document::view& doc);

    Config config_;
    ProxyConfig proxy_config_;
    std::shared_ptr<SchemaMappingRegistry> registry_;
    std::shared_ptr<ClickHouseClient> ch_client_;
    std::shared_ptr<QueryTranslator> translator_;
    std::atomic<bool> running_{false};
    int listen_fd_ = -1;
    std::thread accept_thread_;
    std::vector<std::thread> client_threads_;
};

} // namespace mongoflux
