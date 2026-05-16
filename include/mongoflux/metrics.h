#pragma once

#include <atomic>
#include <chrono>
#include <string>
#include <sstream>
#include <mutex>
#include <unordered_map>

namespace mongoflux {

/**
 * Simple metrics collector for observability.
 * Thread-safe counters and gauges exposed via /metrics endpoint (Prometheus format).
 */
class Metrics {
public:
    static Metrics& instance() {
        static Metrics m;
        return m;
    }

    // Counters (monotonically increasing)
    void inc_rows_synced(const std::string& collection, int64_t count = 1) {
        std::lock_guard<std::mutex> lock(mu_);
        rows_synced_[collection] += count;
        total_rows_synced_ += count;
    }

    void inc_flush_success() { flush_success_.fetch_add(1); }
    void inc_flush_failure() { flush_failure_.fetch_add(1); }
    void inc_oplog_entries_processed() { oplog_entries_.fetch_add(1); }
    void inc_oplog_reconnects() { oplog_reconnects_.fetch_add(1); }

    // Gauges (current value)
    void set_pending_rows(int64_t count) { pending_rows_.store(count); }
    void set_oplog_lag_ms(int64_t ms) { oplog_lag_ms_.store(ms); }
    void set_last_flush_duration_ms(int64_t ms) { last_flush_ms_.store(ms); }
    void set_sync_running(bool running) { sync_running_.store(running ? 1 : 0); }

    // Render Prometheus text format
    std::string render() const {
        std::ostringstream out;

        out << "# HELP mongoflux_rows_synced_total Total rows synced to ClickHouse\n"
            << "# TYPE mongoflux_rows_synced_total counter\n"
            << "mongoflux_rows_synced_total " << total_rows_synced_.load() << "\n\n";

        {
            std::lock_guard<std::mutex> lock(mu_);
            for (const auto& [coll, count] : rows_synced_) {
                out << "mongoflux_rows_synced{collection=\"" << coll << "\"} " << count << "\n";
            }
        }
        out << "\n";

        out << "# HELP mongoflux_flush_success_total Successful flush operations\n"
            << "# TYPE mongoflux_flush_success_total counter\n"
            << "mongoflux_flush_success_total " << flush_success_.load() << "\n\n";

        out << "# HELP mongoflux_flush_failure_total Failed flush operations\n"
            << "# TYPE mongoflux_flush_failure_total counter\n"
            << "mongoflux_flush_failure_total " << flush_failure_.load() << "\n\n";

        out << "# HELP mongoflux_oplog_entries_total Oplog entries processed\n"
            << "# TYPE mongoflux_oplog_entries_total counter\n"
            << "mongoflux_oplog_entries_total " << oplog_entries_.load() << "\n\n";

        out << "# HELP mongoflux_oplog_reconnects_total Oplog reconnection attempts\n"
            << "# TYPE mongoflux_oplog_reconnects_total counter\n"
            << "mongoflux_oplog_reconnects_total " << oplog_reconnects_.load() << "\n\n";

        out << "# HELP mongoflux_pending_rows Current rows pending flush\n"
            << "# TYPE mongoflux_pending_rows gauge\n"
            << "mongoflux_pending_rows " << pending_rows_.load() << "\n\n";

        out << "# HELP mongoflux_oplog_lag_ms Estimated oplog replication lag in ms\n"
            << "# TYPE mongoflux_oplog_lag_ms gauge\n"
            << "mongoflux_oplog_lag_ms " << oplog_lag_ms_.load() << "\n\n";

        out << "# HELP mongoflux_last_flush_duration_ms Duration of last flush in ms\n"
            << "# TYPE mongoflux_last_flush_duration_ms gauge\n"
            << "mongoflux_last_flush_duration_ms " << last_flush_ms_.load() << "\n\n";

        out << "# HELP mongoflux_sync_running Whether sync is currently active\n"
            << "# TYPE mongoflux_sync_running gauge\n"
            << "mongoflux_sync_running " << sync_running_.load() << "\n";

        return out.str();
    }

private:
    Metrics() = default;

    mutable std::mutex mu_;
    std::unordered_map<std::string, int64_t> rows_synced_;
    std::atomic<int64_t> total_rows_synced_{0};
    std::atomic<int64_t> flush_success_{0};
    std::atomic<int64_t> flush_failure_{0};
    std::atomic<int64_t> oplog_entries_{0};
    std::atomic<int64_t> oplog_reconnects_{0};
    std::atomic<int64_t> pending_rows_{0};
    std::atomic<int64_t> oplog_lag_ms_{0};
    std::atomic<int64_t> last_flush_ms_{0};
    std::atomic<int64_t> sync_running_{0};
};

} // namespace mongoflux
