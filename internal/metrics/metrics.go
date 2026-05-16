package metrics

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
)

// Metrics collects observability data exposed via /metrics endpoint.
type Metrics struct {
	mu              sync.Mutex
	rowsSynced      map[string]int64
	totalRowsSynced atomic.Int64
	flushSuccess    atomic.Int64
	flushFailure    atomic.Int64
	oplogEntries    atomic.Int64
	oplogReconnects atomic.Int64
	pendingRows     atomic.Int64
	oplogLagMs      atomic.Int64
	lastFlushMs     atomic.Int64
	syncRunning     atomic.Int64
}

var instance *Metrics
var once sync.Once

// Instance returns the singleton metrics instance.
func Instance() *Metrics {
	once.Do(func() {
		instance = &Metrics{
			rowsSynced: make(map[string]int64),
		}
	})
	return instance
}

func (m *Metrics) IncRowsSynced(collection string, count int64) {
	m.mu.Lock()
	m.rowsSynced[collection] += count
	m.mu.Unlock()
	m.totalRowsSynced.Add(count)
}

func (m *Metrics) IncFlushSuccess()        { m.flushSuccess.Add(1) }
func (m *Metrics) IncFlushFailure()        { m.flushFailure.Add(1) }
func (m *Metrics) IncOplogEntries()        { m.oplogEntries.Add(1) }
func (m *Metrics) IncOplogReconnects()     { m.oplogReconnects.Add(1) }
func (m *Metrics) SetPendingRows(n int64)  { m.pendingRows.Store(n) }
func (m *Metrics) SetOplogLagMs(ms int64)  { m.oplogLagMs.Store(ms) }
func (m *Metrics) SetLastFlushMs(ms int64) { m.lastFlushMs.Store(ms) }
func (m *Metrics) SetSyncRunning(running bool) {
	if running {
		m.syncRunning.Store(1)
	} else {
		m.syncRunning.Store(0)
	}
}

// Render returns Prometheus text format metrics.
func (m *Metrics) Render() string {
	var sb strings.Builder

	sb.WriteString("# HELP mongoflux_rows_synced_total Total rows synced to ClickHouse\n")
	sb.WriteString("# TYPE mongoflux_rows_synced_total counter\n")
	sb.WriteString(fmt.Sprintf("mongoflux_rows_synced_total %d\n\n", m.totalRowsSynced.Load()))

	m.mu.Lock()
	for coll, count := range m.rowsSynced {
		sb.WriteString(fmt.Sprintf("mongoflux_rows_synced{collection=\"%s\"} %d\n", coll, count))
	}
	m.mu.Unlock()
	sb.WriteString("\n")

	sb.WriteString("# HELP mongoflux_flush_success_total Successful flush operations\n")
	sb.WriteString("# TYPE mongoflux_flush_success_total counter\n")
	sb.WriteString(fmt.Sprintf("mongoflux_flush_success_total %d\n\n", m.flushSuccess.Load()))

	sb.WriteString("# HELP mongoflux_flush_failure_total Failed flush operations\n")
	sb.WriteString("# TYPE mongoflux_flush_failure_total counter\n")
	sb.WriteString(fmt.Sprintf("mongoflux_flush_failure_total %d\n\n", m.flushFailure.Load()))

	sb.WriteString("# HELP mongoflux_oplog_entries_total Oplog entries processed\n")
	sb.WriteString("# TYPE mongoflux_oplog_entries_total counter\n")
	sb.WriteString(fmt.Sprintf("mongoflux_oplog_entries_total %d\n\n", m.oplogEntries.Load()))

	sb.WriteString("# HELP mongoflux_oplog_reconnects_total Oplog reconnection attempts\n")
	sb.WriteString("# TYPE mongoflux_oplog_reconnects_total counter\n")
	sb.WriteString(fmt.Sprintf("mongoflux_oplog_reconnects_total %d\n\n", m.oplogReconnects.Load()))

	sb.WriteString("# HELP mongoflux_pending_rows Current rows pending flush\n")
	sb.WriteString("# TYPE mongoflux_pending_rows gauge\n")
	sb.WriteString(fmt.Sprintf("mongoflux_pending_rows %d\n\n", m.pendingRows.Load()))

	sb.WriteString("# HELP mongoflux_oplog_lag_ms Estimated oplog replication lag in ms\n")
	sb.WriteString("# TYPE mongoflux_oplog_lag_ms gauge\n")
	sb.WriteString(fmt.Sprintf("mongoflux_oplog_lag_ms %d\n\n", m.oplogLagMs.Load()))

	sb.WriteString("# HELP mongoflux_last_flush_duration_ms Duration of last flush in ms\n")
	sb.WriteString("# TYPE mongoflux_last_flush_duration_ms gauge\n")
	sb.WriteString(fmt.Sprintf("mongoflux_last_flush_duration_ms %d\n\n", m.lastFlushMs.Load()))

	sb.WriteString("# HELP mongoflux_sync_running Whether sync is currently active\n")
	sb.WriteString("# TYPE mongoflux_sync_running gauge\n")
	sb.WriteString(fmt.Sprintf("mongoflux_sync_running %d\n", m.syncRunning.Load()))

	return sb.String()
}
