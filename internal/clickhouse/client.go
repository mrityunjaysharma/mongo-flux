package clickhouse

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/mongoflux/mongoflux/internal/config"
)

// QueryResult holds the result of a ClickHouse SELECT query.
type QueryResult struct {
	Columns   []string                 `json:"columns"`
	Rows      []map[string]interface{} `json:"rows"`
	ElapsedMs float64                  `json:"elapsed_ms"`
	RowsRead  int                      `json:"rows_read"`
}

// Client is an HTTP-based ClickHouse client.
type Client struct {
	baseURL    string
	database   string
	user       string
	password   string
	httpClient *http.Client
}

// NewClient creates a new ClickHouse HTTP client with connection pooling.
func NewClient(cfg config.ClickHouseConfig) *Client {
	scheme := "http"
	if cfg.Port == 8443 || cfg.Port == 443 {
		scheme = "https"
	}
	baseURL := fmt.Sprintf("%s://%s:%d/", scheme, cfg.Host, cfg.Port)

	transport := &http.Transport{
		MaxIdleConns:        20,
		MaxIdleConnsPerHost: 20,
		IdleConnTimeout:     90 * time.Second,
	}

	return &Client{
		baseURL:  baseURL,
		database: cfg.Database,
		user:     cfg.User,
		password: cfg.Password,
		httpClient: &http.Client{
			Timeout:   300 * time.Second,
			Transport: transport,
		},
	}
}

func (c *Client) buildURL() string {
	u := c.baseURL + "?database=" + url.QueryEscape(c.database)
	if c.user != "" {
		u += "&user=" + url.QueryEscape(c.user)
	}
	if c.password != "" {
		u += "&password=" + url.QueryEscape(c.password)
	}
	return u
}

func (c *Client) doQuery(sql string) (string, error) {
	reqURL := c.buildURL()
	resp, err := c.httpClient.Post(reqURL, "text/plain", strings.NewReader(sql))
	if err != nil {
		return "", fmt.Errorf("clickhouse HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	// Limit response body to 64MB to prevent OOM on unexpected large responses
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024*1024))
	if err != nil {
		return "", fmt.Errorf("failed to read clickhouse response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		errMsg := strings.TrimSpace(string(body))
		if len(errMsg) > 500 {
			errMsg = errMsg[:500] + "..."
		}
		return "", fmt.Errorf("clickhouse error (HTTP %d): %s", resp.StatusCode, errMsg)
	}

	return string(body), nil
}

// Execute runs a DDL or DML statement.
func (c *Client) Execute(sql string) error {
	_, err := c.doQuery(sql)
	return err
}

// Query executes a SELECT and returns results as JSON rows.
func (c *Client) Query(sql string) (*QueryResult, error) {
	start := time.Now()

	response, err := c.doQuery(sql + " FORMAT JSONEachRow")
	if err != nil {
		return nil, err
	}

	elapsed := time.Since(start).Seconds() * 1000

	result := &QueryResult{ElapsedMs: elapsed}
	lines := strings.Split(strings.TrimSpace(response), "\n")

	for _, line := range lines {
		if line == "" {
			continue
		}
		var row map[string]interface{}
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			continue
		}
		if len(result.Columns) == 0 {
			for k := range row {
				result.Columns = append(result.Columns, k)
			}
		}
		result.Rows = append(result.Rows, row)
	}

	result.RowsRead = len(result.Rows)
	return result, nil
}

// InsertBatch inserts rows into a table. Database and table names are validated.
func (c *Client) InsertBatch(database, table string, columns []string, rows [][]string) error {
	if len(rows) == 0 {
		return nil
	}

	// Validate identifiers to prevent SQL injection
	if !isValidIdent(database) {
		return fmt.Errorf("invalid database name: %q", database)
	}
	if !isValidIdent(table) {
		return fmt.Errorf("invalid table name: %q", table)
	}
	for _, col := range columns {
		if !isValidIdent(col) {
			return fmt.Errorf("invalid column name: %q", col)
		}
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "INSERT INTO %s.%s (%s) VALUES ",
		database, table, strings.Join(columns, ", "))

	for i, row := range rows {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("(")
		sb.WriteString(strings.Join(row, ", "))
		sb.WriteString(")")
	}

	_, err := c.doQuery(sb.String())
	return err
}

// isValidIdent checks that a string is safe for use as a SQL identifier.
func isValidIdent(s string) bool {
	if s == "" || len(s) > 128 {
		return false
	}
	for i, c := range s {
		if c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') {
			continue
		}
		if i > 0 && (c >= '0' && c <= '9') {
			continue
		}
		return false
	}
	return true
}

// TableExists checks if a table exists.
func (c *Client) TableExists(database, table string) (bool, error) {
	sql := fmt.Sprintf("EXISTS TABLE %s.%s FORMAT TabSeparated", database, table)
	response, err := c.doQuery(sql)
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(response) == "1", nil
}

// CreateTable executes DDL (supports multi-statement separated by semicolons).
func (c *Client) CreateTable(ddl string) error {
	statements := strings.Split(ddl, ";")
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if _, err := c.doQuery(stmt); err != nil {
			return err
		}
	}
	return nil
}

// Ping tests connectivity.
func (c *Client) Ping() error {
	_, err := c.doQuery("SELECT 1 FORMAT Null")
	return err
}
