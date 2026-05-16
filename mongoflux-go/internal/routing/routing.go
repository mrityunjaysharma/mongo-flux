package routing

import (
	"net/url"
	"strings"
)

// ParsedURI holds the parsed components of a MongoDB connection URI.
type ParsedURI struct {
	Scheme   string
	Host     string
	Port     string
	Database string
	Params   map[string]string
}

// ParseMongoURI parses a MongoDB URI string into components.
func ParseMongoURI(uri string) ParsedURI {
	result := ParsedURI{
		Scheme: "mongodb",
		Port:   "27017",
		Params: make(map[string]string),
	}

	remaining := uri

	// Extract scheme
	if idx := strings.Index(remaining, "://"); idx >= 0 {
		result.Scheme = remaining[:idx]
		remaining = remaining[idx+3:]
	}

	// Strip credentials
	if idx := strings.Index(remaining, "@"); idx >= 0 {
		remaining = remaining[idx+1:]
	}

	// Extract query parameters
	if idx := strings.Index(remaining, "?"); idx >= 0 {
		queryString := remaining[idx+1:]
		remaining = remaining[:idx]

		params := strings.Split(queryString, "&")
		for _, p := range params {
			if eqIdx := strings.Index(p, "="); eqIdx >= 0 {
				key, _ := url.QueryUnescape(p[:eqIdx])
				val, _ := url.QueryUnescape(p[eqIdx+1:])
				result.Params[key] = val
			} else {
				result.Params[p] = "true"
			}
		}
	}

	// Extract database
	if idx := strings.Index(remaining, "/"); idx >= 0 {
		result.Database = remaining[idx+1:]
		remaining = remaining[:idx]
	}

	// Extract host and port (take first host from comma-separated list)
	hosts := strings.Split(remaining, ",")
	if len(hosts) > 0 {
		hostPort := hosts[0]
		if idx := strings.Index(hostPort, ":"); idx >= 0 {
			result.Host = hostPort[:idx]
			result.Port = hostPort[idx+1:]
		} else {
			result.Host = hostPort
		}
	}

	return result
}

// HasClickHouseRouting checks if a URI has the ClickHouse routing parameter set.
func HasClickHouseRouting(uri ParsedURI, paramName string) bool {
	val, ok := uri.Params[paramName]
	if !ok {
		return false
	}
	val = strings.ToLower(val)
	return val == "true" || val == "1" || val == "yes"
}
