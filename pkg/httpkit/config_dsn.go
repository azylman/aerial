package httpkit

import (
	"net"
	"net/url"
	"os"
	"strings"
)

// ResolveConfigPathWithLocal resolves the configuration file path.
// 1. Checks strings.TrimSpace(os.Getenv(envKey)) (defaults envKey to "CONFIG_PATH" if empty).
// 2. If localPath != "", checks os.Stat(localPath) and returns localPath if err == nil && fi.Size() > 0.
// 3. Iterates fallbackPaths and returns the first path where os.Stat(p) succeeds.
// 4. If none exist on disk, returns fallbackPaths[0] (or "" if fallbackPaths is empty).
func ResolveConfigPathWithLocal(envKey, localPath string, fallbackPaths ...string) string {
	key := strings.TrimSpace(envKey)
	if key == "" {
		key = "CONFIG_PATH"
	}
	if env := strings.TrimSpace(os.Getenv(key)); env != "" {
		return env
	}
	if localPath != "" {
		if fi, err := os.Stat(localPath); err == nil && fi.Size() > 0 {
			return localPath
		}
	}
	for _, p := range fallbackPaths {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	if len(fallbackPaths) > 0 {
		return fallbackPaths[0]
	}
	return ""
}

// BuildPostgresDSN constructs a PostgreSQL connection DSN from environment lookup and optional defaultDB.
// If lookup is nil or both POSTGRES_HOST and POSTGRES_PASSWORD (and URL overrides) are empty, it returns "".
func BuildPostgresDSN(lookup func(string) string, defaultDB string) string {
	if lookup == nil {
		return ""
	}
	rawURL := strings.TrimSpace(lookup("POSTGRES_URL"))
	if rawURL == "" {
		rawURL = strings.TrimSpace(lookup("DATABASE_URL"))
	}
	if rawURL != "" {
		if explicitDB := strings.TrimSpace(lookup("POSTGRES_DB")); explicitDB != "" {
			if u, err := url.Parse(rawURL); err == nil && u.Scheme != "" {
				u.Path = "/" + strings.TrimPrefix(explicitDB, "/")
				return u.String()
			}
		}
		return rawURL
	}

	dbHost := strings.TrimSpace(lookup("POSTGRES_HOST"))
	passVal := strings.TrimSpace(lookup("POSTGRES_PASSWORD"))
	if dbHost == "" && passVal != "" {
		dbHost = "postgres"
	}
	if dbHost == "" && passVal == "" {
		return ""
	}

	dbPort := strings.TrimSpace(lookup("POSTGRES_PORT"))
	if idx := strings.LastIndex(dbPort, ":"); idx >= 0 {
		dbPort = dbPort[idx+1:]
	}
	if dbPort == "" {
		dbPort = "5432"
	}
	dbUser := strings.TrimSpace(lookup("POSTGRES_USER"))
	if dbUser == "" {
		dbUser = "aerial"
	}
	dbPass := passVal
	if dbPass == "" {
		dbPass = "aerial_secure_pass"
	}
	dbName := strings.TrimSpace(lookup("POSTGRES_DB"))
	if dbName == "" {
		dbName = strings.TrimSpace(defaultDB)
	}
	if dbName == "" {
		dbName = "aerial"
	}
	dbName = strings.TrimPrefix(dbName, "/")

	u := &url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(dbUser, dbPass),
		Host:     net.JoinHostPort(dbHost, dbPort),
		Path:     "/" + dbName,
		RawQuery: "sslmode=disable",
	}
	return u.String()
}
