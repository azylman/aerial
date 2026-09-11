package main

import (
	"fmt"
	"os"
	"strings"
)

const (
	// DefaultTimezone is the fallback timezone when unspecified.
	DefaultTimezone = "America/Los_Angeles"
	// DefaultPort is the fallback HTTP server port when unspecified.
	DefaultPort = "8080"
)

// Config represents the runtime configuration for the scheduler MCP server.
type Config struct {
	DatabaseURL string
	Timezone    string
	Port        string
}

// NewConfig creates a Config instance with explicit values.
func NewConfig(databaseURL, timezone, port string) *Config {
	if timezone == "" {
		timezone = DefaultTimezone
	}
	if port == "" {
		port = DefaultPort
	}
	return &Config{
		DatabaseURL: databaseURL,
		Timezone:    timezone,
		Port:        port,
	}
}

// LoadConfigFromLookup reads configuration using the provided environment lookup function.
// Returns an error if lookup is nil or if no valid database connection string can be constructed.
func LoadConfigFromLookup(lookup func(string) string) (*Config, error) {
	if lookup == nil {
		return nil, fmt.Errorf("config: lookup function cannot be nil")
	}

	port := strings.TrimSpace(lookup("PORT"))
	if port == "" {
		port = DefaultPort
	}

	tz := strings.TrimSpace(lookup("DEFAULT_TIMEZONE"))
	if tz == "" {
		tz = strings.TrimSpace(lookup("TZ"))
	}
	if tz == "" {
		tz = DefaultTimezone
	}

	dbURL := strings.TrimSpace(lookup("DATABASE_URL"))
	if dbURL == "" {
		dbURL = strings.TrimSpace(lookup("DB_PATH"))
	}
	if dbURL == "" {
		dbHost := strings.TrimSpace(lookup("POSTGRES_HOST"))
		if dbHost != "" {
			dbUser := strings.TrimSpace(lookup("POSTGRES_USER"))
			if dbUser == "" {
				dbUser = "aerial"
			}
			dbPass := strings.TrimSpace(lookup("POSTGRES_PASSWORD"))
			if dbPass == "" {
				dbPass = "aerial_secure_pass"
			}
			dbPort := strings.TrimSpace(lookup("POSTGRES_PORT"))
			if dbPort == "" {
				dbPort = "5432"
			}
			dbName := strings.TrimSpace(lookup("POSTGRES_DB"))
			if dbName == "" {
				dbName = "aerial"
			}
			dbURL = fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", dbUser, dbPass, dbHost, dbPort, dbName)
		}
	}

	if dbURL == "" {
		return nil, fmt.Errorf("database connection string is required (set DATABASE_URL, DB_PATH, or POSTGRES_HOST)")
	}

	return &Config{
		DatabaseURL: dbURL,
		Timezone:    tz,
		Port:        port,
	}, nil
}

// LoadConfig reads configuration from ambient environment variables.
func LoadConfig() (*Config, error) {
	return LoadConfigFromLookup(os.Getenv)
}
