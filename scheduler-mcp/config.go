package main

import (
	"fmt"
	"os"
	"strings"
)

// Config represents the runtime configuration for the scheduler MCP server.
type Config struct {
	DatabaseURL string
	Timezone    string
	Port        string
}

// GetDefaultTimezone returns the configured default timezone for the server.
// Reads DEFAULT_TIMEZONE -> TZ -> fallback "America/Los_Angeles".
func GetDefaultTimezone() string {
	if tz := strings.TrimSpace(os.Getenv("DEFAULT_TIMEZONE")); tz != "" {
		return tz
	}
	if tz := strings.TrimSpace(os.Getenv("TZ")); tz != "" {
		return tz
	}
	return "America/Los_Angeles"
}

// NewConfig creates a Config instance with explicit values.
func NewConfig(databaseURL, timezone, port string) *Config {
	if timezone == "" {
		timezone = "America/Los_Angeles"
	}
	if port == "" {
		port = "8080"
	}
	return &Config{
		DatabaseURL: databaseURL,
		Timezone:    timezone,
		Port:        port,
	}
}

// LoadConfig reads configuration from environment variables.
// Returns an error if no valid database URL or host is specified.
func LoadConfig() (*Config, error) {
	port := strings.TrimSpace(os.Getenv("PORT"))
	if port == "" {
		port = "8080"
	}

	tz := GetDefaultTimezone()

	dbURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if dbURL == "" {
		dbURL = strings.TrimSpace(os.Getenv("DB_PATH"))
	}
	if dbURL == "" {
		dbHost := strings.TrimSpace(os.Getenv("POSTGRES_HOST"))
		if dbHost != "" {
			dbUser := strings.TrimSpace(os.Getenv("POSTGRES_USER"))
			if dbUser == "" {
				dbUser = "aerial"
			}
			dbPass := strings.TrimSpace(os.Getenv("POSTGRES_PASSWORD"))
			if dbPass == "" {
				dbPass = "aerial_secure_pass"
			}
			dbPort := strings.TrimSpace(os.Getenv("POSTGRES_PORT"))
			if dbPort == "" {
				dbPort = "5432"
			}
			dbName := strings.TrimSpace(os.Getenv("POSTGRES_DB"))
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
