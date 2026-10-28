package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"time"
)

// FactItem represents a semantic memory item received from brain
type FactItem struct {
	ID               int64     `json:"id"`
	FactText         string    `json:"fact_text"`
	ReinforceCount   int       `json:"reinforce_count"`
	LastReinforcedAt time.Time `json:"last_reinforced_at,omitzero"`
	CreatedAt        time.Time `json:"created_at"`
}

type FactsAPIResponse struct {
	Facts  []FactItem `json:"facts"`
	Total  int        `json:"total"`
	Limit  int        `json:"limit"`
	Offset int        `json:"offset"`
	Status string     `json:"status,omitempty"`
	Error  string     `json:"error,omitempty"`
}

// ScheduleSummaryMetrics represents aggregated schedule execution metrics
type ScheduleSummaryMetrics struct {
	TotalActive    int     `json:"total_active"`
	CronCount      int     `json:"cron_count"`
	OneShotCount   int     `json:"one_shot_count"`
	TotalRuns24h   int     `json:"total_runs_24h"`
	SuccessRate24h float64 `json:"success_rate_24h"`
}

// CronSchedule represents a recurring cron schedule
type CronSchedule struct {
	ID              string    `json:"id"`
	TargetID        string    `json:"target_id,omitempty"`
	ChannelID       string    `json:"channel_id"`
	TitlePrefix     string    `json:"title_prefix"`
	CronExpr        string    `json:"cron_expr"`
	CronDescription string    `json:"cron_description"`
	Prompt          string    `json:"prompt"`
	Timezone        string    `json:"timezone"`
	NextRunAt       time.Time `json:"next_run_at"`
	Enabled         bool      `json:"enabled"`
	CreatedAt       time.Time `json:"created_at"`
}

// OneShotSchedule represents a one-time scheduled reminder
type OneShotSchedule struct {
	ID        string    `json:"id"`
	ThreadID  string    `json:"thread_id"`
	Prompt    string    `json:"prompt"`
	RunAt     time.Time `json:"run_at"`
	CreatedAt time.Time `json:"created_at"`
}

// SchedulesAPIResponse represents the response payload for schedules and summary metrics
type SchedulesAPIResponse struct {
	Status     string                 `json:"status"`
	SystemTime time.Time              `json:"system_time,omitempty"`
	Summary    ScheduleSummaryMetrics `json:"summary"`
	Crons      []CronSchedule         `json:"crons"`
	OneShots   []OneShotSchedule      `json:"one_shots"`
	Error      string                 `json:"error,omitempty"`
}

// ScheduleRun represents a logged schedule execution run
type ScheduleRun struct {
	ID           string     `json:"id"`
	ScheduleID   string     `json:"schedule_id"`
	ScheduleType string     `json:"schedule_type"`
	MessageID    string     `json:"message_id,omitempty"`
	TargetID     string     `json:"target_id,omitempty"`
	ThreadID     string     `json:"thread_id,omitempty"`
	Title        string     `json:"title,omitempty"`
	Prompt       string     `json:"prompt"`
	Status       string     `json:"status"`
	StartedAt    time.Time  `json:"started_at"`
	CompletedAt  *time.Time `json:"completed_at,omitempty"`
	DurationMs   int64      `json:"duration_ms"`
	Error        string     `json:"error,omitempty"`
}

// ScheduleRunsAPIResponse represents paginated execution run logs
type ScheduleRunsAPIResponse struct {
	Status string        `json:"status"`
	Total  int           `json:"total"`
	Limit  int           `json:"limit"`
	Offset int           `json:"offset"`
	Runs   []ScheduleRun `json:"runs"`
	Error  string        `json:"error,omitempty"`
}

// VanadisChunkDTO represents an individual task chunk within a Vanadis build.
type VanadisChunkDTO struct {
	ID                 string    `json:"id"`
	BuildID            string    `json:"build_id"`
	ChunkIndex         int       `json:"chunk_index"`
	Repo               string    `json:"repo,omitempty"`
	Title              string    `json:"title"`
	Instructions       string    `json:"instructions,omitempty"`
	TargetFiles        []string  `json:"target_files,omitempty"`
	TargetSymbols      []string  `json:"target_symbols,omitempty"`
	Effort             string    `json:"effort,omitempty"`
	AcceptanceCriteria []string  `json:"acceptance_criteria,omitempty"`
	Status             string    `json:"status"`
	PRNumber           int       `json:"pr_number,omitempty"`
	StartedAt          time.Time `json:"started_at,omitempty"`
	CreatedAt          time.Time `json:"created_at"`
	UpdatedAt          time.Time `json:"updated_at"`
}

// VanadisBuildDTO represents a Vanadis build run or build entry.
type VanadisBuildDTO struct {
	ID              string            `json:"id"`
	ThreadID        string            `json:"thread_id"`
	Tier            int               `json:"tier"`
	Title           string            `json:"title"`
	Objective       string            `json:"objective"`
	SharedContext   string            `json:"shared_context,omitempty"`
	Status          string            `json:"status"`
	StatusMessageID string            `json:"status_message_id,omitempty"`
	CreatedAt       time.Time         `json:"created_at"`
	UpdatedAt       time.Time         `json:"updated_at"`
	QueueState      string            `json:"queue_state,omitempty"`
	TotalChunks     int               `json:"total_chunks,omitempty"`
	CompletedChunks int               `json:"completed_chunks,omitempty"`
	RunningChunks   int               `json:"running_chunks,omitempty"`
	PendingChunks   int               `json:"pending_chunks,omitempty"`
	FailedChunks    int               `json:"failed_chunks,omitempty"`
	Chunks          []VanadisChunkDTO `json:"chunks,omitempty"`
}

// BuildsAPIResponse represents the response payload for listing Vanadis builds.
type BuildsAPIResponse struct {
	Status         string            `json:"status"`
	Total          int               `json:"total"`
	RunningCount   int               `json:"running_count"`
	QueuedCount    int               `json:"queued_count"`
	CompletedCount int               `json:"completed_count"`
	Builds         []VanadisBuildDTO `json:"builds"`
	Error          string            `json:"error,omitempty"`
}

// BuildDetailAPIResponse represents the response payload for inspecting a single Vanadis build and its chunks.
type BuildDetailAPIResponse struct {
	Status string            `json:"status"`
	Build  *VanadisBuildDTO  `json:"build,omitempty"`
	Chunks []VanadisChunkDTO `json:"chunks"`
	Entry  *VanadisBuildDTO  `json:"entry,omitempty"`
	Error  string            `json:"error,omitempty"`
}

// Singleton tuned HTTP client for upstream vanadis communication
var vanadisHTTPClient = &http.Client{
	Timeout: 5 * time.Second,
	Transport: &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   2 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          50,
		MaxIdleConnsPerHost:   20,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: 4 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	},
}

// fetchUpstreamJSON executes an HTTP GET request with a 4-second timeout against targetURL,
// decodes any non-200 JSON error into upstreamErrMsg, or unmarshals the 200 OK JSON response into dest.
func fetchUpstreamJSON(ctx context.Context, client *http.Client, targetURL string, dest interface{}) (int, string, error) {
	reqCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, targetURL, nil)
	if err != nil {
		return 0, "", err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	defer drainAndClose(resp.Body, "upstream response body")

	if resp.StatusCode != http.StatusOK {
		var errPayload struct {
			Error string `json:"error"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&errPayload); err == nil && errPayload.Error != "" {
			return resp.StatusCode, strings.TrimSpace(errPayload.Error), fmt.Errorf("upstream returned HTTP %d", resp.StatusCode)
		}
		return resp.StatusCode, "", fmt.Errorf("upstream returned HTTP %d", resp.StatusCode)
	}

	if err := json.NewDecoder(resp.Body).Decode(dest); err != nil {
		return http.StatusOK, "", err
	}
	return http.StatusOK, "", nil
}

func factsHandler(brainBaseURL string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, `{"error":"Method Not Allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		targetURL, limit, offset, err := BuildFactsUpstreamURL(brainBaseURL, r.URL.Query())
		if err != nil {
			http.Error(w, `{"error":"Invalid upstream URL configuration"}`, http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")

		var data FactsAPIResponse
		statusCode, _, fetchErr := fetchUpstreamJSON(r.Context(), brainHTTPClient, targetURL, &data)
		if fetchErr != nil {
			if statusCode == 0 {
				log.Printf("[Dashboard] Upstream brain request failed (%s): %v", targetURL, fetchErr)
				writeJSON(w, r, http.StatusServiceUnavailable, FactsAPIResponse{
					Facts:  []FactItem{},
					Total:  0,
					Limit:  limit,
					Offset: offset,
					Status: "degraded",
					Error:  "Brain service unreachable. Retrying...",
				})
				return
			}
			if statusCode != http.StatusOK {
				log.Printf("[Dashboard] Upstream brain returned status %d", statusCode)
				writeJSON(w, r, statusCode, FactsAPIResponse{
					Facts:  []FactItem{},
					Total:  0,
					Limit:  limit,
					Offset: offset,
					Status: "error",
					Error:  "Upstream brain error occurred",
				})
				return
			}
			writeJSON(w, r, http.StatusBadGateway, FactsAPIResponse{
				Facts:  []FactItem{},
				Total:  0,
				Limit:  limit,
				Offset: offset,
				Status: "error",
				Error:  "Failed to decode upstream brain response",
			})
			return
		}

		if data.Facts == nil {
			data.Facts = []FactItem{}
		}

		writeJSON(w, r, http.StatusOK, data)
	}
}

func schedulesHandler(haroBaseURL string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			http.Error(w, `{"error":"Method Not Allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		targetURL, err := BuildSchedulesUpstreamURL(haroBaseURL)
		if err != nil {
			http.Error(w, `{"error":"Invalid upstream URL configuration"}`, http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")

		var data SchedulesAPIResponse
		statusCode, _, fetchErr := fetchUpstreamJSON(r.Context(), brainHTTPClient, targetURL, &data)
		if fetchErr != nil {
			if statusCode == 0 {
				log.Printf("[Dashboard] Upstream brain request failed (%s): %v", targetURL, fetchErr)
				writeJSON(w, r, http.StatusServiceUnavailable, SchedulesAPIResponse{
					Status: "degraded",
					Error:  "Brain service unreachable. Retrying...",
					Summary: ScheduleSummaryMetrics{
						TotalActive:    0,
						CronCount:      0,
						OneShotCount:   0,
						TotalRuns24h:   0,
						SuccessRate24h: 100.0,
					},
					Crons:    []CronSchedule{},
					OneShots: []OneShotSchedule{},
				})
				return
			}
			if statusCode != http.StatusOK {
				log.Printf("[Dashboard] Upstream brain returned status %d", statusCode)
				writeJSON(w, r, statusCode, SchedulesAPIResponse{
					Status: "error",
					Error:  "Upstream brain error occurred",
					Summary: ScheduleSummaryMetrics{
						TotalActive:    0,
						CronCount:      0,
						OneShotCount:   0,
						TotalRuns24h:   0,
						SuccessRate24h: 100.0,
					},
					Crons:    []CronSchedule{},
					OneShots: []OneShotSchedule{},
				})
				return
			}
			writeJSON(w, r, http.StatusBadGateway, SchedulesAPIResponse{
				Status: "error",
				Error:  "Failed to decode upstream brain response",
				Summary: ScheduleSummaryMetrics{
					TotalActive:    0,
					CronCount:      0,
					OneShotCount:   0,
					TotalRuns24h:   0,
					SuccessRate24h: 100.0,
				},
				Crons:    []CronSchedule{},
				OneShots: []OneShotSchedule{},
			})
			return
		}

		if data.Crons == nil {
			data.Crons = []CronSchedule{}
		}
		if data.OneShots == nil {
			data.OneShots = []OneShotSchedule{}
		}

		writeJSON(w, r, http.StatusOK, data)
	}
}

func scheduleRunsHandler(haroBaseURL string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			http.Error(w, `{"error":"Method Not Allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		targetURL, limit, offset, err := BuildScheduleRunsUpstreamURL(haroBaseURL, r.URL.Query())
		if err != nil {
			http.Error(w, `{"error":"Invalid upstream URL configuration"}`, http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")

		var data ScheduleRunsAPIResponse
		statusCode, _, fetchErr := fetchUpstreamJSON(r.Context(), brainHTTPClient, targetURL, &data)
		if fetchErr != nil {
			if statusCode == 0 {
				log.Printf("[Dashboard] Upstream brain request failed (%s): %v", targetURL, fetchErr)
				writeJSON(w, r, http.StatusServiceUnavailable, ScheduleRunsAPIResponse{
					Status: "degraded",
					Error:  "Brain service unreachable. Retrying...",
					Total:  0,
					Limit:  limit,
					Offset: offset,
					Runs:   []ScheduleRun{},
				})
				return
			}
			if statusCode != http.StatusOK {
				log.Printf("[Dashboard] Upstream brain returned status %d", statusCode)
				writeJSON(w, r, statusCode, ScheduleRunsAPIResponse{
					Status: "error",
					Error:  "Upstream brain error occurred",
					Total:  0,
					Limit:  limit,
					Offset: offset,
					Runs:   []ScheduleRun{},
				})
				return
			}
			writeJSON(w, r, http.StatusBadGateway, ScheduleRunsAPIResponse{
				Status: "error",
				Error:  "Failed to decode upstream brain response",
				Total:  0,
				Limit:  limit,
				Offset: offset,
				Runs:   []ScheduleRun{},
			})
			return
		}

		if data.Runs == nil {
			data.Runs = []ScheduleRun{}
		}

		writeJSON(w, r, http.StatusOK, data)
	}
}

func buildsHandler(vanadisBaseURL string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			http.Error(w, `{"error":"Method Not Allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		targetURL, _, err := BuildVanadisBuildsUpstreamURL(vanadisBaseURL, r.URL.Query())
		if err != nil {
			http.Error(w, `{"error":"Invalid upstream URL configuration"}`, http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")

		var data BuildsAPIResponse
		statusCode, _, fetchErr := fetchUpstreamJSON(r.Context(), vanadisHTTPClient, targetURL, &data)
		if fetchErr != nil {
			if statusCode == 0 {
				log.Printf("[Dashboard] Upstream vanadis request failed (%s): %v", targetURL, fetchErr)
				writeJSON(w, r, http.StatusServiceUnavailable, BuildsAPIResponse{
					Status: "degraded",
					Error:  "Vanadis service unreachable. Retrying...",
					Total:  0,
					Builds: []VanadisBuildDTO{},
				})
				return
			}
			if statusCode != http.StatusOK {
				log.Printf("[Dashboard] Upstream vanadis returned status %d", statusCode)
				writeJSON(w, r, statusCode, BuildsAPIResponse{
					Status: "error",
					Error:  "Upstream vanadis error occurred",
					Total:  0,
					Builds: []VanadisBuildDTO{},
				})
				return
			}
			writeJSON(w, r, http.StatusBadGateway, BuildsAPIResponse{
				Status: "error",
				Error:  "Failed to decode upstream vanadis response",
				Total:  0,
				Builds: []VanadisBuildDTO{},
			})
			return
		}

		if data.Builds == nil {
			data.Builds = []VanadisBuildDTO{}
		}
		for i := range data.Builds {
			data.Builds[i].SharedContext = ""
			for j := range data.Builds[i].Chunks {
				data.Builds[i].Chunks[j].Instructions = ""
				data.Builds[i].Chunks[j].TargetFiles = nil
				data.Builds[i].Chunks[j].TargetSymbols = nil
				data.Builds[i].Chunks[j].AcceptanceCriteria = nil
			}
		}

		writeJSON(w, r, http.StatusOK, data)
	}
}

func buildDetailHandler(vanadisBaseURL string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			http.Error(w, `{"error":"Method Not Allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		targetURL, _, err := BuildVanadisBuildDetailUpstreamURL(vanadisBaseURL, r.URL.Path)
		if err != nil {
			cleanBase := strings.TrimRight(strings.TrimSpace(vanadisBaseURL), "/")
			if cleanBase == "" || strings.ContainsAny(cleanBase, "\x7f\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\r\n") {
				http.Error(w, `{"error":"Invalid upstream URL configuration"}`, http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			writeJSON(w, r, http.StatusBadRequest, BuildDetailAPIResponse{
				Status: "error",
				Error:  err.Error(),
				Chunks: []VanadisChunkDTO{},
			})
			return
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")

		var data BuildDetailAPIResponse
		statusCode, upstreamErrMsg, fetchErr := fetchUpstreamJSON(r.Context(), vanadisHTTPClient, targetURL, &data)
		if fetchErr != nil {
			if statusCode == 0 {
				log.Printf("[Dashboard] Upstream vanadis detail request failed (%s): %v", targetURL, fetchErr)
				writeJSON(w, r, http.StatusServiceUnavailable, BuildDetailAPIResponse{
					Status: "degraded",
					Error:  "Vanadis service unreachable. Retrying...",
					Chunks: []VanadisChunkDTO{},
				})
				return
			}
			if statusCode != http.StatusOK {
				log.Printf("[Dashboard] Upstream vanadis detail returned status %d", statusCode)
				errMsg := "Upstream vanadis error occurred"
				if statusCode == http.StatusNotFound {
					errMsg = "Build not found"
				}
				if upstreamErrMsg != "" {
					errMsg = upstreamErrMsg
				}
				writeJSON(w, r, statusCode, BuildDetailAPIResponse{
					Status: "error",
					Error:  errMsg,
					Chunks: []VanadisChunkDTO{},
				})
				return
			}
			writeJSON(w, r, http.StatusBadGateway, BuildDetailAPIResponse{
				Status: "error",
				Error:  "Failed to decode upstream vanadis response",
				Chunks: []VanadisChunkDTO{},
			})
			return
		}

		if data.Status == "" {
			data.Status = "ok"
		}
		if data.Chunks == nil {
			data.Chunks = []VanadisChunkDTO{}
		}

		writeJSON(w, r, http.StatusOK, data)
	}
}
