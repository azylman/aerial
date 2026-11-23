package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/k8sauth"
	"github.com/azylman/aerial/pkg/tracing"
)

// Singleton tuned HTTP client for upstream vanadis communication. Every request carries the
// console ServiceAccount token (vanadis authorizes reads via TokenReview + SubjectAccessReview).
var vanadisHTTPClient = newVanadisHTTPClient(k8sauth.NewTokenSource("", time.Now))

// newVanadisHTTPClient builds the vanadis client around ts: the projected token is re-read
// through ts (60s cache), so kubelet rotations are picked up without a restart.
func newVanadisHTTPClient(ts *k8sauth.TokenSource) *http.Client {
	base := &http.Transport{
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
	}
	return tracing.Client(&http.Client{
		Timeout: 5 * time.Second,
		Transport: &optionalServiceAccountTransport{
			base:   base,
			tokens: ts,
			authed: k8sauth.NewServiceAccountTransportWithSource(base, ts),
		},
	})
}

// optionalServiceAccountTransport injects the ServiceAccount bearer token when the token file
// exists and sends the request unauthenticated when it does not (local dev outside a pod), so
// the upstream answers 401 visibly instead of the client failing before the request leaves.
// Any other token error (unreadable or empty file) fails the request.
type optionalServiceAccountTransport struct {
	base   http.RoundTripper
	tokens *k8sauth.TokenSource
	authed http.RoundTripper
}

func (t *optionalServiceAccountTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if _, err := t.tokens.Token(); errors.Is(err, fs.ErrNotExist) {
		return t.base.RoundTrip(req)
	}
	return t.authed.RoundTrip(req)
}

// CloseIdleConnections forwards to the shared base transport.
func (t *optionalServiceAccountTransport) CloseIdleConnections() {
	if c, ok := t.base.(interface{ CloseIdleConnections() }); ok {
		c.CloseIdleConnections()
	}
}

// fetchUpstreamJSON executes an HTTP GET request with a 4-second timeout against targetURL,
// decodes any non-200 JSON error into upstreamErrMsg, or unmarshals the 200 OK JSON response into dest.
func fetchUpstreamJSON(ctx context.Context, client *http.Client, targetURL string, dest any) (int, string, error) {
	reqCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, targetURL, http.NoBody)
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

func serveUpstreamGET[T any](
	w http.ResponseWriter,
	r *http.Request,
	client *http.Client,
	upstreamLabel, unreachableMsg, targetURL string,
	urlErr error,
	makeErrPayload func(status, errMsg string) T,
	normalize func(*T),
) {
	if r.Method != http.MethodGet {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		http.Error(w, `{"error":"Method Not Allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	if urlErr != nil {
		http.Error(w, `{"error":"Invalid upstream URL configuration"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")

	var data T
	statusCode, _, fetchErr := fetchUpstreamJSON(r.Context(), client, targetURL, &data)
	if fetchErr != nil {
		if statusCode == 0 {
			log.Printf("[Console] Upstream %s request failed (%s): %v", upstreamLabel, targetURL, fetchErr)
			writeJSON(w, r, http.StatusServiceUnavailable, makeErrPayload("degraded", unreachableMsg))
			return
		}
		if statusCode != http.StatusOK {
			log.Printf("[Console] Upstream %s returned status %d", upstreamLabel, statusCode)
			writeJSON(w, r, statusCode, makeErrPayload("error", "Upstream "+upstreamLabel+" error occurred"))
			return
		}
		writeJSON(w, r, http.StatusBadGateway, makeErrPayload("error", "Failed to decode upstream "+upstreamLabel+" response"))
		return
	}

	if normalize != nil {
		normalize(&data)
	}

	writeJSON(w, r, http.StatusOK, data)
}

func factsHandler(brainBaseURL string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		targetURL, limit, offset, err := BuildFactsUpstreamURL(brainBaseURL, r.URL.Query())
		serveUpstreamGET(
			w, r, brainHTTPClient, "brain", "Brain service unreachable. Retrying...", targetURL, err,
			func(status, errMsg string) FactsAPIResponse {
				return FactsAPIResponse{
					Facts:  []FactItem{},
					Total:  0,
					Limit:  limit,
					Offset: offset,
					Status: status,
					Error:  errMsg,
				}
			},
			func(d *FactsAPIResponse) {
				if d.Facts == nil {
					d.Facts = []FactItem{}
				}
				if agent := r.URL.Query().Get("agent"); agent != "" && !strings.EqualFold(agent, "all") {
					d.Facts = FilterFactsByAgent(d.Facts, agent)
				}
			},
		)
	}
}

func schedulesHandler(haroBaseURL string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		targetURL, err := BuildSchedulesUpstreamURL(haroBaseURL, r.URL.Query())
		serveUpstreamGET(
			w, r, brainHTTPClient, "brain", "Brain service unreachable. Retrying...", targetURL, err,
			func(status, errMsg string) SchedulesAPIResponse {
				return SchedulesAPIResponse{
					Status: status,
					Error:  errMsg,
					Summary: ScheduleSummaryMetrics{
						TotalActive:    0,
						CronCount:      0,
						OneShotCount:   0,
						TotalRuns24h:   0,
						SuccessRate24h: 100.0,
					},
					Crons:    []CronSchedule{},
					OneShots: []OneShotSchedule{},
				}
			},
			func(d *SchedulesAPIResponse) {
				if d.Crons == nil {
					d.Crons = []CronSchedule{}
				}
				if d.OneShots == nil {
					d.OneShots = []OneShotSchedule{}
				}
				if agent := r.URL.Query().Get("agent"); agent != "" && !strings.EqualFold(agent, "all") {
					d.Crons = FilterCronsByAgent(d.Crons, agent)
					d.OneShots = FilterOneShotsByAgent(d.OneShots, agent)
					d.Summary.CronCount = len(d.Crons)
					d.Summary.OneShotCount = len(d.OneShots)
					d.Summary.TotalActive = d.Summary.CronCount + d.Summary.OneShotCount
				}
			},
		)
	}
}

func scheduleRunsHandler(haroBaseURL string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		targetURL, limit, offset, err := BuildScheduleRunsUpstreamURL(haroBaseURL, r.URL.Query())
		serveUpstreamGET(
			w, r, brainHTTPClient, "brain", "Brain service unreachable. Retrying...", targetURL, err,
			func(status, errMsg string) ScheduleRunsAPIResponse {
				return ScheduleRunsAPIResponse{
					Status: status,
					Error:  errMsg,
					Total:  0,
					Limit:  limit,
					Offset: offset,
					Runs:   []ScheduleRun{},
				}
			},
			func(d *ScheduleRunsAPIResponse) {
				if d.Runs == nil {
					d.Runs = []ScheduleRun{}
				}
				if agent := r.URL.Query().Get("agent"); agent != "" && !strings.EqualFold(agent, "all") {
					d.Runs = FilterScheduleRunsByAgent(d.Runs, agent)
				}
			},
		)
	}
}

func ensureChunksDependsOn(chunks []VanadisChunkDTO) {
	for i := range chunks {
		if chunks[i].DependsOn == nil {
			chunks[i].DependsOn = []string{}
		}
	}
}

func normalizeBuildDetailChunks(data *BuildDetailAPIResponse) {
	if data.Chunks == nil {
		data.Chunks = []VanadisChunkDTO{}
	}
	ensureChunksDependsOn(data.Chunks)
	if data.Build != nil {
		ensureChunksDependsOn(data.Build.Chunks)
	}
	if data.Entry != nil {
		ensureChunksDependsOn(data.Entry.Chunks)
	}
}

func buildsHandler(vanadisBaseURL string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		targetURL, _, err := BuildVanadisBuildsUpstreamURL(vanadisBaseURL, r.URL.Query())
		serveUpstreamGET(
			w, r, vanadisHTTPClient, "vanadis", "Vanadis service unreachable. Retrying...", targetURL, err,
			func(status, errMsg string) BuildsAPIResponse {
				return BuildsAPIResponse{
					Status: status,
					Error:  errMsg,
					Total:  0,
					Builds: []VanadisBuildDTO{},
				}
			},
			func(d *BuildsAPIResponse) {
				if d.Builds == nil {
					d.Builds = []VanadisBuildDTO{}
				}
				for i := range d.Builds {
					d.Builds[i].SharedContext = ""
					for j := range d.Builds[i].Chunks {
						d.Builds[i].Chunks[j].Instructions = ""
						d.Builds[i].Chunks[j].TargetFiles = nil
						d.Builds[i].Chunks[j].TargetSymbols = nil
						d.Builds[i].Chunks[j].AcceptanceCriteria = nil
					}
					ensureChunksDependsOn(d.Builds[i].Chunks)
				}
				if agent := r.URL.Query().Get("agent"); agent != "" && !strings.EqualFold(agent, "all") {
					d.Builds = FilterBuildsByAgent(d.Builds, agent)
				}
			},
		)
	}
}

func handleBuildDetailFetchError(w http.ResponseWriter, r *http.Request, targetURL string, statusCode int, upstreamErrMsg string, fetchErr error) {
	if statusCode == 0 {
		log.Printf("[Console] Upstream vanadis detail request failed (%s): %v", targetURL, fetchErr)
		writeJSON(w, r, http.StatusServiceUnavailable, BuildDetailAPIResponse{
			Status: statusDegraded,
			Error:  "Vanadis service unreachable. Retrying...",
			Chunks: []VanadisChunkDTO{},
		})
		return
	}
	if statusCode != http.StatusOK {
		log.Printf("[Console] Upstream vanadis detail returned status %d", statusCode)
		errMsg := "Upstream vanadis error occurred"
		if statusCode == http.StatusNotFound {
			errMsg = "Build not found"
		}
		if upstreamErrMsg != "" {
			errMsg = upstreamErrMsg
		}
		writeJSON(w, r, statusCode, BuildDetailAPIResponse{
			Status: statusError,
			Error:  errMsg,
			Chunks: []VanadisChunkDTO{},
		})
		return
	}
	writeJSON(w, r, http.StatusBadGateway, BuildDetailAPIResponse{
		Status: statusError,
		Error:  "Failed to decode upstream vanadis response",
		Chunks: []VanadisChunkDTO{},
	})
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
				Status: statusError,
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
			handleBuildDetailFetchError(w, r, targetURL, statusCode, upstreamErrMsg, fetchErr)
			return
		}

		if data.Status == "" {
			data.Status = "ok"
		}
		normalizeBuildDetailChunks(&data)

		writeJSON(w, r, http.StatusOK, data)
	}
}

const (
	docketUnreachableMsg   = "Docket service unreachable. Retrying..."
	docketNotConfiguredMsg = "docket not configured"
)

// Singleton tuned HTTP client for upstream docket-mcp communication.
var docketHTTPClient = tracing.Client(&http.Client{
	Timeout: 5 * time.Second,
	Transport: &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   2 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          20,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: 4 * time.Second,
	},
})

// rejectDocketRequest answers 405 for non-GET methods and 503 degraded when docket_url is
// unset, reporting whether the request was already answered.
func rejectDocketRequest[T any](w http.ResponseWriter, r *http.Request, docketBaseURL string, makeErrPayload func(status, errMsg string) T) bool {
	if r.Method != http.MethodGet {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		http.Error(w, `{"error":"Method Not Allowed"}`, http.StatusMethodNotAllowed)
		return true
	}
	if strings.TrimSpace(docketBaseURL) == "" {
		writeJSON(w, r, http.StatusServiceUnavailable, makeErrPayload("degraded", docketNotConfiguredMsg))
		return true
	}
	return false
}

func docketTasksErrPayload(status, errMsg string) DocketTasksAPIResponse {
	return DocketTasksAPIResponse{Status: status, Tasks: []DocketTaskSummaryDTO{}, Error: errMsg}
}

func docketSearchErrPayload(status, errMsg string) DocketSearchAPIResponse {
	return DocketSearchAPIResponse{Status: status, Results: []DocketScoredTaskDTO{}, Error: errMsg}
}

func docketTasksHandler(docketBaseURL string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if rejectDocketRequest(w, r, docketBaseURL, docketTasksErrPayload) {
			return
		}
		targetURL, err := BuildDocketTasksUpstreamURL(docketBaseURL, r.URL.Query())
		serveUpstreamGET(
			w, r, docketHTTPClient, "docket", docketUnreachableMsg, targetURL, err,
			docketTasksErrPayload,
			func(d *DocketTasksAPIResponse) {
				d.Status = "ok"
				if d.Tasks == nil {
					d.Tasks = []DocketTaskSummaryDTO{}
				}
			},
		)
	}
}

func docketSearchHandler(docketBaseURL string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if rejectDocketRequest(w, r, docketBaseURL, docketSearchErrPayload) {
			return
		}
		targetURL, err := BuildDocketSearchUpstreamURL(docketBaseURL, r.URL.Query())
		if errors.Is(err, errInvalidDocketRequest) {
			writeJSON(w, r, http.StatusBadRequest, docketSearchErrPayload("error", err.Error()))
			return
		}
		serveUpstreamGET(
			w, r, docketHTTPClient, "docket", docketUnreachableMsg, targetURL, err,
			docketSearchErrPayload,
			func(d *DocketSearchAPIResponse) {
				d.Status = "ok"
				if d.Results == nil {
					d.Results = []DocketScoredTaskDTO{}
				}
			},
		)
	}
}

func docketTaskDetailErrPayload(status, errMsg string) DocketTaskDetailAPIResponse {
	return DocketTaskDetailAPIResponse{Status: status, Error: errMsg}
}

func handleDocketTaskDetailFetchError(w http.ResponseWriter, r *http.Request, targetURL string, statusCode int, upstreamErrMsg string, fetchErr error) {
	if statusCode == 0 {
		log.Printf("[Console] Upstream docket detail request failed (%s): %v", targetURL, fetchErr)
		writeJSON(w, r, http.StatusServiceUnavailable, docketTaskDetailErrPayload("degraded", docketUnreachableMsg))
		return
	}
	if statusCode != http.StatusOK {
		log.Printf("[Console] Upstream docket detail returned status %d", statusCode)
		errMsg := "Upstream docket error occurred"
		if statusCode == http.StatusNotFound {
			errMsg = "Task not found"
		}
		if upstreamErrMsg != "" {
			errMsg = upstreamErrMsg
		}
		writeJSON(w, r, statusCode, docketTaskDetailErrPayload("error", errMsg))
		return
	}
	writeJSON(w, r, http.StatusBadGateway, docketTaskDetailErrPayload("error", "Failed to decode upstream docket response"))
}

func docketTaskDetailHandler(docketBaseURL string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if rejectDocketRequest(w, r, docketBaseURL, docketTaskDetailErrPayload) {
			return
		}

		targetURL, _, err := BuildDocketTaskDetailUpstreamURL(docketBaseURL, r.URL.Path)
		if errors.Is(err, errInvalidDocketRequest) {
			writeJSON(w, r, http.StatusBadRequest, docketTaskDetailErrPayload("error", err.Error()))
			return
		}
		if err != nil {
			http.Error(w, `{"error":"Invalid upstream URL configuration"}`, http.StatusInternalServerError)
			return
		}

		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")

		var task DocketTaskDTO
		statusCode, upstreamErrMsg, fetchErr := fetchUpstreamJSON(r.Context(), docketHTTPClient, targetURL, &task)
		if fetchErr != nil {
			handleDocketTaskDetailFetchError(w, r, targetURL, statusCode, upstreamErrMsg, fetchErr)
			return
		}

		writeJSON(w, r, http.StatusOK, DocketTaskDetailAPIResponse{Status: "ok", Task: &task})
	}
}
