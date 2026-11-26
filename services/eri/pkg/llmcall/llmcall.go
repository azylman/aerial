// Package llmcall defines eri's durable LLM call: the request contract shared by
// stateless eri and its callers, the Idempotency-Key derivation, and the Restate
// LLM service that eri-restate binds to run each call as one journaled step.
package llmcall

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"log"
	"os"
	"path/filepath"
	"strings"

	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/pkg/agent"
	"github.com/azylman/aerial/pkg/restateingress"
	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/delivery"
	"github.com/azylman/aerial/services/eri/pkg/runner"
)

const (
	// serviceBase is the Restate service name for the default agent.
	serviceBase = "LLM"
	// StepComplete names the single Run step of Service.Complete.
	StepComplete = "llm-complete"
	// MaxCallerKeyLen caps the caller-supplied operation identity.
	MaxCallerKeyLen = 200
	// idempotencyKeyPrefix starts every derived Idempotency-Key.
	idempotencyKeyPrefix = "llm:"
)

// Attachment is one internal file the model should inspect.
type Attachment struct {
	URL      string `json:"url"`
	Filename string `json:"filename"`
}

// Request is the input of one LLM call.
type Request struct {
	Prompt      string       `json:"prompt"`
	Model       string       `json:"model,omitempty"`
	Effort      string       `json:"effort,omitempty"`
	Attachments []Attachment `json:"attachments,omitempty"`
	// CallerKey names the caller's logical operation; blank means no dedupe.
	CallerKey string `json:"caller_key,omitempty"`
}

// Response is the output of one LLM call.
type Response struct {
	Response string `json:"response"`
}

// ServiceName returns the per-agent Restate service name ("LLM", "LLM-agent-alpha").
func ServiceName(agentName string) string {
	return agent.ScopedServiceName(serviceBase, agentName)
}

// Validate rejects requests no retry could ever complete.
func Validate(req Request) error {
	if strings.TrimSpace(req.Prompt) == "" {
		return errors.New("prompt cannot be empty")
	}
	if err := validateAttachments(req.Attachments); err != nil {
		return err
	}
	switch req.Effort {
	case "", config.EffortLow, config.EffortMedium, config.EffortHigh:
	default:
		return fmt.Errorf("effort %q must be empty, %q, %q or %q", req.Effort, config.EffortLow, config.EffortMedium, config.EffortHigh)
	}
	return validateCallerKey(req.CallerKey)
}

func validateAttachments(atts []Attachment) error {
	if len(atts) > delivery.MaxInternalLLMAttachments {
		return fmt.Errorf("attachments count %d exceeds maximum of %d", len(atts), delivery.MaxInternalLLMAttachments)
	}
	seen := make(map[string]bool, len(atts))
	for _, a := range atts {
		filename := strings.TrimSpace(a.Filename)
		if strings.TrimSpace(a.URL) == "" || filename == "" {
			return errors.New("attachment URL and filename cannot be empty")
		}
		lower := strings.ToLower(filename)
		if seen[lower] {
			return fmt.Errorf("duplicate attachment filename %q", filename)
		}
		seen[lower] = true
	}
	return nil
}

func validateCallerKey(key string) error {
	if len(key) > MaxCallerKeyLen {
		return fmt.Errorf("caller key length %d exceeds maximum of %d", len(key), MaxCallerKeyLen)
	}
	for i := range len(key) {
		// Printable ASCII without space: '!' (0x21) through '~' (0x7e).
		if key[i] < '!' || key[i] > '~' {
			return fmt.Errorf("caller key contains invalid byte 0x%02x at offset %d", key[i], i)
		}
	}
	return nil
}

// IdempotencyKey derives the Restate Idempotency-Key for req: blank without a
// CallerKey, otherwise "llm:<CallerKey>:<sha256 of the call content>". The
// content hash means a changed prompt, model, effort or attachment list never
// reuses a result recorded for different input under the same caller key.
func IdempotencyKey(req Request) string {
	if req.CallerKey == "" {
		return ""
	}
	h := sha256.New()
	writeField(h, req.Prompt)
	writeField(h, req.Model)
	writeField(h, req.Effort)
	writeLen(h, len(req.Attachments))
	for _, a := range req.Attachments {
		writeField(h, a.URL)
		writeField(h, a.Filename)
	}
	return idempotencyKeyPrefix + req.CallerKey + ":" + hex.EncodeToString(h.Sum(nil))
}

// writeField length-prefixes s so adjacent fields can never run together.
func writeField(h hash.Hash, s string) {
	writeLen(h, len(s))
	_, _ = h.Write([]byte(s))
}

func writeLen(h hash.Hash, n int) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(n))
	_, _ = h.Write(b[:])
}

// FetchFunc fetches one internal attachment.
type FetchFunc func(ctx context.Context, rawURL, filename string) (*delivery.Attachment, error)

// Service is the Restate LLM service. Each Complete call runs one journaled
// step, so a retried or redelivered invocation with the same Idempotency-Key
// attaches to the recorded result instead of paying for a new call.
type Service struct {
	llm       runner.LLMFunc
	agentName string
	fetch     FetchFunc
}

// NewService builds the LLM service for agentName on top of llm.
func NewService(llm runner.LLMFunc, agentName string) *Service {
	return &Service{llm: llm, agentName: agentName, fetch: delivery.ResolveAndFetchInternalAttachment}
}

// ServiceName is the Restate service name Reflect binds.
func (s *Service) ServiceName() string {
	return ServiceName(s.agentName)
}

// errCancelled is returned when the step's context was cancelled: a retryable
// error after cancellation makes sdk-go v1.2.0 run the step again.
var errCancelled = errors.New("llm call cancelled")

// Complete validates req and runs the LLM call as the single "llm-complete"
// step under Restate's default retry policy.
func (s *Service) Complete(ctx restate.Context, req Request) (Response, error) {
	if err := Validate(req); err != nil {
		return Response{}, restate.ToTerminalError(err, restate.WithErrorCode(400))
	}
	return restateingress.RunInSpan(ctx, "llm.complete", func(stepCtx context.Context, _ restate.RunContext) (Response, error) {
		return s.complete(stepCtx, req)
	}, restate.WithName(StepComplete))
}

func (s *Service) complete(ctx context.Context, req Request) (Response, error) {
	prompt := strings.TrimSpace(req.Prompt)
	dir, block, err := StageAttachments(ctx, s.fetch, req.Attachments)
	if err != nil {
		return Response{}, stepError(ctx, err)
	}
	if dir != "" {
		defer removeStagingDir(dir)
		prompt += block
	}
	out, err := s.llm(ctx, req.Model, prompt)
	if err != nil {
		return Response{}, stepError(ctx, err)
	}
	return Response{Response: out}, nil
}

// stepError classifies a step failure: invalid attachments are terminal 400, a
// failure after cancellation is terminal 412, anything else stays retryable.
func stepError(ctx context.Context, err error) error {
	if errors.Is(err, delivery.ErrInvalidInternalAttachment) {
		return restate.ToTerminalError(err, restate.WithErrorCode(400))
	}
	if ctx.Err() != nil {
		return restate.ToTerminalError(fmt.Errorf("%w: %w", errCancelled, err), restate.WithErrorCode(412))
	}
	return err
}

type stagedFile struct {
	path     string
	mimeType string
	size     int
}

// StageAttachments downloads atts into a fresh world-readable temp dir (agent
// turns run as uid 1000) and returns the dir plus the <ATTACHMENTS> prompt
// block. No attachments returns an empty dir and block. The caller removes dir.
func StageAttachments(ctx context.Context, fetch FetchFunc, atts []Attachment) (dir, block string, err error) {
	if len(atts) == 0 {
		return "", "", nil
	}
	dir, err = os.MkdirTemp("", "eri-llm-*")
	if err != nil {
		return "", "", fmt.Errorf("create attachment staging dir: %w", err)
	}
	staged, err := stageInto(ctx, fetch, dir, atts)
	if err != nil {
		removeStagingDir(dir)
		return "", "", err
	}
	var sb strings.Builder
	sb.WriteString("\n\n<ATTACHMENTS>\n")
	fmt.Fprintf(&sb, "Staged directory: %s\n", dir)
	sb.WriteString("Files are pre-downloaded and staged for direct inspection. Use read_file or view_file to examine them; do not execute curl, wget, or rm commands.\n")
	for _, f := range staged {
		fmt.Fprintf(&sb, "- %s (%s, %d bytes)\n", f.path, f.mimeType, f.size)
	}
	sb.WriteString("</ATTACHMENTS>")
	return dir, sb.String(), nil
}

func stageInto(ctx context.Context, fetch FetchFunc, dir string, atts []Attachment) ([]stagedFile, error) {
	if err := os.Chmod(dir, 0o755); err != nil {
		return nil, fmt.Errorf("set attachment staging dir permissions: %w", err)
	}
	staged := make([]stagedFile, 0, len(atts))
	for _, a := range atts {
		att, err := fetch(ctx, a.URL, a.Filename)
		if err != nil {
			return nil, err
		}
		path := filepath.Join(dir, att.Filename)
		if err := os.WriteFile(path, att.Data, 0o644); err != nil {
			return nil, fmt.Errorf("stage attachment %q: %w", att.Filename, err)
		}
		// WriteFile honours the umask; force the mode uid 1000 needs.
		if err := os.Chmod(path, 0o644); err != nil {
			return nil, fmt.Errorf("set permissions on staged attachment %q: %w", att.Filename, err)
		}
		staged = append(staged, stagedFile{path: path, mimeType: att.ContentType, size: len(att.Data)})
	}
	return staged, nil
}

func removeStagingDir(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		log.Printf("[LLM] Warning: failed to remove attachment staging dir %s: %v", dir, err)
	}
}
