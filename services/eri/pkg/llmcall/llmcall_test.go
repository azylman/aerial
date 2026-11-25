package llmcall

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"

	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/services/eri/pkg/delivery"
)

// pngBytes is a PNG signature plus padding, enough for MIME sniffing.
var pngBytes = append([]byte("\x89PNG\r\n\x1a\n"), make([]byte, 24)...)

// attachmentBase is the httptest attachment server, registered as an allowed
// internal host before any test runs so parallel tests only read the map.
var attachmentBase string

func TestMain(m *testing.M) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/media/pic.png", "/media/other.png":
			w.Header().Set("Content-Type", "image/png")
			_, _ = w.Write(pngBytes)
		case "/media/flaky.png":
			w.WriteHeader(http.StatusServiceUnavailable)
		default:
			http.NotFound(w, r)
		}
	}))
	u, err := url.Parse(srv.URL)
	if err != nil {
		panic(err)
	}
	delivery.AllowedInternalHosts[strings.ToLower(u.Hostname())] = true
	attachmentBase = srv.URL
	code := m.Run()
	srv.Close()
	os.Exit(code)
}

func TestValidate(t *testing.T) {
	t.Parallel()

	tooMany := make([]Attachment, 33)
	for i := range tooMany {
		tooMany[i] = Attachment{URL: "http://vision.aerial/a", Filename: fmt.Sprintf("f%d.png", i)}
	}
	tests := []struct {
		name    string
		req     Request
		wantErr string
	}{
		{name: "minimal", req: Request{Prompt: "hi"}},
		{name: "full", req: Request{Prompt: "hi", Model: "m", Effort: "high", CallerKey: "vision:caption:event:42:init", Attachments: []Attachment{{URL: "http://vision.aerial/a", Filename: "a.png"}}}},
		{name: "caller key at 200 chars", req: Request{Prompt: "hi", CallerKey: strings.Repeat("k", 200)}},
		{name: "blank prompt", req: Request{Prompt: " \n\t"}, wantErr: "prompt cannot be empty"},
		{name: "unknown effort", req: Request{Prompt: "hi", Effort: "max"}, wantErr: `effort "max"`},
		{name: "too many attachments", req: Request{Prompt: "hi", Attachments: tooMany}, wantErr: "attachments count 33 exceeds maximum of 32"},
		{name: "blank attachment url", req: Request{Prompt: "hi", Attachments: []Attachment{{URL: " ", Filename: "a.png"}}}, wantErr: "cannot be empty"},
		{name: "blank attachment filename", req: Request{Prompt: "hi", Attachments: []Attachment{{URL: "http://vision.aerial/a", Filename: ""}}}, wantErr: "cannot be empty"},
		{name: "case-insensitive duplicate filename", req: Request{Prompt: "hi", Attachments: []Attachment{{URL: "http://vision.aerial/a", Filename: "A.png"}, {URL: "http://vision.aerial/b", Filename: "a.PNG"}}}, wantErr: `duplicate attachment filename "a.PNG"`},
		{name: "caller key too long", req: Request{Prompt: "hi", CallerKey: strings.Repeat("k", 201)}, wantErr: "caller key length 201 exceeds maximum of 200"},
		{name: "caller key with space", req: Request{Prompt: "hi", CallerKey: "a b"}, wantErr: "invalid byte 0x20 at offset 1"},
		{name: "caller key with newline", req: Request{Prompt: "hi", CallerKey: "ab\n"}, wantErr: "invalid byte 0x0a at offset 2"},
		{name: "caller key non-ascii", req: Request{Prompt: "hi", CallerKey: "é"}, wantErr: "invalid byte 0xc3 at offset 0"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := Validate(tc.req)
			if tc.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

var keyPattern = regexp.MustCompile(`^llm:coven:extract:aerial:t1:9:[0-9a-f]{64}$`)

func baseKeyed() Request {
	return Request{
		Prompt: "summarize", Model: "flash", Effort: "low", CallerKey: "coven:extract:aerial:t1:9",
		Attachments: []Attachment{{URL: "http://vision.aerial/a.png", Filename: "a.png"}, {URL: "http://vision.aerial/b.png", Filename: "b.png"}},
	}
}

func TestIdempotencyKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		other     Request
		wantEqual bool
	}{
		{name: "identical input is stable", other: baseKeyed(), wantEqual: true},
		{name: "prompt changes key", other: Request{Prompt: "summarize!", Model: "flash", Effort: "low", CallerKey: "coven:extract:aerial:t1:9", Attachments: baseKeyed().Attachments}},
		{name: "model changes key", other: Request{Prompt: "summarize", Model: "pro", Effort: "low", CallerKey: "coven:extract:aerial:t1:9", Attachments: baseKeyed().Attachments}},
		{name: "effort changes key", other: Request{Prompt: "summarize", Model: "flash", Effort: "high", CallerKey: "coven:extract:aerial:t1:9", Attachments: baseKeyed().Attachments}},
		{name: "attachment url changes key", other: Request{Prompt: "summarize", Model: "flash", Effort: "low", CallerKey: "coven:extract:aerial:t1:9", Attachments: []Attachment{{URL: "http://vision.aerial/c.png", Filename: "a.png"}, {URL: "http://vision.aerial/b.png", Filename: "b.png"}}}},
		{name: "attachment filename changes key", other: Request{Prompt: "summarize", Model: "flash", Effort: "low", CallerKey: "coven:extract:aerial:t1:9", Attachments: []Attachment{{URL: "http://vision.aerial/a.png", Filename: "z.png"}, {URL: "http://vision.aerial/b.png", Filename: "b.png"}}}},
		{name: "attachment order changes key", other: Request{Prompt: "summarize", Model: "flash", Effort: "low", CallerKey: "coven:extract:aerial:t1:9", Attachments: []Attachment{{URL: "http://vision.aerial/b.png", Filename: "b.png"}, {URL: "http://vision.aerial/a.png", Filename: "a.png"}}}},
		{name: "dropped attachment changes key", other: Request{Prompt: "summarize", Model: "flash", Effort: "low", CallerKey: "coven:extract:aerial:t1:9", Attachments: []Attachment{{URL: "http://vision.aerial/a.png", Filename: "a.png"}}}},
		{name: "field boundary shift changes key", other: Request{Prompt: "summarizeflash", Model: "", Effort: "low", CallerKey: "coven:extract:aerial:t1:9", Attachments: baseKeyed().Attachments}},
	}
	base := IdempotencyKey(baseKeyed())
	assert.Regexp(t, keyPattern, base)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := IdempotencyKey(tc.other)
			assert.Regexp(t, keyPattern, got)
			assert.Equal(t, tc.wantEqual, got == base, "base %s other %s", base, got)
		})
	}
}

func TestIdempotencyKeyBlankWithoutCallerKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		req  Request
	}{
		{name: "empty request", req: Request{}},
		{name: "full request without caller key", req: Request{Prompt: "p", Model: "m", Effort: "low", Attachments: []Attachment{{URL: "http://vision.aerial/a", Filename: "a.png"}}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Empty(t, IdempotencyKey(tc.req))
		})
	}
}

func TestServiceName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		agent string
		want  string
	}{
		{agent: "", want: "LLM"},
		{agent: "aerial", want: "LLM"},
		{agent: "agent-alpha", want: "LLM-agent-alpha"},
		{agent: " Agent-Alpha ", want: "LLM-agent-alpha"},
	}
	for _, tc := range tests {
		t.Run(tc.agent, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, ServiceName(tc.agent))
			assert.Equal(t, tc.want, NewService(nil, tc.agent).ServiceName())
		})
	}
}

type mockRunContext struct {
	context.Context
}

func (mockRunContext) Log() *slog.Logger         { return slog.Default() }
func (mockRunContext) Request() *restate.Request { return &restate.Request{ID: "inv-llm-1"} }

// fakeLLM records each prompt and the staged dir mode seen while the call ran.
type fakeLLM struct {
	err string

	mu       sync.Mutex
	prompts  []string
	dirModes []fs.FileMode
}

var stagedDirPattern = regexp.MustCompile(`Staged directory: (\S+)`)

func (f *fakeLLM) call(ctx context.Context, model, prompt string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.prompts = append(f.prompts, prompt)
	if m := stagedDirPattern.FindStringSubmatch(prompt); m != nil {
		if info, err := os.Stat(m[1]); err == nil {
			f.dirModes = append(f.dirModes, info.Mode().Perm())
		}
	}
	if f.err != "" {
		return "", errors.New(f.err)
	}
	return "answer from " + model, nil
}

// stepRecord captures what Complete's Run step did.
type stepRecord struct {
	ran       bool
	name      string
	retryOpts []string
	err       error
}

// runOptionFields lists the RunOptions fields an option set; only Name is allowed.
func runOptionFields(opts []restate.RunOption) (name string, set []string) {
	for _, opt := range opts {
		m := reflect.ValueOf(opt).MethodByName("BeforeRun")
		arg := reflect.New(m.Type().In(0).Elem())
		m.Call([]reflect.Value{arg})
		v := arg.Elem()
		for i := range v.NumField() {
			field := v.Type().Field(i).Name
			if field == "Name" {
				if s := v.Field(i).String(); s != "" {
					name = s
				}
				continue
			}
			if !v.Field(i).IsZero() {
				set = append(set, field)
			}
		}
	}
	return name, set
}

func completeContext(rcCtx context.Context, t *testing.T, rec *stepRecord) restate.Context {
	t.Helper()
	mc := mocks.NewMockContext(t)
	mc.EXPECT().Value(mock.Anything).Return(nil).Maybe()
	mc.EXPECT().Run(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(fn func(restate.RunContext) (any, error), output any, opts ...restate.RunOption) restate.TerminalError {
		rec.ran = true
		rec.name, rec.retryOpts = runOptionFields(opts)
		res, err := fn(mockRunContext{Context: rcCtx})
		rec.err = err
		if err != nil {
			if restate.IsTerminalError(err) {
				return restate.AsTerminalError(err)
			}
			// A real Run would abort the attempt for a retry; the test inspects rec.err.
			return nil
		}
		reflect.ValueOf(output).Elem().Set(reflect.ValueOf(res))
		return nil
	}).Maybe()
	return restate.WithMockContext(mc)
}

func TestServiceComplete(t *testing.T) {
	t.Parallel()

	pic := attachmentBase + "/media/pic.png"
	tests := []struct {
		name      string
		req       Request
		llmErr    string
		cancelled bool
		// wantCode is the terminal error code Complete returns (0: none).
		wantCode uint16
		// wantRetryable is the step error message that must stay retryable.
		wantRetryable    string
		wantRan          bool
		wantResponse     string
		wantPromptSubstr []string
		wantStagedDir    bool
	}{
		{name: "unkeyed success", req: Request{Prompt: "  hello  ", Model: "flash"}, wantRan: true, wantResponse: "answer from flash", wantPromptSubstr: []string{"hello"}},
		{name: "blank prompt is terminal 400 without a step", req: Request{Prompt: " "}, wantCode: 400},
		{name: "bad caller key is terminal 400 without a step", req: Request{Prompt: "hi", CallerKey: "has space"}, wantCode: 400},
		{name: "duplicate filenames are terminal 400 without a step", req: Request{Prompt: "hi", Attachments: []Attachment{{URL: pic, Filename: "a.png"}, {URL: pic, Filename: "A.png"}}}, wantCode: 400},
		{name: "unkeyed llm error stays retryable", req: Request{Prompt: "hi"}, llmErr: "engine exploded", wantRan: true, wantRetryable: "engine exploded"},
		{name: "keyed llm error stays retryable", req: Request{Prompt: "hi", CallerKey: "vision:caption:event:1:init"}, llmErr: "engine exploded", wantRan: true, wantRetryable: "engine exploded"},
		{name: "cancelled step is terminal 412", req: Request{Prompt: "hi", CallerKey: "k"}, llmErr: "context canceled", cancelled: true, wantRan: true, wantCode: 412},
		{name: "keyed attachment 404 is terminal 400", req: Request{Prompt: "hi", CallerKey: "k", Attachments: []Attachment{{URL: attachmentBase + "/media/missing.png", Filename: "m.png"}}}, wantRan: true, wantCode: 400},
		{name: "non-internal attachment url is terminal 400", req: Request{Prompt: "hi", Attachments: []Attachment{{URL: "https://example.com/a.png", Filename: "a.png"}}}, wantRan: true, wantCode: 400},
		{name: "attachment 503 stays retryable", req: Request{Prompt: "hi", CallerKey: "k", Attachments: []Attachment{{URL: attachmentBase + "/media/flaky.png", Filename: "f.png"}}}, wantRan: true, wantRetryable: "unexpected http status 503"},
		{
			name:             "attachments are staged into the prompt",
			req:              Request{Prompt: "look", Model: "flash", CallerKey: "k", Attachments: []Attachment{{URL: pic, Filename: "first.png"}, {URL: attachmentBase + "/media/other.png", Filename: "second.png"}}},
			wantRan:          true,
			wantResponse:     "answer from flash",
			wantPromptSubstr: []string{"look\n\n<ATTACHMENTS>\nStaged directory: ", "/first.png (image/png, 32 bytes)", "/second.png (image/png, 32 bytes)", "do not execute curl, wget, or rm commands.", "</ATTACHMENTS>"},
			wantStagedDir:    true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rcCtx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tc.cancelled {
				cancel()
			}
			llm := &fakeLLM{err: tc.llmErr}
			svc := NewService(llm.call, "aerial")
			rec := &stepRecord{}

			resp, err := svc.Complete(completeContext(rcCtx, t, rec), tc.req)

			assert.Equal(t, tc.wantRan, rec.ran)
			if tc.wantRan {
				assert.Equal(t, "llm-complete", rec.name)
				assert.Empty(t, rec.retryOpts, "step must use the server default retry policy")
			} else {
				assert.Empty(t, llm.prompts)
			}
			switch {
			case tc.wantCode != 0:
				require.Error(t, err)
				require.True(t, restate.IsTerminalError(err), "want terminal, got %v", err)
				assert.Equal(t, restate.Code(tc.wantCode), restate.AsTerminalError(err).Code())
			case tc.wantRetryable != "":
				require.Error(t, rec.err)
				assert.False(t, restate.IsTerminalError(rec.err), "want retryable, got %v", rec.err)
				assert.Contains(t, rec.err.Error(), tc.wantRetryable)
			default:
				require.NoError(t, err)
				assert.Equal(t, tc.wantResponse, resp.Response)
			}
			for _, s := range tc.wantPromptSubstr {
				require.Len(t, llm.prompts, 1)
				assert.Contains(t, llm.prompts[0], s)
			}
			if tc.wantStagedDir {
				assert.Equal(t, []fs.FileMode{0o755}, llm.dirModes)
				m := stagedDirPattern.FindStringSubmatch(llm.prompts[0])
				require.NotNil(t, m)
				_, statErr := os.Stat(m[1])
				assert.ErrorIs(t, statErr, fs.ErrNotExist, "staging dir must be removed")
			}
		})
	}
}

// fetchAs returns pngBytes under the requested filename without any network.
func fetchAs(_ context.Context, _, filename string) (*delivery.Attachment, error) {
	return &delivery.Attachment{Filename: filename, ContentType: "image/png", Data: pngBytes}, nil
}

func TestStageIntoFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		subdir   string
		filename string
		wantErr  string
	}{
		{name: "missing staging dir", subdir: "gone", filename: "a.png", wantErr: "set attachment staging dir permissions"},
		{name: "unwritable target path", filename: "nested/a.png", wantErr: `stage attachment "nested/a.png"`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			if tc.subdir != "" {
				dir += "/" + tc.subdir
			}
			_, err := stageInto(t.Context(), fetchAs, dir, []Attachment{{URL: "http://vision.aerial/a", Filename: tc.filename}})
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}
