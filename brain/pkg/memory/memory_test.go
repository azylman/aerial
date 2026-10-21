package memory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
)

type roundTripperFunc func(req *http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func mockJSONResponse(statusCode int, body string) *http.Response {
	return &http.Response{
		StatusCode: statusCode,
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
	}
}

func mockEmbeddingResponse(vec []float32) *http.Response {
	b, _ := json.Marshal(EmbeddingResponse{Embedding: vec})
	return mockJSONResponse(http.StatusOK, string(b))
}

type mockCloseErrReader struct {
	io.Reader
}

func (m *mockCloseErrReader) Close() error {
	return errors.New("simulated close error")
}

func newMockClient(rt roundTripperFunc, roots ...string) *Client {
	c := NewClient("http://mock-ollama:11434", roots...)
	c.httpClient = &http.Client{Transport: rt}
	c.retryDelay = -1
	return c
}

func makeDimVector(x, y float32) []float32 {
	v := make([]float32, db.ExpectedEmbeddingDim)
	v[0] = x
	v[1] = y
	return v
}

func setupTestStore(t *testing.T) *db.FakeStore {
	t.Helper()
	return db.NewFakeStore()
}

type errFactStore struct {
	db.FactStore
}

func (e *errFactStore) SearchSimilarFacts(ctx context.Context, embedding []float32, queryText string, limit int, minScore float64) ([]db.Fact, error) {
	return nil, fmt.Errorf("search failed")
}

type errRowIDStore struct {
	*db.FakeStore
}

func (e *errRowIDStore) GetMaxMessageRowID(ctx context.Context, threadID string) (int64, error) {
	return 0, fmt.Errorf("simulated max rowid error")
}

type errInsertFactStore struct {
	*db.FakeStore
}

func (e *errInsertFactStore) GetMaxMessageRowID(ctx context.Context, threadID string) (int64, error) {
	return 1, nil
}

func (e *errInsertFactStore) GetUnextractedAdminTurns(ctx context.Context, threadID string) ([]db.Message, error) {
	return []db.Message{{ID: "m1", RowID: 5, Content: "user msg", ResponseText: "assistant reply", Status: db.StatusCompleted, Metadata: db.MessageMetadata{IsAdmin: true}}}, nil
}

func (e *errInsertFactStore) UpdateConversationFactWatermark(ctx context.Context, threadID string, maxRowID int64) error {
	return nil
}

func (e *errInsertFactStore) InsertFact(ctx context.Context, category, factText string, importance float64, embedding []float32) (int64, error) {
	return 0, fmt.Errorf("simulated insert error")
}

type mockErrWatermarkFactStore struct {
	*db.FakeStore
	watermarkErr   error
	extractedAtErr error
}

func (m *mockErrWatermarkFactStore) UpdateConversationFactWatermark(ctx context.Context, threadID string, maxRowID int64) error {
	if m.watermarkErr != nil {
		return m.watermarkErr
	}
	return m.FakeStore.UpdateConversationFactWatermark(ctx, threadID, maxRowID)
}

func (m *mockErrWatermarkFactStore) UpdateConversationFactExtractedAt(ctx context.Context, threadID string) error {
	if m.extractedAtErr != nil {
		return m.extractedAtErr
	}
	return m.FakeStore.UpdateConversationFactExtractedAt(ctx, threadID)
}

func TestClient_NewAndConfig(t *testing.T) {
	cfgCustom := config.NewFromData(&config.ConfigData{
		Ollama: config.OllamaConfig{
			BaseURL: "http://custom:11434/",
			Model:   "nomic-embed-text",
			NumCtx:  1024,
		},
	})
	cfgEmpty := config.NewFromData(nil)

	tests := []struct {
		name        string
		client      *Client
		wantBaseURL string
		wantRoots   []string
		wantNumCtx  int
	}{
		{"NewNilConfig", New(nil), DefaultOllamaURL, nil, 512},
		{"NewCustomConfig", New(cfgCustom, " /r1 ", "", " /r2 "), "http://custom:11434", []string{"/r1", "/r2"}, 1024},
		{"NewClientString", NewClient("http://host:11434/", "/path"), "http://host:11434", []string{"/path"}, 512},
		{"NewClientConfig", NewClient(cfgCustom), "http://custom:11434", nil, 1024},
		{"NewClientUnknownType", NewClient(12345), DefaultOllamaURL, nil, 512},
		{"NilClient", nil, DefaultOllamaURL, nil, 512},
		{"EmptyClient", &Client{}, DefaultOllamaURL, nil, 512},
		{"EmptyDataConfig", New(cfgEmpty), DefaultOllamaURL, nil, 512},
		{"UninitializedConfig", New(&config.Config{}), DefaultOllamaURL, nil, 512},
		{"ClientWithNilConfig", &Client{cfg: nil}, DefaultOllamaURL, nil, 512},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.client.BaseURL(); got != tc.wantBaseURL {
				t.Errorf("BaseURL() = %q, want %q", got, tc.wantBaseURL)
			}
			roots := tc.client.Roots()
			if len(roots) != len(tc.wantRoots) {
				t.Fatalf("Roots() len = %d, want %d", len(roots), len(tc.wantRoots))
			}
			for i, r := range roots {
				if r != tc.wantRoots[i] {
					t.Errorf("Roots()[%d] = %q, want %q", i, r, tc.wantRoots[i])
				}
			}
			if len(roots) > 0 {
				roots[0] = "mutated"
				if tc.client.Roots()[0] == "mutated" {
					t.Errorf("Roots() is not a defensive copy")
				}
			}
			cfg := tc.client.getOllamaConfig()
			if cfg.NumCtx != tc.wantNumCtx {
				t.Errorf("getOllamaConfig().NumCtx = %d, want %d", cfg.NumCtx, tc.wantNumCtx)
			}
		})
	}
}

func TestDotProduct(t *testing.T) {
	tests := []struct {
		name string
		a    []float32
		b    []float32
		want float64
	}{
		{"IdenticalUnit", []float32{1.0, 0.0, 0.0}, []float32{1.0, 0.0, 0.0}, 1.0},
		{"Orthogonal", []float32{1.0, 0.0, 0.0}, []float32{0.0, 1.0, 0.0}, 0.0},
		{"Scaled", []float32{0.5, 0.5}, []float32{0.5, 0.5}, 0.5},
		{"NilFirst", nil, []float32{1.0}, 0.0},
		{"NilSecond", []float32{1.0}, nil, 0.0},
		{"MismatchedLen", []float32{1.0}, []float32{1.0, 2.0}, 0.0},
		{"BothEmpty", []float32{}, []float32{}, 0.0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := DotProduct(tc.a, tc.b); got != tc.want {
				t.Errorf("DotProduct() = %f, want %f", got, tc.want)
			}
		})
	}
}

func TestGenerateEmbedding(t *testing.T) {
	tests := []struct {
		name         string
		text         string
		isQuery      bool
		maxRetries   int
		queryPrefix  string
		model        string
		numCtx       int
		httpStatus   int
		httpBody     string
		netErr       bool
		closeErr     bool
		cancelCtx    bool
		nilHTTP      bool
		badURL       bool
		wantLen      int
		wantErr      bool
		wantSubstr   string
		expectPrompt string
	}{
		{
			name:       "EmptyText",
			text:       "",
			wantErr:    true,
			wantSubstr: "text cannot be empty",
		},
		{
			name:         "SuccessDocument",
			text:         "hello world",
			isQuery:      false,
			maxRetries:   1,
			httpStatus:   http.StatusOK,
			httpBody:     `{"embedding":[0.1, 0.2, 0.3]}`,
			wantLen:      3,
			expectPrompt: "hello world",
		},
		{
			name:         "SuccessQueryWithPrefix",
			text:         "user search",
			isQuery:      true,
			queryPrefix:  "query: ",
			httpStatus:   http.StatusOK,
			httpBody:     `{"embedding":[0.5, 0.6]}`,
			wantLen:      2,
			expectPrompt: "query: user search",
		},
		{
			name:         "SuccessCustomModelAndCtx",
			text:         "custom test",
			model:        "custom-model",
			numCtx:       1024,
			httpStatus:   http.StatusOK,
			httpBody:     `{"embedding":[1.0]}`,
			wantLen:      1,
			expectPrompt: "custom test",
		},
		{
			name:       "HTTP500Error",
			text:       "fail 500",
			httpStatus: http.StatusInternalServerError,
			httpBody:   "internal error",
			wantErr:    true,
			wantSubstr: "ollama HTTP 500",
		},
		{
			name:       "APIErrorResponse",
			text:       "fail api error",
			httpStatus: http.StatusOK,
			httpBody:   `{"error":"model not loaded"}`,
			wantErr:    true,
			wantSubstr: "model not loaded",
		},
		{
			name:       "EmptyEmbeddingResponse",
			text:       "fail empty emb",
			httpStatus: http.StatusOK,
			httpBody:   `{"embedding":[]}`,
			wantErr:    true,
			wantSubstr: "empty embedding returned",
		},
		{
			name:       "InvalidJSON",
			text:       "fail json",
			httpStatus: http.StatusOK,
			httpBody:   "not-valid-json",
			wantErr:    true,
		},
		{
			name:       "NetworkErrorNegativeRetries",
			text:       "fail net",
			maxRetries: -1,
			netErr:     true,
			wantErr:    true,
			wantSubstr: "connection refused",
		},
		{
			name:      "ContextCancelled",
			text:      "cancelled",
			cancelCtx: true,
			wantErr:   true,
		},
		{
			name:       "BodyCloseErrorIgnored",
			text:       "close err",
			httpStatus: http.StatusOK,
			httpBody:   `{"embedding":[0.7, 0.8]}`,
			closeErr:   true,
			wantLen:    2,
		},
		{
			name:      "NilHTTPClientFallbackCancelled",
			text:      "nil http",
			nilHTTP:   true,
			cancelCtx: true,
			wantErr:   true,
		},
		{
			name:    "BadURLRequestError",
			text:    "bad url",
			badURL:  true,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var receivedPrompt string
			rt := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				if tc.netErr {
					return nil, errors.New("connection refused")
				}
				var reqBody EmbeddingRequest
				_ = json.NewDecoder(req.Body).Decode(&reqBody)
				receivedPrompt = reqBody.Prompt

				resp := mockJSONResponse(tc.httpStatus, tc.httpBody)
				if tc.closeErr {
					resp.Body = &mockCloseErrReader{Reader: strings.NewReader(tc.httpBody)}
				}
				return resp, nil
			})

			cfg := config.NewFromData(&config.ConfigData{
				Ollama: config.OllamaConfig{
					BaseURL:     "http://mock-ollama:11434",
					Model:       tc.model,
					QueryPrefix: tc.queryPrefix,
					NumCtx:      tc.numCtx,
				},
			})
			client := New(cfg)
			client.httpClient = &http.Client{Transport: rt}
			client.retryDelay = -1

			if tc.nilHTTP {
				client.httpClient = nil
			}
			if tc.badURL {
				client = NewClient("http://\x7f")
				client.retryDelay = -1
			}

			ctx := context.Background()
			if tc.cancelCtx {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}

			emb, err := client.GenerateEmbedding(ctx, tc.text, tc.isQuery, tc.maxRetries)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if tc.wantSubstr != "" && !strings.Contains(err.Error(), tc.wantSubstr) {
					t.Errorf("error %q does not contain %q", err.Error(), tc.wantSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(emb) != tc.wantLen {
				t.Errorf("len(emb) = %d, want %d", len(emb), tc.wantLen)
			}
			if tc.expectPrompt != "" && receivedPrompt != tc.expectPrompt {
				t.Errorf("prompt = %q, want %q", receivedPrompt, tc.expectPrompt)
			}
		})
	}
}

func TestExtractQueryText(t *testing.T) {
	longText := strings.Repeat("word ", 300)
	tests := []struct {
		name    string
		content string
		want    string
		maxLen  int
	}{
		{
			name: "DiscordPromptWithMention",
			content: `<USER_REQUEST>
Here's a message someone sent you from Discord:
- id: 1543706746294509568
- content: <@1542035925603713086> What office do I work from on Thursdays?
- mentions: [Aerial]
</USER_REQUEST>`,
			want: "What office do I work from on Thursdays?",
		},
		{
			name:    "XMLPrompt",
			content: "<USER_REQUEST>\n  Show me system status\n</USER_REQUEST>",
			want:    "Show me system status",
		},
		{
			name:    "PlainText",
			content: "What is the weather today?",
			want:    "What is the weather today?",
		},
		{
			name:    "Empty",
			content: "",
			want:    "",
		},
		{
			name:    "MentionsStripped",
			content: "Hey <@&987654> check <#123456> please",
			want:    "Hey check please",
		},
		{
			name:    "PreviousSessionStripped",
			content: "<PREVIOUS_SESSION>\nPrior context\n</PREVIOUS_SESSION>\nWhat is my schedule?",
			want:    "What is my schedule?",
		},
		{
			name:    "LongTextTruncated",
			content: longText,
			maxLen:  1000,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ExtractQueryText(tc.content)
			if tc.maxLen > 0 {
				if len(got) > tc.maxLen {
					t.Errorf("len(got) = %d, want <= %d", len(got), tc.maxLen)
				}
				return
			}
			if got != tc.want {
				t.Errorf("ExtractQueryText() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRankFacts(t *testing.T) {
	factsStandard := []db.FactWithEmbedding{
		{Fact: db.Fact{ID: 1, Category: "c1", FactText: "Fact 1", Importance: 1.0}, Embedding: []float32{0.9, 0.1, 0.0}},
		{Fact: db.Fact{ID: 2, Category: "c2", FactText: "Fact 2", Importance: 1.0}, Embedding: []float32{0.3, 0.7, 0.0}},
		{Fact: db.Fact{ID: 3, Category: "c3", FactText: "Fact 3", Importance: 1.0}, Embedding: []float32{0.95, 0.05, 0.0}},
	}
	tests := []struct {
		name     string
		qVec     []float32
		facts    []db.FactWithEmbedding
		minScore float64
		topN     int
		wantIDs  []int64
	}{
		{"StandardThresholdAndRanking", []float32{1.0, 0.0, 0.0}, factsStandard, 0.45, 10, []int64{3, 1}},
		{"TopNTruncation", []float32{1.0, 0.0, 0.0}, factsStandard, 0.1, 1, []int64{3}},
		{"DefaultTopN", []float32{1.0, 0.0, 0.0}, factsStandard, 0.45, 0, []int64{3, 1}},
		{"DimensionMismatch", []float32{1.0, 0.0}, []db.FactWithEmbedding{{Fact: db.Fact{ID: 1}, Embedding: []float32{1.0}}}, 0.0, 10, nil},
		{"EmptyFacts", []float32{1.0, 0.0}, nil, 0.0, 10, nil},
		{"EmptyQueryVector", nil, factsStandard, 0.0, 10, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := RankFacts(tc.qVec, tc.facts, tc.minScore, tc.topN)
			if len(got) != len(tc.wantIDs) {
				t.Fatalf("RankFacts() len = %d, want %d", len(got), len(tc.wantIDs))
			}
			for i, id := range tc.wantIDs {
				if got[i].ID != id {
					t.Errorf("got[%d].ID = %d, want %d", i, got[i].ID, id)
				}
			}
		})
	}
}

func TestFormatMemoryContext(t *testing.T) {
	tests := []struct {
		name         string
		facts        []db.Fact
		wantContains []string
		wantEmpty    bool
	}{
		{
			name: "StandardFacts",
			facts: []db.Fact{
				{Category: "user_preference", FactText: "Prefers Pacific Time"},
				{Category: "system_config", FactText: "Docker Daemon at unix:///var/run/docker.sock"},
			},
			wantContains: []string{"<retrieved_memory>", "- [user_preference] Prefers Pacific Time", "- [system_config] Docker Daemon at unix:///var/run/docker.sock", "</retrieved_memory>"},
		},
		{
			name:         "DefaultGeneralCategory",
			facts:        []db.Fact{{Category: "", FactText: "No category fact"}},
			wantContains: []string{"- [general] No category fact"},
		},
		{"NilFacts", nil, nil, true},
		{"EmptyFacts", []db.Fact{}, nil, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := FormatMemoryContext(tc.facts)
			if tc.wantEmpty {
				if got != "" {
					t.Errorf("FormatMemoryContext() = %q, want empty", got)
				}
				return
			}
			for _, sub := range tc.wantContains {
				if !strings.Contains(got, sub) {
					t.Errorf("FormatMemoryContext() missing %q, got: %s", sub, got)
				}
			}
		})
	}
}

func TestRetrieveRelevantFacts(t *testing.T) {
	tests := []struct {
		name       string
		nilStore   bool
		nilClient  bool
		storeErr   bool
		clientErr  bool
		query      string
		maxFacts   int
		wantLen    int
		wantErr    bool
		wantSubstr string
	}{
		{"NilStore", true, false, false, false, "query", 5, 0, false, ""},
		{"NilClient", false, true, false, false, "query", 5, 0, false, ""},
		{"LongQuerySucceeds", false, false, false, false, strings.Repeat("hello world ", 100), 5, 1, false, "Server port is 8080"},
		{"StoreSearchError", false, false, true, false, "query", 5, 0, true, "failed to search similar facts"},
		{"ClientEmbeddingErrorFallback", false, false, false, true, "What port?", 5, 0, false, ""},
		{"SuccessVectorRetrieval", false, false, false, false, "What port is server?", 5, 1, false, "Server port is 8080"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			var store db.FactStore
			if !tc.nilStore {
				if tc.storeErr {
					store = &errFactStore{}
				} else {
					fs := setupTestStore(t)
					defer fs.Close()
					_, _ = fs.InsertFact(ctx, "system_config", "Server port is 8080", 1.0, makeDimVector(0.9, 0.1))
					_, _ = fs.InsertFact(ctx, "routine", "Low scoring fact", 1.0, makeDimVector(0.1, 0.9))
					store = fs
				}
			}

			var client *Client
			if !tc.nilClient {
				client = newMockClient(func(req *http.Request) (*http.Response, error) {
					if tc.clientErr {
						return nil, errors.New("connection refused")
					}
					return mockEmbeddingResponse(makeDimVector(1.0, 0.0)), nil
				})
			}

			facts, err := RetrieveRelevantFacts(ctx, store, client, tc.query, tc.maxFacts)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if tc.wantSubstr != "" && !strings.Contains(err.Error(), tc.wantSubstr) {
					t.Errorf("error %q does not contain %q", err.Error(), tc.wantSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(facts) != tc.wantLen {
				t.Fatalf("len(facts) = %d, want %d", len(facts), tc.wantLen)
			}
			if tc.wantSubstr != "" && facts[0].FactText != tc.wantSubstr {
				t.Errorf("fact text = %q, want %q", facts[0].FactText, tc.wantSubstr)
			}
		})
	}
}

func TestRetrieveFormattedContext(t *testing.T) {
	tests := []struct {
		name         string
		nilStore     bool
		nilClient    bool
		storeErr     bool
		emptyQuery   bool
		zeroFacts    bool
		wantContains []string
		wantEmpty    bool
	}{
		{"NilStore", true, false, false, false, false, nil, true},
		{"NilClient", false, true, false, false, false, nil, true},
		{"EmptyQuery", false, false, false, true, false, nil, true},
		{"StoreSearchErrorFallback", false, false, true, false, false, nil, true},
		{"ZeroFacts", false, false, false, false, true, nil, true},
		{
			name:         "SuccessContextFormatting",
			wantContains: []string{"<retrieved_memory>", "- [user_preference] User prefers dark mode", "</retrieved_memory>"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			var store db.FactStore
			if !tc.nilStore {
				if tc.storeErr {
					store = &errFactStore{}
				} else {
					fs := setupTestStore(t)
					defer fs.Close()
					if !tc.zeroFacts {
						_, _ = fs.InsertFact(ctx, "user_preference", "User prefers dark mode", 1.0, makeDimVector(0.9, 0.1))
					}
					store = fs
				}
			}

			var client *Client
			if !tc.nilClient {
				client = newMockClient(func(req *http.Request) (*http.Response, error) {
					return mockEmbeddingResponse(makeDimVector(1.0, 0.0)), nil
				})
			}

			query := "dark mode"
			if tc.emptyQuery {
				query = "   \t\n"
			}

			res, err := RetrieveFormattedContext(ctx, store, client, query, 5)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantEmpty {
				if res != "" {
					t.Errorf("RetrieveFormattedContext() = %q, want empty", res)
				}
				return
			}
			for _, sub := range tc.wantContains {
				if !strings.Contains(res, sub) {
					t.Errorf("RetrieveFormattedContext() missing %q, got: %s", sub, res)
				}
			}
		})
	}
}

func TestBackfillMissingEmbeddings(t *testing.T) {
	tests := []struct {
		name          string
		nilStore      bool
		nilClient     bool
		closedDB      bool
		zeroMissing   bool
		emptyEmbResp  bool
		embGenErr     bool
		cancelContext bool
		updateErr     bool
		wantCount     int
		wantErr       bool
	}{
		{"NilStore", true, false, false, false, false, false, false, false, 0, true},
		{"NilClient", false, true, false, false, false, false, false, false, 0, true},
		{"ClosedDB", false, false, true, false, false, false, false, false, 0, true},
		{"ZeroMissing", false, false, false, true, false, false, false, false, 0, false},
		{"EmptyEmbeddingReturned", false, false, false, false, true, false, false, false, 0, false},
		{"EmbeddingGenError", false, false, false, false, false, true, false, false, 0, false},
		{"CancelledContext", false, false, false, false, false, false, true, false, 0, true},
		{"UpdateEmbeddingError", false, false, false, false, false, false, false, true, 0, false},
		{"SuccessBackfill", false, false, false, false, false, false, false, false, 2, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var factStore db.FactStore
			if !tc.nilStore {
				store := setupTestStore(t)
				defer store.Close()
				if !tc.zeroMissing {
					_, _ = store.InsertFact(ctx, "c1", "Fact 1 has emb", 1.0, makeDimVector(0.1, 0.2))
					_, _ = store.InsertFact(ctx, "c2", "Fact 2 needs emb", 1.0, nil)
					if tc.emptyEmbResp || tc.updateErr {
						// single fact
					} else {
						_, _ = store.InsertFact(ctx, "c3", "Fact 3 needs emb", 0.9, nil)
						_, _ = store.InsertFact(ctx, "c4", "   ", 0.5, nil) // whitespace skipped
					}
				}
				if tc.closedDB {
					_ = store.Close()
				}
				factStore = store
			}

			var client *Client
			if !tc.nilClient {
				client = newMockClient(func(req *http.Request) (*http.Response, error) {
					if tc.cancelContext {
						cancel()
						return nil, context.Canceled
					}
					if tc.embGenErr {
						return nil, errors.New("embedding generation error")
					}
					if tc.emptyEmbResp {
						return mockEmbeddingResponse([]float32{}), nil
					}
					if tc.updateErr && factStore != nil {
						if fs, ok := factStore.(*db.FakeStore); ok {
							_ = fs.Close()
						}
					}
					return mockEmbeddingResponse(makeDimVector(0.5, 0.5)), nil
				})
			}

			count, err := BackfillMissingEmbeddings(ctx, factStore, client)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if count != tc.wantCount {
				t.Errorf("BackfillMissingEmbeddings() count = %d, want %d", count, tc.wantCount)
			}
		})
	}
}

func TestParseFactsJSON(t *testing.T) {
	tests := []struct {
		name       string
		raw        string
		wantFacts  int
		wantText   string
		wantErr    bool
		wantSubstr string
	}{
		{
			name:      "ValidMarkdownCodeBlock",
			raw:       "```json\n{\"facts\":[{\"category\":\"user_preference\",\"fact_text\":\"User likes Go\",\"importance_score\":1.0}]}\n```",
			wantFacts: 1,
			wantText:  "User likes Go",
		},
		{
			name:      "ValidRawJSON",
			raw:       `{"facts":[{"category":"routine","fact_text":"Morning coffee at 8am","importance_score":0.8}]}`,
			wantFacts: 1,
			wantText:  "Morning coffee at 8am",
		},
		{
			name:       "EmptyPayload",
			raw:        "   ",
			wantErr:    true,
			wantSubstr: "empty raw facts payload",
		},
		{
			name:       "NoJSONObject",
			raw:        "Here are no facts found",
			wantErr:    true,
			wantSubstr: "no json object found",
		},
		{
			name:    "InvalidJSONSyntax",
			raw:     "{not a valid json}",
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			payload, err := parseFactsJSON(tc.raw)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if tc.wantSubstr != "" && !strings.Contains(err.Error(), tc.wantSubstr) {
					t.Errorf("error %q does not contain %q", err.Error(), tc.wantSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(payload.Facts) != tc.wantFacts {
				t.Fatalf("len(payload.Facts) = %d, want %d", len(payload.Facts), tc.wantFacts)
			}
			if tc.wantText != "" && payload.Facts[0].FactText != tc.wantText {
				t.Errorf("fact text = %q, want %q", payload.Facts[0].FactText, tc.wantText)
			}
		})
	}
}

func TestFormatExtractableTurns(t *testing.T) {
	var longTurns []db.Message
	for i := 0; i < 400; i++ {
		longTurns = append(longTurns, db.Message{
			Content:      "Long conversation user prompt message snippet for testing truncation boundary",
			ResponseText: "Long conversation assistant response message snippet for testing truncation",
		})
	}

	tests := []struct {
		name      string
		turns     []db.Message
		wantEmpty bool
		maxLen    int
	}{
		{"NilTurns", nil, true, 0},
		{"WhitespaceTurns", []db.Message{{Content: "   ", ResponseText: "  "}}, true, 0},
		{"TruncationBoundary", longTurns, false, 20000},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := formatExtractableTurns(tc.turns)
			if tc.wantEmpty {
				if got != "" {
					t.Errorf("formatExtractableTurns() = %q, want empty", got)
				}
				return
			}
			if len(got) == 0 || (tc.maxLen > 0 && len(got) > tc.maxLen) {
				t.Errorf("len(got) = %d, want > 0 and <= %d", len(got), tc.maxLen)
			}
		})
	}
}

func TestExtractActiveConversationFacts(t *testing.T) {
	tests := []struct {
		name          string
		nilStore      bool
		nilClient     bool
		nilLLM        bool
		closedDB      bool
		zeroActive    bool
		mutexLocked   bool
		cancelContext bool
		threadErr     bool
		wantErr       bool
	}{
		{"NilArgs", true, true, true, false, false, false, false, false, true},
		{"ClosedDB", false, false, false, true, false, false, false, false, true},
		{"ZeroActiveConversations", false, false, false, false, true, false, false, false, false},
		{"MutexContentionReturnsNil", false, false, false, false, false, true, false, false, false},
		{"CancelledContext", false, false, false, false, false, false, true, false, true},
		{"ThreadProcessingErrorLogged", false, false, false, false, false, false, false, true, false},
		{"SuccessExtraction", false, false, false, false, false, false, false, false, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var store *db.FakeStore
			if !tc.nilStore {
				store = setupTestStore(t)
				defer store.Close()
				now := time.Now().UTC()
				if !tc.zeroActive {
					_ = store.InsertMessage(ctx, db.Message{
						ID: "m-act-1", ThreadID: "th-act-1", Content: "I prefer vim keybindings", ResponseText: "Noted!",
						Status: db.StatusCompleted, Metadata: db.MessageMetadata{IsAdmin: true}, CreatedAt: now, UpdatedAt: now,
					})
				}
				if tc.closedDB {
					_ = store.Close()
				}
			}

			var client *Client
			if !tc.nilClient {
				client = newMockClient(func(req *http.Request) (*http.Response, error) {
					return mockEmbeddingResponse(makeDimVector(1.0, 0.0)), nil
				})
			}

			var llmFunc LLMClientFunc
			if !tc.nilLLM {
				llmFunc = func(ctx context.Context, prompt string) (string, error) {
					if tc.threadErr {
						return "", errors.New("simulated thread llm error")
					}
					return `{"facts":[{"category":"user_preference","fact_text":"User prefers vim keybindings","importance_score":0.9}]}`, nil
				}
			}

			if tc.mutexLocked {
				extractionMutex.Lock()
				defer extractionMutex.Unlock()
			}

			if tc.cancelContext {
				cancel()
			}

			err := ExtractActiveConversationFacts(ctx, store, client, llmFunc, 12)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestProcessThreadFacts(t *testing.T) {
	tests := []struct {
		name           string
		nilStore       bool
		emptyAmbient   bool
		turnsErr       bool
		llmErr         bool
		llmBadJSON     bool
		emptyFactText  bool
		maxRowErr      bool
		dupCheckErr    bool
		reinforceErr   bool
		insertErr      bool
		emptyWatermark bool
		postWatermark  bool
		postExtractErr bool
		semanticDup    bool
		crossReinforce bool
		wantErr        bool
		wantSubstr     string
	}{
		{"NilStore", true, false, false, false, false, false, false, false, false, false, false, false, false, false, false, true, "database is nil"},
		{"EmptyExtractableTurnsAmbient", false, true, false, false, false, false, false, false, false, false, false, false, false, false, false, false, ""},
		{"GetUnextractedTurnsError", false, false, true, false, false, false, false, false, false, false, false, false, false, false, false, true, "failed to get unextracted turns"},
		{"LLMCallError", false, false, false, true, false, false, false, false, false, false, false, false, false, false, false, true, "LLM fact extraction call failed"},
		{"LLMInvalidJSON", false, false, false, false, true, false, false, false, false, false, false, false, false, false, false, true, "failed to parse extracted facts JSON"},
		{"EmptyFactTextEmbeddingErrorSwallowed", false, false, false, false, false, true, false, false, false, false, false, false, false, false, false, false, ""},
		{"MaxRowIDError", false, false, false, false, false, false, true, false, false, false, false, false, false, false, false, true, "failed to get max message rowid"},
		{"FindDuplicateFactErrorWarningSwallowed", false, false, false, false, false, false, false, true, false, false, false, false, false, false, false, false, ""},
		{"ReinforceFactErrorWarningSwallowed", false, false, false, false, false, false, false, false, true, false, false, false, false, false, false, false, ""},
		{"InsertFactErrorWarningSwallowed", false, false, false, false, false, false, false, false, false, true, false, false, false, false, false, false, ""},
		{"EmptyTranscriptWatermarkWarningSwallowed", false, false, false, false, false, false, false, false, false, false, true, false, false, false, false, false, ""},
		{"PostExtractionWatermarkWarningsSwallowed", false, false, false, false, false, false, false, false, false, false, false, true, true, false, false, false, ""},
		{"SemanticDeduplicationWithinThread", false, false, false, false, false, false, false, false, false, false, false, false, false, true, false, false, ""},
		{"CrossThreadGlobalDeduplicationAndReinforce", false, false, false, false, false, false, false, false, false, false, false, false, false, false, true, false, ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			now := time.Now().UTC()
			adminMeta := db.MessageMetadata{IsAdmin: true}

			var store db.Store
			var fake *db.FakeStore
			if !tc.nilStore {
				fake = setupTestStore(t)
				defer fake.Close()

				if tc.maxRowErr {
					store = &errRowIDStore{FakeStore: fake}
				} else if tc.insertErr {
					store = &errInsertFactStore{FakeStore: fake}
				} else if tc.emptyWatermark {
					store = &mockErrWatermarkFactStore{FakeStore: fake, watermarkErr: errors.New("watermark error")}
				} else if tc.postWatermark {
					store = &mockErrWatermarkFactStore{FakeStore: fake, watermarkErr: errors.New("post watermark error"), extractedAtErr: errors.New("post extracted_at error")}
				} else {
					store = fake
				}

				if tc.turnsErr {
					fake.FailNext("GetUnextractedAdminTurns", errors.New("turns query error"))
				}
				if tc.dupCheckErr {
					fake.FailNext("FindDuplicateFact", errors.New("dup check error"))
				}
				if tc.reinforceErr {
					fake.FailNext("ReinforceFact", errors.New("reinforce error"))
				}

				if tc.emptyAmbient {
					_ = fake.InsertMessage(ctx, db.Message{
						ID: "m-amb", ThreadID: "th-amb", Content: "hello", ResponseText: "[AMBIENT_SILENT]",
						Status: db.StatusCompleted, Metadata: adminMeta, CreatedAt: now, UpdatedAt: now,
					})
				} else if !tc.turnsErr && !tc.maxRowErr && !tc.emptyWatermark {
					_ = fake.InsertMessage(ctx, db.Message{
						ID: "m-main", ThreadID: "th-main", Content: "Alex likes matcha", ResponseText: "Noted!",
						Status: db.StatusCompleted, Metadata: adminMeta, CreatedAt: now, UpdatedAt: now,
					})
				}

				if tc.semanticDup || tc.reinforceErr {
					_, _ = fake.InsertFact(ctx, "user_pref", "Alex likes matcha", 1.0, makeDimVector(1.0, 0.0))
				}
				if tc.crossReinforce {
					_, _ = fake.InsertFact(ctx, "user_pref", "Alex likes matcha", 0.50, makeDimVector(1.0, 0.0))
				}
			}

			client := newMockClient(func(req *http.Request) (*http.Response, error) {
				if tc.emptyFactText {
					return nil, errors.New("embedding fail")
				}
				return mockEmbeddingResponse(makeDimVector(1.0, 0.0)), nil
			})

			llmFunc := func(ctx context.Context, prompt string) (string, error) {
				if tc.llmErr {
					return "", errors.New("llm rate limited")
				}
				if tc.llmBadJSON {
					return "invalid json output", nil
				}
				if tc.emptyFactText {
					return `{"facts":[{"category":"user_pref","fact_text":"","importance_score":1.0},{"category":"user_pref","fact_text":"Valid fact","importance_score":1.0}]}`, nil
				}
				return `{"facts":[{"category":"user_pref","fact_text":"Alex likes matcha","importance_score":0.90}]}`, nil
			}

			threadID := "th-main"
			if tc.emptyAmbient {
				threadID = "th-amb"
			} else if tc.emptyWatermark {
				threadID = "th-empty"
			}

			err := processThreadFacts(ctx, store, client, llmFunc, threadID)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if tc.wantSubstr != "" && !strings.Contains(err.Error(), tc.wantSubstr) {
					t.Errorf("error %q does not contain %q", err.Error(), tc.wantSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tc.semanticDup {
				facts, err := fake.GetFactsByThreadWithEmbeddings(ctx, "th-main")
				if err != nil || len(facts) != 1 {
					t.Errorf("expected 1 fact after deduplication, got %d (err: %v)", len(facts), err)
				}
			}
			if tc.crossReinforce {
				facts, err := fake.GetFactsPaginated(ctx, db.FactsFilter{Limit: 10})
				if err != nil || len(facts.Facts) != 1 {
					t.Fatalf("expected 1 reinforced fact, got %d (err: %v)", len(facts.Facts), err)
				}
				if facts.Facts[0].ReinforceCount < 2 {
					t.Errorf("expected reinforce count >= 2, got %d", facts.Facts[0].ReinforceCount)
				}
			}
		})
	}
}
