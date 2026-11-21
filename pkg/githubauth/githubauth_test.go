package githubauth

import (
	"bytes"
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// t0 is 2026-11-21T12:00:00Z (unix 1795262400).
var t0 = time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)

var testKey = sync.OnceValue(func() *rsa.PrivateKey {
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	return k
})

// pemFixture returns the PEM text for a named key fixture.
func pemFixture(kind string) string {
	switch kind {
	case "pkcs1":
		return string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(testKey())}))
	case "pkcs8":
		der, err := x509.MarshalPKCS8PrivateKey(testKey())
		if err != nil {
			panic(err)
		}
		return string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
	case "ec":
		k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			panic(err)
		}
		der, err := x509.MarshalPKCS8PrivateKey(k)
		if err != nil {
			panic(err)
		}
		return string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
	case "garbage-pem":
		return "-----BEGIN RSA PRIVATE " + "KEY-----\neyJhbGciOiJSUzI1NiJ9\n-----END RSA PRIVATE " + "KEY-----\n"
	default:
		return kind
	}
}

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

type fakeMetrics struct {
	mu        sync.Mutex
	requests  map[string]int
	remaining map[string]float64
	mints     map[string]int
}

func newFakeMetrics() *fakeMetrics {
	return &fakeMetrics{requests: map[string]int{}, remaining: map[string]float64{}, mints: map[string]int{}}
}

func (m *fakeMetrics) IncRequest(source string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requests[source]++
}

func (m *fakeMetrics) SetRateLimitRemaining(source string, v float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.remaining[source] = v
}

func (m *fakeMetrics) IncTokenMint(result string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mints[result]++
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func newTestLogger() (*slog.Logger, *syncBuffer) {
	buf := &syncBuffer{}
	return slog.New(slog.NewTextHandler(buf, nil)), buf
}

func assertNoSecrets(t *testing.T, label, text string) {
	t.Helper()
	for _, marker := range []string{"eyJ", "ghs_", "BEGIN"} {
		if strings.Contains(text, marker) {
			t.Fatalf("%s leaks %q: %s", label, marker, text)
		}
	}
}

type mintReply struct {
	status int
	body   string
}

// newMintServer serves POST /app/installations/42/access_tokens, answering
// call i with replies[min(i, len-1)] after verifying the App JWT.
func newMintServer(t *testing.T, replies []mintReply) *httptest.Server {
	t.Helper()
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/app/installations/42/access_tokens" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if !validJWT(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")) {
			http.Error(w, "bad jwt", http.StatusUnauthorized)
			return
		}
		i := int(calls.Add(1)) - 1
		if i >= len(replies) {
			i = len(replies) - 1
		}
		w.WriteHeader(replies[i].status)
		_, _ = w.Write([]byte(replies[i].body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func validJWT(jwt string) bool {
	parts := strings.Split(jwt, ".")
	if len(parts) != 3 {
		return false
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return false
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	return rsa.VerifyPKCS1v15(&testKey().PublicKey, crypto.SHA256, digest[:], sig) == nil
}

const (
	tok1Body = `{"token":"tok-1","expires_at":"2026-11-21T13:00:00Z"}`
	tok2Body = `{"token":"tok-2","expires_at":"2026-11-21T14:00:00Z"}`
	leakBody = `{"message":"Bad credentials ghs_leaked eyJleaked"}`
)

func TestSignJWT(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		appID      int64
		now        time.Time
		wantClaims string
	}{
		{"t0", 12345, t0, `{"iat":1795262340,"exp":1795262700,"iss":"12345"}`},
		{"epoch plus 100s", 7, time.Unix(100, 0), `{"iat":40,"exp":400,"iss":"7"}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			jwt, err := signJWT(testKey(), tc.appID, tc.now)
			if err != nil {
				t.Fatalf("signJWT() error = %v", err)
			}
			parts := strings.Split(jwt, ".")
			if len(parts) != 3 {
				t.Fatalf("jwt has %d parts, want 3", len(parts))
			}
			if parts[0] != "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9" {
				t.Fatalf("header = %q", parts[0])
			}
			claims, err := base64.RawURLEncoding.DecodeString(parts[1])
			if err != nil {
				t.Fatalf("decode claims: %v", err)
			}
			if string(claims) != tc.wantClaims {
				t.Fatalf("claims = %s, want %s", claims, tc.wantClaims)
			}
			if !validJWT(jwt) {
				t.Fatal("signature does not verify")
			}
		})
	}
}

func TestNewAppTokenSource(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		appID          int64
		installationID int64
		pemKind        string
		wantErr        string
	}{
		{"pkcs1", 1, 42, "pkcs1", ""},
		{"pkcs8", 1, 42, "pkcs8", ""},
		{"ecdsa key", 1, 42, "ec", "not RSA"},
		{"not pem", 1, 42, "eyJnot-a-key ghs_x", "not PEM encoded"},
		{"garbage pem body", 1, 42, "garbage-pem", "neither PKCS#1 nor PKCS#8"},
		{"empty pem", 1, 42, "", "not PEM encoded"},
		{"zero app id", 0, 42, "pkcs1", "app id must be positive"},
		{"zero installation id", 1, 0, "pkcs1", "installation id must be positive"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			src, err := NewAppTokenSource(AppConfig{
				AppID:          tc.appID,
				InstallationID: tc.installationID,
				PrivateKeyPEM:  pemFixture(tc.pemKind),
			}, nil, nil, "")
			if tc.wantErr == "" {
				if err != nil || src == nil {
					t.Fatalf("NewAppTokenSource() = %v, %v; want source", src, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("NewAppTokenSource() error = %v, want containing %q", err, tc.wantErr)
			}
			assertNoSecrets(t, "error", err.Error())
		})
	}
}

type tokenStep struct {
	advance    time.Duration
	invalidate string
	cancel     bool
	concurrent int
	wantValue  string
	wantSource string
	wantErr    bool
}

func TestAppTokenSourceToken(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		replies   []mintReply
		pat       string
		steps     []tokenStep
		wantMints map[string]int
		wantLog   string
	}{
		{
			name:    "caches until five minutes before expiry",
			replies: []mintReply{{201, tok1Body}, {201, tok2Body}},
			steps: []tokenStep{
				{wantValue: "tok-1", wantSource: "app"},
				{advance: 54 * time.Minute, wantValue: "tok-1", wantSource: "app"},
				{advance: 2 * time.Minute, wantValue: "tok-2", wantSource: "app"},
			},
			wantMints: map[string]int{"success": 2},
		},
		{
			name:    "mint failure falls back to PAT for 60s",
			replies: []mintReply{{500, leakBody}, {201, tok2Body}},
			pat:     "pat-x",
			steps: []tokenStep{
				{wantValue: "pat-x", wantSource: "pat"},
				{advance: 59 * time.Second, wantValue: "pat-x", wantSource: "pat"},
				{advance: time.Second, wantValue: "tok-2", wantSource: "app"},
			},
			wantMints: map[string]int{"error": 1, "success": 1},
			wantLog:   "mint returned status 500",
		},
		{
			name:      "mint failure without PAT errors",
			replies:   []mintReply{{403, leakBody}},
			steps:     []tokenStep{{wantErr: true}, {advance: time.Second, wantErr: true}},
			wantMints: map[string]int{"error": 1},
			wantLog:   "mint returned status 403",
		},
		{
			name:    "invalidate drops only the matching token",
			replies: []mintReply{{201, tok1Body}, {201, tok2Body}},
			steps: []tokenStep{
				{wantValue: "tok-1", wantSource: "app"},
				{invalidate: "other", wantValue: "tok-1", wantSource: "app"},
				{invalidate: "tok-1", wantValue: "tok-2", wantSource: "app"},
			},
			wantMints: map[string]int{"success": 2},
		},
		{
			name:      "malformed mint body falls back",
			replies:   []mintReply{{201, `{"token":"ghs_partial"`}},
			pat:       "pat-x",
			steps:     []tokenStep{{wantValue: "pat-x", wantSource: "pat"}},
			wantMints: map[string]int{"error": 1},
			wantLog:   "decode mint response",
		},
		{
			name:      "mint body without expiry falls back",
			replies:   []mintReply{{201, `{"token":"ghs_noexpiry"}`}},
			pat:       "pat-x",
			steps:     []tokenStep{{wantValue: "pat-x", wantSource: "pat"}},
			wantMints: map[string]int{"error": 1},
			wantLog:   "missing token or expires_at",
		},
		{
			name:    "canceled context does not arm backoff",
			replies: []mintReply{{201, tok1Body}},
			pat:     "pat-x",
			steps: []tokenStep{
				{cancel: true, wantErr: true},
				{wantValue: "tok-1", wantSource: "app"},
			},
			wantMints: map[string]int{"error": 1, "success": 1},
		},
		{
			name:      "concurrent callers share one mint",
			replies:   []mintReply{{201, tok1Body}, {201, tok2Body}},
			steps:     []tokenStep{{concurrent: 16, wantValue: "tok-1", wantSource: "app"}},
			wantMints: map[string]int{"success": 1},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := newMintServer(t, tc.replies)
			clock := &fakeClock{t: t0}
			metrics := newFakeMetrics()
			logger, logs := newTestLogger()
			src, err := NewAppTokenSource(AppConfig{
				AppID:          12345,
				InstallationID: 42,
				PrivateKeyPEM:  pemFixture("pkcs1"),
				APIURL:         srv.URL + "/",
				Logger:         logger,
				Metrics:        metrics,
			}, srv.Client(), clock.Now, tc.pat)
			if err != nil {
				t.Fatalf("NewAppTokenSource() error = %v", err)
			}
			for i, st := range tc.steps {
				runTokenStep(t, src, clock, i, st)
			}
			metrics.mu.Lock()
			assertCounts(t, "mint metrics", metrics.mints, tc.wantMints)
			metrics.mu.Unlock()
			out := logs.String()
			if tc.wantLog != "" && !strings.Contains(out, tc.wantLog) {
				t.Fatalf("logs = %q, want containing %q", out, tc.wantLog)
			}
			assertNoSecrets(t, "logs", out)
		})
	}
}

// runTokenStep advances the clock, applies the step's Invalidate, then calls
// Token (concurrently when st.concurrent > 1) and checks every result.
func runTokenStep(t *testing.T, src TokenSource, clock *fakeClock, i int, st tokenStep) {
	t.Helper()
	clock.Advance(st.advance)
	if st.invalidate != "" {
		src.Invalidate(st.invalidate)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if st.cancel {
		cancel()
	}
	n := max(st.concurrent, 1)
	values := make([]string, n)
	sources := make([]string, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for j := range n {
		wg.Go(func() { values[j], sources[j], errs[j] = src.Token(ctx) })
	}
	wg.Wait()
	for j := range n {
		if (errs[j] != nil) != st.wantErr {
			t.Fatalf("step %d: Token() error = %v, wantErr %v", i, errs[j], st.wantErr)
		}
		if errs[j] != nil {
			assertNoSecrets(t, "error", errs[j].Error())
		}
		if values[j] != st.wantValue || sources[j] != st.wantSource {
			t.Fatalf("step %d: Token() = %q, %q; want %q, %q", i, values[j], sources[j], st.wantValue, st.wantSource)
		}
	}
}

func assertCounts[V int | float64](t *testing.T, label string, got, want map[string]V) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s = %v, want %v", label, got, want)
	}
	for k, v := range want {
		if g, ok := got[k]; !ok || g != v {
			t.Fatalf("%s = %v, want %v", label, got, want)
		}
	}
}

func TestNewStaticTokenSource(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		pat        string
		invalidate string
		wantValue  string
	}{
		{"trims whitespace", "  pat-a \n", "pat-a", "pat-a"},
		{"empty means unauthenticated", "", "", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			src := NewStaticTokenSource(tc.pat)
			src.Invalidate(tc.invalidate)
			value, source, err := src.Token(context.Background())
			if err != nil || value != tc.wantValue || source != "pat" {
				t.Fatalf("Token() = %q, %q, %v; want %q, pat, nil", value, source, err, tc.wantValue)
			}
		})
	}
}

func TestNewTokenSourceFromValues(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		appID          string
		installationID string
		pemKind        string
		pat            string
		wantValue      string
		wantSource     string
		wantLog        string
	}{
		{"no app config uses PAT", "", "", "", "pat-a", "pat-a", "pat", ""},
		{"whitespace-only app config uses PAT", " ", "\n", " ", "pat-a", "pat-a", "pat", ""},
		{"missing key", "12345", "42", "", "pat-a", "pat-a", "pat", "missing=GITHUB_APP_PRIVATE_KEY"},
		{"missing ids", "", "", "pkcs1", "pat-a", "pat-a", "pat", "missing=GITHUB_APP_ID,GITHUB_APP_INSTALLATION_ID"},
		{"non-numeric app id", "abc", "42", "pkcs1", "pat-a", "pat-a", "pat", "GITHUB_APP_ID is not an integer"},
		{"non-numeric installation id", "12345", "x42", "pkcs1", "pat-a", "pat-a", "pat", "GITHUB_APP_INSTALLATION_ID is not an integer"},
		{"bad pem", "12345", "42", "garbage-pem", "pat-a", "pat-a", "pat", "neither PKCS#1 nor PKCS#8"},
		{"zero app id", "0", "42", "pkcs8", "pat-a", "pat-a", "pat", "app id must be positive"},
		{"full app config mints", " 12345 ", "42", "pkcs8", "pat-a", "tok-1", "app", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := newMintServer(t, []mintReply{{201, tok1Body}})
			logger, logs := newTestLogger()
			clock := &fakeClock{t: t0}
			src := NewTokenSourceFromValues(tc.appID, tc.installationID, pemFixture(tc.pemKind), tc.pat, FromValuesOptions{
				APIURL: srv.URL,
				Client: srv.Client(),
				Now:    clock.Now,
				Logger: logger,
			})
			value, source, err := src.Token(context.Background())
			if err != nil || value != tc.wantValue || source != tc.wantSource {
				t.Fatalf("Token() = %q, %q, %v; want %q, %q, nil", value, source, err, tc.wantValue, tc.wantSource)
			}
			out := logs.String()
			if tc.wantLog == "" && out != "" {
				t.Fatalf("unexpected logs: %q", out)
			}
			if !strings.Contains(out, tc.wantLog) {
				t.Fatalf("logs = %q, want containing %q", out, tc.wantLog)
			}
			assertNoSecrets(t, "logs", out)
		})
	}
}
