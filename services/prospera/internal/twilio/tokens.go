package twilio

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1" //nolint:gosec // G505: Twilio signs X-Twilio-Signature with HMAC-SHA1; the protocol fixes the hash
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

// ValidateTwilioSignature validates Twilio request authenticity via RFC 2104 HMAC-SHA1.
func ValidateTwilioSignature(authToken, fullURL string, form url.Values, signature string) bool {
	if authToken == "" || signature == "" {
		return false
	}

	keys := make([]string, 0, len(form))
	for k := range form {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var sb strings.Builder
	sb.WriteString(fullURL)
	for _, k := range keys {
		for _, v := range form[k] {
			sb.WriteString(k)
			sb.WriteString(v)
		}
	}

	mac := hmac.New(sha1.New, []byte(authToken))
	mac.Write([]byte(sb.String()))
	expected := base64.StdEncoding.EncodeToString(mac.Sum(nil))

	return hmac.Equal([]byte(expected), []byte(signature))
}

type streamTokenEntry struct {
	callID    string
	expiresAt time.Time
}

// StreamTokenManager manages single-use crypto-random tokens for WebSocket authentication.
type StreamTokenManager struct {
	mu     sync.Mutex
	nowFn  func() time.Time
	randFn func([]byte) (int, error)
	tokens map[string]streamTokenEntry
}

// StreamTokenOption configures a StreamTokenManager.
type StreamTokenOption func(*StreamTokenManager)

// WithEntropyReader overrides the crypto-random source used for token generation (tests).
func WithEntropyReader(fn func([]byte) (int, error)) StreamTokenOption {
	return func(m *StreamTokenManager) {
		m.randFn = fn
	}
}

// NewStreamTokenManager initializes a StreamTokenManager.
func NewStreamTokenManager(nowFn func() time.Time, opts ...StreamTokenOption) *StreamTokenManager {
	if nowFn == nil {
		nowFn = time.Now
	}
	m := &StreamTokenManager{
		nowFn:  nowFn,
		tokens: make(map[string]streamTokenEntry),
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Issue generates a 32-byte crypto-random hex token bound to callID with a specified TTL.
func (m *StreamTokenManager) Issue(callID string, ttl time.Duration) (string, error) {
	b := make([]byte, 32)
	readRand := rand.Read
	if m.randFn != nil {
		readRand = m.randFn
	}
	if _, err := readRand(b); err != nil {
		return "", fmt.Errorf("stream token: generate random bytes: %w", err)
	}
	token := hex.EncodeToString(b)

	m.mu.Lock()
	defer m.mu.Unlock()
	m.cleanupLocked()

	m.tokens[token] = streamTokenEntry{
		callID:    callID,
		expiresAt: m.nowFn().Add(ttl),
	}
	return token, nil
}

// Consume atomically retrieves and deletes a token, checking for expiration.
func (m *StreamTokenManager) Consume(token string) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cleanupLocked()

	entry, ok := m.tokens[token]
	if !ok {
		return "", false
	}
	delete(m.tokens, token)

	if !m.nowFn().Before(entry.expiresAt) {
		return "", false
	}
	return entry.callID, true
}

func (m *StreamTokenManager) cleanupLocked() {
	now := m.nowFn()
	for k, v := range m.tokens {
		if !now.Before(v.expiresAt) {
			delete(m.tokens, k)
		}
	}
}

var xmlEscaper = strings.NewReplacer(
	"&", "&amp;",
	"<", "&lt;",
	">", "&gt;",
	"\"", "&#34;",
	"'", "&#39;",
)

// EscapeXML escapes a string for safe embedding in TwiML XML.
func EscapeXML(s string) string {
	return xmlEscaper.Replace(s)
}

// GenerateConnectStreamTwiML generates XML-escaped TwiML connecting the call to a Media Stream WebSocket.
func GenerateConnectStreamTwiML(wsURL string) string {
	return fmt.Sprintf("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Response>\n  <Connect>\n    <Stream url=\"%s\" />\n  </Connect>\n</Response>", EscapeXML(wsURL))
}

// GenerateVoicemailTwiML generates XML-escaped TwiML playing a greeting and recording incoming voicemail.
func GenerateVoicemailTwiML(greeting, callbackURL string) string {
	return fmt.Sprintf("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Response>\n  <Say>%s</Say>\n  <Record action=\"%s\" method=\"POST\" maxLength=\"120\" playBeep=\"true\" transcribe=\"false\" />\n</Response>", EscapeXML(greeting), EscapeXML(callbackURL))
}
