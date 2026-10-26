package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"
)

func computeExpectedTwilioSignature(authToken, fullURL string, form url.Values) string {
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
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func TestValidateTwilioSignature(t *testing.T) {
	t.Parallel()

	token := "12345authtoken"
	targetURL := "https://prospera.aerial/twilio/voice?call_id=c123"
	form := url.Values{
		"CallSid": {"CA1234567890abcdef"},
		"From":    {"+15105550100"},
		"To":      {"+15105550199"},
		"Digits":  {"4"},
	}

	validSig := computeExpectedTwilioSignature(token, targetURL, form)

	tests := []struct {
		name      string
		token     string
		fullURL   string
		form      url.Values
		sig       string
		wantValid bool
	}{
		{
			name:      "exact match with form",
			token:     token,
			fullURL:   targetURL,
			form:      form,
			sig:       validSig,
			wantValid: true,
		},
		{
			name:      "nil form match",
			token:     token,
			fullURL:   targetURL,
			form:      nil,
			sig:       computeExpectedTwilioSignature(token, targetURL, nil),
			wantValid: true,
		},
		{
			name:      "tampered url",
			token:     token,
			fullURL:   targetURL + "&extra=1",
			form:      form,
			sig:       validSig,
			wantValid: false,
		},
		{
			name:      "tampered form value",
			token:     token,
			fullURL:   targetURL,
			form:      url.Values{"CallSid": {"CA999999"}},
			sig:       validSig,
			wantValid: false,
		},
		{
			name:      "wrong signature",
			token:     token,
			fullURL:   targetURL,
			form:      form,
			sig:       "invalid_base64_sig==",
			wantValid: false,
		},
		{
			name:      "empty auth token",
			token:     "",
			fullURL:   targetURL,
			form:      form,
			sig:       validSig,
			wantValid: false,
		},
		{
			name:      "empty signature",
			token:     token,
			fullURL:   targetURL,
			form:      form,
			sig:       "",
			wantValid: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ValidateTwilioSignature(tc.token, tc.fullURL, tc.form, tc.sig)
			if got != tc.wantValid {
				t.Fatalf("ValidateTwilioSignature() = %v, want %v", got, tc.wantValid)
			}
		})
	}
}

func TestStreamTokenManager(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)
	mgr := NewStreamTokenManager(func() time.Time {
		return now
	})

	tests := []struct {
		name       string
		callID     string
		ttl        time.Duration
		advance    time.Duration
		wantFirst  bool
		wantReplay bool
	}{
		{
			name:       "valid token consumed once and rejected on replay",
			callID:     "call-101",
			ttl:        30 * time.Second,
			advance:    5 * time.Second,
			wantFirst:  true,
			wantReplay: false,
		},
		{
			name:       "expired token ttl zero rejected",
			callID:     "call-102",
			ttl:        0,
			advance:    0,
			wantFirst:  false,
			wantReplay: false,
		},
		{
			name:       "expired token past ttl rejected",
			callID:     "call-103",
			ttl:        10 * time.Second,
			advance:    15 * time.Second,
			wantFirst:  false,
			wantReplay: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			currentTime := now
			mgrInstance := NewStreamTokenManager(func() time.Time {
				return currentTime
			})

			token, err := mgrInstance.Issue(tc.callID, tc.ttl)
			if err != nil {
				t.Fatalf("Issue() failed: %v", err)
			}
			if len(token) != 64 {
				t.Fatalf("Issue() token length = %d, want 64 hex chars (32 bytes)", len(token))
			}

			currentTime = currentTime.Add(tc.advance)
			gotCallID, ok := mgrInstance.Consume(token)
			if ok != tc.wantFirst {
				t.Fatalf("first Consume() ok = %v, want %v", ok, tc.wantFirst)
			}
			if tc.wantFirst && gotCallID != tc.callID {
				t.Fatalf("first Consume() callID = %q, want %q", gotCallID, tc.callID)
			}

			gotReplayID, replayOK := mgrInstance.Consume(token)
			if replayOK != tc.wantReplay {
				t.Fatalf("replay Consume() ok = %v, want %v", replayOK, tc.wantReplay)
			}
			if replayOK && gotReplayID != "" {
				t.Fatalf("replay Consume() should return empty callID, got %q", gotReplayID)
			}
		})
	}

	// Consume non-existent token
	if _, ok := mgr.Consume("nonexistent_token_123"); ok {
		t.Fatal("Consume(nonexistent) returned ok = true, want false")
	}
}

func TestGenerateConnectStreamTwiML(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		streamURL   string
		mustContain []string
	}{
		{
			name:        "connect stream twiml with escaped url",
			streamURL:   "wss://prospera.aerial/twilio/media-stream?token=abc&call=123",
			mustContain: []string{"<Connect>", "<Stream url=\"wss://prospera.aerial/twilio/media-stream?token=abc&amp;call=123\"", "</Connect>"},
		},
		{
			name:        "connect stream twiml with query parameters",
			streamURL:   "wss://example.com/stream?a=1&b=2",
			mustContain: []string{"<Connect>", "<Stream url=\"wss://example.com/stream?a=1&amp;b=2\"", "</Connect>"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := GenerateConnectStreamTwiML(tc.streamURL)
			for _, exp := range tc.mustContain {
				if !strings.Contains(got, exp) {
					t.Fatalf("generated TwiML %q does not contain %q", got, exp)
				}
			}
		})
	}
}

func TestGenerateVoicemailTwiML(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		greeting    string
		actionURL   string
		mustContain []string
	}{
		{
			name:        "voicemail twiml with escaped greeting and action",
			greeting:    "Hello & welcome to Aerial's line",
			actionURL:   "https://prospera.aerial/twilio/voicemail?call=1&foo=bar",
			mustContain: []string{"<Say>Hello &amp; welcome to Aerial&#39;s line</Say>", "<Record action=\"https://prospera.aerial/twilio/voicemail?call=1&amp;foo=bar\""},
		},
		{
			name:        "escape xml chars in greeting and callback",
			greeting:    "Hello <User> & friends",
			actionURL:   "https://example.com/cb?x=1&y=2",
			mustContain: []string{"Hello &lt;User&gt; &amp; friends", "https://example.com/cb?x=1&amp;y=2"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := GenerateVoicemailTwiML(tc.greeting, tc.actionURL)
			for _, exp := range tc.mustContain {
				if !strings.Contains(got, exp) {
					t.Fatalf("generated TwiML %q does not contain %q", got, exp)
				}
			}
		})
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestDownloadRecording(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		recordingURL  string
		redirectURL   string
		mockStatus    int
		mockBody      []byte
		mockType      string
		wantErrSubstr string
	}{
		{
			name:          "rejects non-https scheme",
			recordingURL:  "http://169.254.169.254/latest/meta-data",
			wantErrSubstr: "https required",
		},
		{
			name:          "rejects untrusted host",
			recordingURL:  "https://evil.example.com/recording.wav",
			wantErrSubstr: "untrusted recording host",
		},
		{
			name:         "successful download with twiliocdn redirect and auth strip",
			recordingURL: "https://api.twilio.com/2010-04-01/Accounts/AC123/Recordings/RE123.wav",
			redirectURL:  "https://media.twiliocdn.com/rec123.wav",
			mockStatus:   http.StatusOK,
			mockBody:     []byte("WAVE-AUDIO-DATA-BYTES"),
			mockType:     "audio/wav",
		},
		{
			name:          "rejects recording exceeding 8MB limit",
			recordingURL:  "https://api.twilio.com/2010-04-01/Accounts/AC123/Recordings/huge.wav",
			mockStatus:    http.StatusOK,
			mockBody:      make([]byte, 9*1024*1024),
			mockType:      "audio/wav",
			wantErrSubstr: "exceeds maximum size limit",
		},
		{
			name:          "rejects redirect to untrusted host",
			recordingURL:  "https://api.twilio.com/2010-04-01/Accounts/AC123/Recordings/bad_redirect.wav",
			redirectURL:   "https://attacker.com/stolen.wav",
			wantErrSubstr: "redirect to untrusted host",
		},
		{
			name:          "rejects recording download with 404 status",
			recordingURL:  "https://api.twilio.com/2010-04-01/Accounts/AC123/Recordings/notfound.wav",
			mockStatus:    http.StatusNotFound,
			wantErrSubstr: "download recording returned status 404",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tripper := roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.URL.Hostname() == "api.twilio.com" && tc.redirectURL != "" {
					if !strings.HasPrefix(req.Header.Get("Authorization"), "Basic ") {
						t.Errorf("api.twilio.com request missing Basic auth, got %q", req.Header.Get("Authorization"))
					}
					header := make(http.Header)
					header.Set("Location", tc.redirectURL)
					return &http.Response{
						StatusCode: http.StatusFound,
						Header:     header,
						Body:       io.NopCloser(strings.NewReader("")),
					}, nil
				}
				if tc.redirectURL != "" && req.URL.Hostname() == "media.twiliocdn.com" {
					if req.Header.Get("Authorization") != "" {
						t.Errorf("CDN redirect still contains Authorization header: %s", req.Header.Get("Authorization"))
					}
				}
				header := make(http.Header)
				if tc.mockType != "" {
					header.Set("Content-Type", tc.mockType)
				}
				return &http.Response{
					StatusCode: tc.mockStatus,
					Header:     header,
					Body:       io.NopCloser(bytes.NewReader(tc.mockBody)),
				}, nil
			})

			client := NewHTTPTwilioClient("AC123", "token123", WithTwilioHTTPClient(&http.Client{
				Transport: tripper,
			}))

			data, contentType, err := client.DownloadRecording(context.Background(), tc.recordingURL)
			if tc.wantErrSubstr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrSubstr) {
					t.Fatalf("DownloadRecording() err = %v, want substring %q", err, tc.wantErrSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("DownloadRecording() unexpected error: %v", err)
			}
			if string(data) != string(tc.mockBody) {
				t.Fatalf("DownloadRecording() data = %q, want %q", string(data), string(tc.mockBody))
			}
			if contentType != tc.mockType {
				t.Fatalf("DownloadRecording() contentType = %q, want %q", contentType, tc.mockType)
			}
		})
	}
}

func TestHTTPTwilioClient_CreateCall(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		req        CreateCallRequest
		statusCode int
		respBody   string
		wantSID    string
		wantErr    bool
	}{
		{
			name: "successful call creation with AMD and SendDigits",
			req: CreateCallRequest{
				To:                     "+15105550199",
				From:                   "+15105550100",
				TwiMLURL:               "https://prospera.aerial/twilio/voice",
				AsyncAmdStatusCallback: "https://prospera.aerial/twilio/amd",
				SendDigits:             "ww1w2",
			},
			statusCode: http.StatusOK,
			respBody:   `{"sid":"CA9988776655"}`,
			wantSID:    "CA9988776655",
		},
		{
			name: "validation failure missing To",
			req: CreateCallRequest{
				From:     "+15105550100",
				TwiMLURL: "https://prospera.aerial/twilio/voice",
			},
			wantErr: true,
		},
		{
			name: "server returns 500 error",
			req: CreateCallRequest{
				To:       "+15105550199",
				From:     "+15105550100",
				TwiMLURL: "https://prospera.aerial/twilio/voice",
			},
			statusCode: http.StatusInternalServerError,
			respBody:   "internal error",
			wantErr:    true,
		},
		{
			name: "missing SID in response body",
			req: CreateCallRequest{
				To:       "+15105550199",
				From:     "+15105550100",
				TwiMLURL: "https://prospera.aerial/twilio/voice",
			},
			statusCode: http.StatusOK,
			respBody:   `{"status":"queued"}`,
			wantErr:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/2010-04-01/Accounts/AC123/Calls.json" {
					http.Error(w, "not found", http.StatusNotFound)
					return
				}
				_ = r.ParseForm()
				if tc.req.SendDigits != "" && r.FormValue("SendDigits") != tc.req.SendDigits {
					http.Error(w, "missing SendDigits", http.StatusBadRequest)
					return
				}
				if tc.req.AsyncAmdStatusCallback != "" && r.FormValue("AsyncAmdStatusCallback") != tc.req.AsyncAmdStatusCallback {
					http.Error(w, "missing AsyncAmdStatusCallback", http.StatusBadRequest)
					return
				}
				if r.FormValue("AsyncAmd") != "true" || r.FormValue("MachineDetection") != "DetectMessageEnd" {
					http.Error(w, "missing AMD params", http.StatusBadRequest)
					return
				}
				if tc.statusCode != 0 {
					w.WriteHeader(tc.statusCode)
				}
				_, _ = w.Write([]byte(tc.respBody))
			}))
			t.Cleanup(server.Close)

			client := NewHTTPTwilioClient("AC123", "token123", WithTwilioBaseURL(server.URL), WithTwilioHTTPClient(server.Client()))
			sid, err := client.CreateCall(context.Background(), tc.req)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if sid != tc.wantSID {
				t.Fatalf("sid = %q, want %q", sid, tc.wantSID)
			}
		})
	}
}

func TestHTTPTwilioClient_HangupCall(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		callSID    string
		statusCode int
		wantErr    bool
	}{
		{
			name:       "successful hangup",
			callSID:    "CA123",
			statusCode: http.StatusOK,
		},
		{
			name:    "empty call sid fails",
			callSID: "",
			wantErr: true,
		},
		{
			name:       "server returns 500 error",
			callSID:    "CA123",
			statusCode: http.StatusInternalServerError,
			wantErr:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/2010-04-01/Accounts/AC123/Calls/"+tc.callSID+".json" {
					http.Error(w, "not found", http.StatusNotFound)
					return
				}
				_ = r.ParseForm()
				if r.FormValue("Status") != "completed" {
					http.Error(w, "expected completed status", http.StatusBadRequest)
					return
				}
				w.WriteHeader(tc.statusCode)
			}))
			t.Cleanup(server.Close)

			client := NewHTTPTwilioClient("AC123", "token123", WithTwilioBaseURL(server.URL), WithTwilioHTTPClient(server.Client()))
			err := client.HangupCall(context.Background(), tc.callSID)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestHTTPTwilioClient_IsCallActive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		callSID    string
		statusCode int
		respBody   string
		wantActive bool
		wantErr    bool
	}{
		{
			name:       "active in-progress call",
			callSID:    "CA_active",
			statusCode: http.StatusOK,
			respBody:   `{"status":"in-progress"}`,
			wantActive: true,
		},
		{
			name:       "completed inactive call",
			callSID:    "CA_ended",
			statusCode: http.StatusOK,
			respBody:   `{"status":"completed"}`,
			wantActive: false,
		},
		{
			name:    "empty call sid fails",
			callSID: "",
			wantErr: true,
		},
		{
			name:       "server returns 500 error",
			callSID:    "CA123",
			statusCode: http.StatusInternalServerError,
			respBody:   "error",
			wantErr:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Path != "/2010-04-01/Accounts/AC123/Calls/"+tc.callSID+".json" {
					http.Error(w, "not found", http.StatusNotFound)
					return
				}
				if tc.statusCode != 0 {
					w.WriteHeader(tc.statusCode)
				}
				_, _ = w.Write([]byte(tc.respBody))
			}))
			t.Cleanup(server.Close)

			client := NewHTTPTwilioClient("AC123", "token123", WithTwilioBaseURL(server.URL), WithTwilioHTTPClient(server.Client()))
			active, err := client.IsCallActive(context.Background(), tc.callSID)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if active != tc.wantActive {
				t.Fatalf("active = %v, want %v", active, tc.wantActive)
			}
		})
	}
}

func TestHTTPTwilioClient_SendSMS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		to         string
		from       string
		body       string
		statusCode int
		respBody   string
		wantSID    string
		wantErr    bool
	}{
		{
			name:       "successful sms dispatch",
			to:         "+15105550199",
			from:       "+15105550100",
			body:       "Hello test message",
			statusCode: http.StatusOK,
			respBody:   `{"sid":"SM12345"}`,
			wantSID:    "SM12345",
		},
		{
			name:    "empty fields validation error",
			to:      "",
			from:    "",
			body:    "",
			wantErr: true,
		},
		{
			name:       "server returns 500 error",
			to:         "+15105550199",
			from:       "+15105550100",
			body:       "Hello test message",
			statusCode: http.StatusInternalServerError,
			respBody:   "internal error",
			wantErr:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/2010-04-01/Accounts/AC123/Messages.json" {
					http.Error(w, "not found", http.StatusNotFound)
					return
				}
				_ = r.ParseForm()
				if r.FormValue("To") != tc.to || r.FormValue("Body") != tc.body {
					http.Error(w, "bad form", http.StatusBadRequest)
					return
				}
				if tc.statusCode != 0 {
					w.WriteHeader(tc.statusCode)
				}
				_, _ = w.Write([]byte(tc.respBody))
			}))
			t.Cleanup(server.Close)

			client := NewHTTPTwilioClient("AC123", "token123", WithTwilioBaseURL(server.URL), WithTwilioHTTPClient(server.Client()))
			sid, err := client.SendSMS(context.Background(), tc.to, tc.from, tc.body)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if sid != tc.wantSID {
				t.Fatalf("sid = %q, want %q", sid, tc.wantSID)
			}
		})
	}
}

func TestHTTPTwilioClient_EnsurePhoneNumber(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		ownedBody  string
		ownedCode  int
		updateCode int
		searchBody string
		searchCode int
		buyBody    string
		buyCode    int
		wantNumber string
		wantErr    bool
	}{
		{
			name:       "existing owned number updated",
			ownedBody:  `{"incoming_phone_numbers":[{"sid":"PN111","phone_number":"+15105550100","voice_url":"http://old"}]}`,
			ownedCode:  http.StatusOK,
			updateCode: http.StatusOK,
			wantNumber: "+15105550100",
		},
		{
			name:       "purchase available number when none owned",
			ownedBody:  `{"incoming_phone_numbers":[]}`,
			ownedCode:  http.StatusOK,
			searchBody: `{"available_phone_numbers":[{"phone_number":"+15105550999"}]}`,
			searchCode: http.StatusOK,
			buyBody:    `{"phone_number":"+15105550999"}`,
			buyCode:    http.StatusOK,
			wantNumber: "+15105550999",
		},
		{
			name:      "list incoming numbers returns error",
			ownedCode: http.StatusInternalServerError,
			wantErr:   true,
		},
		{
			name:       "search numbers returns error",
			ownedBody:  `{"incoming_phone_numbers":[]}`,
			ownedCode:  http.StatusOK,
			searchCode: http.StatusInternalServerError,
			wantErr:    true,
		},
		{
			name:       "no numbers available in area code",
			ownedBody:  `{"incoming_phone_numbers":[]}`,
			ownedCode:  http.StatusOK,
			searchBody: `{"available_phone_numbers":[]}`,
			searchCode: http.StatusOK,
			wantErr:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/2010-04-01/Accounts/AC123/IncomingPhoneNumbers.json":
					if tc.ownedCode != 0 {
						w.WriteHeader(tc.ownedCode)
					}
					_, _ = w.Write([]byte(tc.ownedBody))
				case r.Method == http.MethodPost && r.URL.Path == "/2010-04-01/Accounts/AC123/IncomingPhoneNumbers/PN111.json":
					if err := r.ParseForm(); err == nil {
						if r.FormValue("VoiceUrl") != "https://prospera.aerial/api/webhooks/twilio/voice" {
							t.Errorf("VoiceUrl = %q, want https://prospera.aerial/api/webhooks/twilio/voice", r.FormValue("VoiceUrl"))
						}
						if r.FormValue("StatusCallback") != "https://prospera.aerial/api/webhooks/twilio/status" {
							t.Errorf("StatusCallback = %q, want https://prospera.aerial/api/webhooks/twilio/status", r.FormValue("StatusCallback"))
						}
						if r.FormValue("SmsUrl") != "https://prospera.aerial/api/webhooks/twilio/sms" {
							t.Errorf("SmsUrl = %q, want https://prospera.aerial/api/webhooks/twilio/sms", r.FormValue("SmsUrl"))
						}
					}
					if tc.updateCode != 0 {
						w.WriteHeader(tc.updateCode)
					}
				case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "AvailablePhoneNumbers"):
					if tc.searchCode != 0 {
						w.WriteHeader(tc.searchCode)
					}
					_, _ = w.Write([]byte(tc.searchBody))
				case r.Method == http.MethodPost && r.URL.Path == "/2010-04-01/Accounts/AC123/IncomingPhoneNumbers.json":
					if err := r.ParseForm(); err == nil && r.FormValue("VoiceUrl") != "" {
						if r.FormValue("VoiceUrl") != "https://prospera.aerial/api/webhooks/twilio/voice" {
							t.Errorf("buy VoiceUrl = %q, want https://prospera.aerial/api/webhooks/twilio/voice", r.FormValue("VoiceUrl"))
						}
					}
					if tc.buyCode != 0 {
						w.WriteHeader(tc.buyCode)
					}
					_, _ = w.Write([]byte(tc.buyBody))
				default:
					http.NotFound(w, r)
				}
			}))
			t.Cleanup(server.Close)

			client := NewHTTPTwilioClient("AC123", "token123", WithTwilioBaseURL(server.URL), WithTwilioHTTPClient(server.Client()))
			phone, err := client.EnsurePhoneNumber(context.Background(), "510", "https://prospera.aerial")
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if phone != tc.wantNumber {
				t.Fatalf("phone = %q, want %q", phone, tc.wantNumber)
			}
		})
	}
}

func TestHTTPTwilioClient_NetworkErrors(t *testing.T) {
	t.Parallel()

	clientBad := NewHTTPTwilioClient("AC123", "token123", WithTwilioBaseURL("http://127.0.0.1:1"))

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	if _, err := clientBad.CreateCall(ctx, CreateCallRequest{To: "+15105550199", From: "+15105550100", TwiMLURL: "https://test"}); err == nil {
		t.Error("expected create call network error")
	}
	if err := clientBad.HangupCall(ctx, "CA123"); err == nil {
		t.Error("expected hangup network error")
	}
	if _, err := clientBad.IsCallActive(ctx, "CA123"); err == nil {
		t.Error("expected is call active network error")
	}
	if _, err := clientBad.SendSMS(ctx, "+15105550199", "+15105550100", "test"); err == nil {
		t.Error("expected send sms network error")
	}
	if _, err := clientBad.EnsurePhoneNumber(ctx, "510", "https://test"); err == nil {
		t.Error("expected ensure phone number network error")
	}
}

func TestReadBodySnippet(t *testing.T) {
	t.Parallel()

	if got := readBodySnippet(strings.NewReader("sample error body")); got != "sample error body" {
		t.Errorf("readBodySnippet = %q, want sample error body", got)
	}
	if got := readBodySnippet(&errSnippetReader{}); got != "" {
		t.Errorf("readBodySnippet on error = %q, want empty", got)
	}
}

type errSnippetReader struct{}

func (*errSnippetReader) Read(p []byte) (int, error) {
	return 0, errors.New("read error")
}

func WithTwilioBaseURL(baseURL string) TwilioOption {
	return func(c *HTTPTwilioClient) {
		c.baseURL = strings.TrimRight(baseURL, "/")
	}
}

func WithTwilioHTTPClient(client *http.Client) TwilioOption {
	return func(c *HTTPTwilioClient) {
		c.httpClient = client
	}
}

func TestEscapeXML(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in   string
		want string
	}{
		{"normal", "normal"},
		{"<tag>&'\"", "&lt;tag&gt;&amp;&#39;&#34;"},
	}
	for _, tc := range tests {
		if got := escapeXML(tc.in); got != tc.want {
			t.Fatalf("escapeXML(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestHTTPTwilioClient_DownloadRecording_AllBranches(t *testing.T) {
	t.Parallel()

	client := NewHTTPTwilioClient("AC123", "token123")

	t.Run("invalid url", func(t *testing.T) {
		t.Parallel()
		_, _, err := client.DownloadRecording(context.Background(), "::not-a-url")
		if err == nil {
			t.Fatal("expected error on invalid url")
		}
	})

	t.Run("non https scheme", func(t *testing.T) {
		t.Parallel()
		_, _, err := client.DownloadRecording(context.Background(), "http://api.twilio.com/rec.wav")
		if err == nil {
			t.Fatal("expected error on non-https scheme")
		}
	})

	t.Run("untrusted host", func(t *testing.T) {
		t.Parallel()
		_, _, err := client.DownloadRecording(context.Background(), "https://evil.com/rec.wav")
		if err == nil {
			t.Fatal("expected error on untrusted host")
		}
	})

	t.Run("transport error", func(t *testing.T) {
		t.Parallel()
		c := NewHTTPTwilioClient("AC123", "token123", WithTwilioHTTPClient(&http.Client{
			Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
				return nil, errors.New("network drop")
			}),
		}))
		_, _, err := c.DownloadRecording(context.Background(), "https://api.twilio.com/rec.wav")
		if err == nil {
			t.Fatal("expected error on transport failure")
		}
	})

	t.Run("status error", func(t *testing.T) {
		t.Parallel()
		c := NewHTTPTwilioClient("AC123", "token123", WithTwilioHTTPClient(&http.Client{
			Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusNotFound,
					Body:       io.NopCloser(strings.NewReader("not found")),
				}, nil
			}),
		}))
		_, _, err := c.DownloadRecording(context.Background(), "https://api.twilio.com/rec.wav")
		if err == nil {
			t.Fatal("expected error on status 404")
		}
	})

	t.Run("success wav", func(t *testing.T) {
		t.Parallel()
		c := NewHTTPTwilioClient("AC123", "token123", WithTwilioHTTPClient(&http.Client{
			Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
				header := make(http.Header)
				header.Set("Content-Type", "audio/wav")
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     header,
					Body:       io.NopCloser(bytes.NewReader([]byte("RIFF1234WAVEfmt"))),
				}, nil
			}),
		}))
		data, ctype, err := c.DownloadRecording(context.Background(), "https://api.twilio.com/rec.wav")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ctype != "audio/wav" || len(data) == 0 {
			t.Fatalf("unexpected recording response: %q, len=%d", ctype, len(data))
		}
	})
}

func TestStreamTokenManager_EntropyError(t *testing.T) {
	t.Parallel()
	mgr := NewStreamTokenManager(nil)
	mgr.randFn = func([]byte) (int, error) {
		return 0, errors.New("entropy error")
	}
	if _, err := mgr.Issue("c1", time.Minute); err == nil {
		t.Fatal("expected error on entropy failure")
	}
}
