package twilio

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/tracing"
)

var twilioHostRegex = regexp.MustCompile(`^(?:api\.twilio\.com|.*\.twiliocdn\.com)$`)

func readBodySnippet(r io.Reader) string {
	b, err := io.ReadAll(io.LimitReader(r, 4096))
	if err != nil {
		return ""
	}
	return string(b)
}

// CreateCallRequest encapsulates arguments for dialing an outbound call via Twilio.
type CreateCallRequest struct {
	To                     string
	From                   string
	TwiMLURL               string
	StatusCallback         string
	AsyncAmdStatusCallback string
	SendDigits             string
	MachineDetection       string
	AsyncAmd               string
}

// TwilioClient defines the outbound Twilio telephony interface.
type TwilioClient interface {
	CreateCall(ctx context.Context, req CreateCallRequest) (string, error)
	HangupCall(ctx context.Context, providerCallSID string) error
	IsCallActive(ctx context.Context, providerCallSID string) (bool, error)
	SendSMS(ctx context.Context, to, from, body string) (string, error)
	EnsurePhoneNumber(ctx context.Context, areaCode, webhookBaseURL string) (string, error)
	DownloadRecording(ctx context.Context, recordingURL string) ([]byte, string, error)
}

// TwilioOption configures HTTPTwilioClient.
type TwilioOption func(*HTTPTwilioClient)

// WithTwilioBaseURL overrides the Twilio API base URL.
func WithTwilioBaseURL(baseURL string) TwilioOption {
	return func(c *HTTPTwilioClient) {
		c.baseURL = strings.TrimRight(baseURL, "/")
	}
}

// WithTwilioHTTPClient overrides the HTTP client used for Twilio requests.
func WithTwilioHTTPClient(client *http.Client) TwilioOption {
	return func(c *HTTPTwilioClient) {
		c.httpClient = client
	}
}

// HTTPTwilioClient implements TwilioClient using standard HTTP requests.
type HTTPTwilioClient struct {
	accountSID string
	authToken  string
	baseURL    string
	httpClient *http.Client
}

// NewHTTPTwilioClient initializes an HTTPTwilioClient.
func NewHTTPTwilioClient(accountSID, authToken string, opts ...TwilioOption) *HTTPTwilioClient {
	c := &HTTPTwilioClient{
		accountSID: accountSID,
		authToken:  authToken,
		baseURL:    "https://api.twilio.com",
		httpClient: tracing.ExternalClient(&http.Client{Timeout: 30 * time.Second}),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// CreateCall initiates an outbound call with Async AMD enabled.
func (c *HTTPTwilioClient) CreateCall(ctx context.Context, req CreateCallRequest) (string, error) {
	if req.To == "" || req.From == "" || req.TwiMLURL == "" {
		return "", fmt.Errorf("twilio: to, from, and url are required")
	}

	vals := url.Values{}
	vals.Set("To", req.To)
	vals.Set("From", req.From)
	vals.Set("Url", req.TwiMLURL)
	if req.StatusCallback != "" {
		vals.Set("StatusCallback", req.StatusCallback)
	}
	if req.SendDigits != "" {
		vals.Set("SendDigits", req.SendDigits)
	}

	machDetection := req.MachineDetection
	if machDetection == "" {
		machDetection = "DetectMessageEnd"
	}
	asyncAmd := req.AsyncAmd
	if asyncAmd == "" {
		asyncAmd = "true"
	}
	vals.Set("MachineDetection", machDetection)
	vals.Set("AsyncAmd", asyncAmd)
	if req.AsyncAmdStatusCallback != "" {
		vals.Set("AsyncAmdStatusCallback", req.AsyncAmdStatusCallback)
	}

	endpoint := fmt.Sprintf("%s/2010-04-01/Accounts/%s/Calls.json", c.baseURL, c.accountSID)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(vals.Encode()))
	if err != nil {
		return "", err
	}
	httpReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	httpReq.SetBasicAuth(c.accountSID, c.authToken)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("twilio: create call: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("twilio: create call status %d: %s", resp.StatusCode, readBodySnippet(resp.Body))
	}

	var result struct {
		SID string `json:"sid"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("twilio: decode call response: %w", err)
	}
	if result.SID == "" {
		return "", fmt.Errorf("twilio: missing call sid in response")
	}
	return result.SID, nil
}

func (c *HTTPTwilioClient) postFormNoContent(ctx context.Context, endpointURL, opLabel string, data url.Values) error {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpointURL, strings.NewReader(data.Encode()))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	httpReq.SetBasicAuth(c.accountSID, c.authToken)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("twilio: %s: %w", opLabel, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("twilio: %s status %d: %s", opLabel, resp.StatusCode, readBodySnippet(resp.Body))
	}
	return nil
}

// HangupCall completes or cancels an active call.
func (c *HTTPTwilioClient) HangupCall(ctx context.Context, providerCallSID string) error {
	if providerCallSID == "" {
		return fmt.Errorf("twilio: call sid is required")
	}

	vals := url.Values{}
	vals.Set("Status", "completed")

	endpoint := fmt.Sprintf("%s/2010-04-01/Accounts/%s/Calls/%s.json", c.baseURL, c.accountSID, providerCallSID)
	return c.postFormNoContent(ctx, endpoint, "hangup call", vals)
}

// IsCallActive checks if the remote Twilio call is queued, ringing, or in-progress.
func (c *HTTPTwilioClient) IsCallActive(ctx context.Context, providerCallSID string) (bool, error) {
	if providerCallSID == "" {
		return false, fmt.Errorf("twilio: call sid is required")
	}

	endpoint := fmt.Sprintf("%s/2010-04-01/Accounts/%s/Calls/%s.json", c.baseURL, c.accountSID, providerCallSID)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, http.NoBody)
	if err != nil {
		return false, err
	}
	httpReq.SetBasicAuth(c.accountSID, c.authToken)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return false, fmt.Errorf("twilio: is call active: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return false, fmt.Errorf("twilio: query call status %d: %s", resp.StatusCode, readBodySnippet(resp.Body))
	}

	var result struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return false, fmt.Errorf("twilio: decode call status: %w", err)
	}

	switch strings.ToLower(result.Status) {
	case "queued", "ringing", "in-progress":
		return true, nil
	default:
		return false, nil
	}
}

// SendSMS transmits an outbound SMS message via Twilio REST.
func (c *HTTPTwilioClient) SendSMS(ctx context.Context, to, from, body string) (string, error) {
	if to == "" || from == "" || body == "" {
		return "", fmt.Errorf("twilio: to, from, and body are required")
	}

	vals := url.Values{}
	vals.Set("To", to)
	vals.Set("From", from)
	vals.Set("Body", body)

	endpoint := fmt.Sprintf("%s/2010-04-01/Accounts/%s/Messages.json", c.baseURL, c.accountSID)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(vals.Encode()))
	if err != nil {
		return "", err
	}
	httpReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	httpReq.SetBasicAuth(c.accountSID, c.authToken)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("twilio: send sms: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("twilio: send sms status %d: %s", resp.StatusCode, readBodySnippet(resp.Body))
	}

	var result struct {
		SID string `json:"sid"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("twilio: decode sms response: %w", err)
	}
	return result.SID, nil
}

// EnsurePhoneNumber discovers or purchases an incoming phone number and updates webhook URLs.
func (c *HTTPTwilioClient) EnsurePhoneNumber(ctx context.Context, areaCode, webhookBaseURL string) (string, error) {
	var voiceURL, statusURL, smsURL string
	if webhookBaseURL != "" {
		webhookBaseURL = strings.TrimRight(webhookBaseURL, "/")
		if !strings.HasSuffix(webhookBaseURL, "/api/webhooks") {
			webhookBaseURL += "/api/webhooks"
		}
		voiceURL = webhookBaseURL + "/twilio/voice"
		statusURL = webhookBaseURL + "/twilio/status"
		smsURL = webhookBaseURL + "/twilio/sms"
	}

	existing, err := c.listIncomingPhoneNumbers(ctx)
	if err != nil {
		return "", err
	}

	if len(existing) > 0 {
		target := existing[0]
		if target.VoiceURL != voiceURL || target.StatusCallback != statusURL || target.SmsURL != smsURL {
			if err := c.updatePhoneNumberWebhooks(ctx, target.SID, voiceURL, statusURL, smsURL); err != nil {
				return "", err
			}
		}
		return target.PhoneNumber, nil
	}

	return c.searchAndPurchasePhoneNumber(ctx, areaCode, voiceURL, statusURL, smsURL)
}

type incomingNumberRecord struct {
	SID            string `json:"sid"`
	PhoneNumber    string `json:"phone_number"`
	VoiceURL       string `json:"voice_url"`
	StatusCallback string `json:"status_callback"`
	SmsURL         string `json:"sms_url"`
}

func (c *HTTPTwilioClient) listIncomingPhoneNumbers(ctx context.Context) ([]incomingNumberRecord, error) {
	endpoint := fmt.Sprintf("%s/2010-04-01/Accounts/%s/IncomingPhoneNumbers.json", c.baseURL, c.accountSID)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, http.NoBody)
	if err != nil {
		return nil, err
	}
	httpReq.SetBasicAuth(c.accountSID, c.authToken)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("twilio: list incoming numbers: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("twilio: list incoming numbers status %d: %s", resp.StatusCode, readBodySnippet(resp.Body))
	}

	var listRes struct {
		IncomingPhoneNumbers []incomingNumberRecord `json:"incoming_phone_numbers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&listRes); err != nil {
		return nil, fmt.Errorf("twilio: decode incoming numbers: %w", err)
	}
	return listRes.IncomingPhoneNumbers, nil
}

func (c *HTTPTwilioClient) updatePhoneNumberWebhooks(ctx context.Context, sid, voiceURL, statusURL, smsURL string) error {
	vals := url.Values{}
	vals.Set("VoiceUrl", voiceURL)
	vals.Set("StatusCallback", statusURL)
	vals.Set("SmsUrl", smsURL)

	updateEndpoint := fmt.Sprintf("%s/2010-04-01/Accounts/%s/IncomingPhoneNumbers/%s.json", c.baseURL, c.accountSID, sid)
	return c.postFormNoContent(ctx, updateEndpoint, "update incoming number", vals)
}

func (c *HTTPTwilioClient) searchAndPurchasePhoneNumber(ctx context.Context, areaCode, voiceURL, statusURL, smsURL string) (string, error) {
	if areaCode == "" {
		areaCode = "510"
	}
	searchEndpoint := fmt.Sprintf("%s/2010-04-01/Accounts/%s/AvailablePhoneNumbers/US/Local.json?AreaCode=%s", c.baseURL, c.accountSID, areaCode)
	sReq, err := http.NewRequestWithContext(ctx, http.MethodGet, searchEndpoint, http.NoBody)
	if err != nil {
		return "", err
	}
	sReq.SetBasicAuth(c.accountSID, c.authToken)

	sResp, err := c.httpClient.Do(sReq)
	if err != nil {
		return "", fmt.Errorf("twilio: search available numbers: %w", err)
	}
	defer sResp.Body.Close()

	if sResp.StatusCode < 200 || sResp.StatusCode >= 300 {
		return "", fmt.Errorf("twilio: search available numbers status %d: %s", sResp.StatusCode, readBodySnippet(sResp.Body))
	}

	var searchRes struct {
		AvailablePhoneNumbers []struct {
			PhoneNumber string `json:"phone_number"`
		} `json:"available_phone_numbers"`
	}
	if err = json.NewDecoder(sResp.Body).Decode(&searchRes); err != nil {
		return "", fmt.Errorf("twilio: decode available numbers: %w", err)
	}
	if len(searchRes.AvailablePhoneNumbers) == 0 {
		return "", fmt.Errorf("twilio: no available phone numbers found in area code %s", areaCode)
	}

	chosen := searchRes.AvailablePhoneNumbers[0].PhoneNumber

	buyVals := url.Values{}
	buyVals.Set("PhoneNumber", chosen)
	buyVals.Set("VoiceUrl", voiceURL)
	buyVals.Set("StatusCallback", statusURL)
	buyVals.Set("SmsUrl", smsURL)

	buyEndpoint := fmt.Sprintf("%s/2010-04-01/Accounts/%s/IncomingPhoneNumbers.json", c.baseURL, c.accountSID)
	bReq, err := http.NewRequestWithContext(ctx, http.MethodPost, buyEndpoint, strings.NewReader(buyVals.Encode()))
	if err != nil {
		return "", err
	}
	bReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	bReq.SetBasicAuth(c.accountSID, c.authToken)

	bResp, err := c.httpClient.Do(bReq)
	if err != nil {
		return "", fmt.Errorf("twilio: purchase phone number: %w", err)
	}
	defer bResp.Body.Close()

	if bResp.StatusCode < 200 || bResp.StatusCode >= 300 {
		return "", fmt.Errorf("twilio: purchase phone number status %d: %s", bResp.StatusCode, readBodySnippet(bResp.Body))
	}

	var buyRes struct {
		PhoneNumber string `json:"phone_number"`
	}
	if err := json.NewDecoder(bResp.Body).Decode(&buyRes); err != nil {
		return "", fmt.Errorf("twilio: decode purchased phone number: %w", err)
	}
	return buyRes.PhoneNumber, nil
}

// DownloadRecording downloads and validates a Twilio recording with SSRF, redirect, and size bounds.
func (c *HTTPTwilioClient) DownloadRecording(ctx context.Context, recordingURL string) ([]byte, string, error) {
	parsed, err := url.Parse(recordingURL)
	if err != nil {
		return nil, "", fmt.Errorf("invalid recording url: %w", err)
	}
	if parsed.Scheme != "https" {
		return nil, "", fmt.Errorf("insecure recording url scheme %q: https required", parsed.Scheme)
	}
	hostname := parsed.Hostname()
	if !twilioHostRegex.MatchString(hostname) {
		return nil, "", fmt.Errorf("untrusted recording host %q: must be twilio domain", hostname)
	}

	redirectClient := &http.Client{
		Timeout: 30 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return fmt.Errorf("stopped after 10 redirects")
			}
			if req.URL.Scheme != "https" {
				return fmt.Errorf("redirect to insecure scheme: %s", req.URL.Scheme)
			}
			if !twilioHostRegex.MatchString(req.URL.Hostname()) {
				return fmt.Errorf("redirect to untrusted host: %s", req.URL.Hostname())
			}
			if len(via) > 0 && req.URL.Hostname() != via[0].URL.Hostname() {
				req.Header.Del("Authorization")
			}
			return nil
		},
	}
	if c.httpClient != nil && c.httpClient.Transport != nil {
		redirectClient.Transport = c.httpClient.Transport
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, recordingURL, http.NoBody)
	if err != nil {
		return nil, "", err
	}
	if parsed.Hostname() == "api.twilio.com" {
		httpReq.SetBasicAuth(c.accountSID, c.authToken)
	}

	resp, err := redirectClient.Do(httpReq)
	if err != nil {
		return nil, "", fmt.Errorf("download recording request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, "", fmt.Errorf("download recording returned status %d", resp.StatusCode)
	}

	const maxSize = 8 << 20 // 8 MB
	limited := io.LimitReader(resp.Body, maxSize+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, "", fmt.Errorf("read recording body: %w", err)
	}
	if len(data) > maxSize {
		return nil, "", fmt.Errorf("recording exceeds maximum size limit of 8MB")
	}

	contentType := resp.Header.Get("Content-Type")
	return data, contentType, nil
}
