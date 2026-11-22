package k8sauth

import (
	"crypto/tls"
	"net"
	"net/http"
	"strings"
	"time"
)

// DefaultCAPath is the standard mounted path for the Kubernetes ServiceAccount CA bundle.
const DefaultCAPath = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

const defaultAPIServerTimeout = 15 * time.Second

// NewAPIServerClient returns the HTTP client for talking to apiServer. A
// non-https apiServer gets base unchanged (a 15s client when base is nil). An
// https apiServer gets a new client with base's timeout (15s when base is nil)
// and a dedicated transport trusting the CA bundle at caPath when it loads.
// Build it once per API server and reuse it; never per request.
func NewAPIServerClient(apiServer string, base *http.Client, caPath string) *http.Client {
	timeout := defaultAPIServerTimeout
	if base != nil {
		timeout = base.Timeout
	}
	if !strings.HasPrefix(apiServer, "https://") {
		if base != nil {
			return base
		}
		return &http.Client{Timeout: timeout}
	}
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		IdleConnTimeout: 90 * time.Second,
	}
	if rootCAs := LoadCAPool(caPath); rootCAs != nil {
		transport.TLSClientConfig = &tls.Config{RootCAs: rootCAs, MinVersion: tls.VersionTLS12}
	}
	return &http.Client{Timeout: timeout, Transport: transport}
}
