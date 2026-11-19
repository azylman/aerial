package kube

import (
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

var (
	KubeCAPath    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	KubeTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"
)

// LoadK8sCertPool reads the Kubernetes service account CA certificate and returns a CertPool.
func LoadK8sCertPool(caPath string) *x509.CertPool {
	if caPath == "" {
		return nil
	}
	caData, err := os.ReadFile(caPath)
	if err != nil || len(caData) == 0 {
		return nil
	}
	certPool, err := x509.SystemCertPool()
	if err != nil || certPool == nil {
		certPool = x509.NewCertPool()
	} else {
		certPool = certPool.Clone()
	}
	if certPool.AppendCertsFromPEM(caData) {
		return certPool
	}
	return nil
}

// GetKubeServiceAccountToken returns the explicit token if provided, or reads from tokenPath (defaulting to KubeTokenPath).
func GetKubeServiceAccountToken(explicitToken string, tokenPath ...string) string {
	if explicitToken != "" {
		return explicitToken
	}
	path := KubeTokenPath
	if len(tokenPath) > 0 && tokenPath[0] != "" {
		path = tokenPath[0]
	}
	if data, err := os.ReadFile(path); err == nil {
		return strings.TrimSpace(string(data))
	}
	return ""
}

// GetK8sHTTPClient builds an HTTP client configured with service account CA certificates for HTTPS endpoints.
func GetK8sHTTPClient(apiServer string, httpClient *http.Client, caPath ...string) *http.Client {
	if httpClient != nil && !strings.HasPrefix(apiServer, "https://") {
		return httpClient
	}
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		IdleConnTimeout: 90 * time.Second,
	}
	path := KubeCAPath
	if len(caPath) > 0 && caPath[0] != "" {
		path = caPath[0]
	}
	if rootCAs := LoadK8sCertPool(path); rootCAs != nil {
		transport.TLSClientConfig = &tls.Config{
			RootCAs: rootCAs,
		}
	}
	return &http.Client{
		Timeout:   15 * time.Second,
		Transport: transport,
	}
}
