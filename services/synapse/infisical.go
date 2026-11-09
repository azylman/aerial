package main

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
	kubeCAPath    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	kubeTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"
)

func loadK8sCertPool(caPath string) *x509.CertPool {
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

func getKubeServiceAccountToken(explicitToken string) string {
	if explicitToken != "" {
		return explicitToken
	}
	if data, err := os.ReadFile(kubeTokenPath); err == nil {
		return strings.TrimSpace(string(data))
	}
	return ""
}

func getK8sHTTPClient(apiServer string, httpClient *http.Client) *http.Client {
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
	if rootCAs := loadK8sCertPool(kubeCAPath); rootCAs != nil {
		transport.TLSClientConfig = &tls.Config{
			RootCAs: rootCAs,
		}
	}
	return &http.Client{
		Timeout:   15 * time.Second,
		Transport: transport,
	}
}
