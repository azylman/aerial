package main

import (
	"net/http"
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, goleak.Cleanup(func(exitCode int) {
		http.DefaultClient.CloseIdleConnections()
		if tr, ok := http.DefaultTransport.(*http.Transport); ok {
			tr.CloseIdleConnections()
		}
		if brainHTTPClient != nil {
			brainHTTPClient.CloseIdleConnections()
		}
		if dockerSocketClient != nil {
			dockerSocketClient.CloseIdleConnections()
		}
		if nomadHTTPClient != nil {
			nomadHTTPClient.CloseIdleConnections()
		}
		if vanadisHTTPClient != nil {
			vanadisHTTPClient.CloseIdleConnections()
		}
		if globalGHPoller != nil && globalGHPoller.client != nil {
			globalGHPoller.client.CloseIdleConnections()
		}
	}))
}
