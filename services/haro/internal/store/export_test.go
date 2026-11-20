package store

import (
	"github.com/azylman/aerial/pkg/k8sauth"
)

// SetVanadisTokenSource sets the token source for outbound Vanadis calls during tests.
func SetVanadisTokenSource(ts *k8sauth.TokenSource) {
	vanadisMu.Lock()
	defer vanadisMu.Unlock()
	vanadisTokenSource = ts
}

// GetVanadisTokenSource returns the current token source for outbound Vanadis calls during tests.
func GetVanadisTokenSource() *k8sauth.TokenSource {
	vanadisMu.RLock()
	defer vanadisMu.RUnlock()
	return vanadisTokenSource
}

// SetVanadisTokenPath sets the path to the ServiceAccount token for Vanadis calls during tests.
func SetVanadisTokenPath(path string) {
	vanadisMu.Lock()
	defer vanadisMu.Unlock()
	vanadisTokenSource = k8sauth.NewTokenSource(path, nil)
}
