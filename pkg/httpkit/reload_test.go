package httpkit

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

type testCfg struct {
	Port  string
	Model string
	Tags  []string
}

// fixtureLoader returns a declarative load func: it yields loadErr when set,
// otherwise nextCfg (which may be nil to simulate a broken loader).
func fixtureLoader(nextCfg *testCfg, loadErr error) func() (*testCfg, error) {
	return func() (*testCfg, error) {
		if loadErr != nil {
			return nil, loadErr
		}
		return nextCfg, nil
	}
}

func portChanged(old, next *testCfg) bool {
	return old.Port != next.Port
}

func TestNewReloadable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		initial   *testCfg
		nilLoader bool
		wantPanic bool
	}{
		{name: "valid", initial: &testCfg{Port: "8080"}},
		{name: "nil initial", initial: nil, wantPanic: true},
		{name: "nil loader", initial: &testCfg{Port: "8080"}, nilLoader: true, wantPanic: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			load := fixtureLoader(&testCfg{}, nil)
			if tc.nilLoader {
				load = nil
			}
			defer func() {
				if got := recover() != nil; got != tc.wantPanic {
					t.Fatalf("panic = %v, want %v", got, tc.wantPanic)
				}
			}()
			r := NewReloadable(tc.initial, load, nil)
			if r.Current() != tc.initial {
				t.Fatalf("Current() = %p, want initial %p", r.Current(), tc.initial)
			}
		})
	}
}

func TestReloadable_Reload(t *testing.T) {
	t.Parallel()

	initial := &testCfg{Port: "8080", Model: "a", Tags: []string{"x"}}
	tests := []struct {
		name           string
		nextCfg        *testCfg
		loadErr        error
		restartCheck   bool
		wantStatus     ReloadStatus
		wantErr        bool
		wantCurrent    testCfg
		wantRestarting bool
	}{
		{
			name:        "load error retains lkgc",
			loadErr:     errors.New("yaml: bad indent"),
			wantStatus:  ReloadLKGCRetained,
			wantErr:     true,
			wantCurrent: testCfg{Port: "8080", Model: "a", Tags: []string{"x"}},
		},
		{
			name:        "nil snapshot retains lkgc",
			nextCfg:     nil,
			wantStatus:  ReloadLKGCRetained,
			wantErr:     true,
			wantCurrent: testCfg{Port: "8080", Model: "a", Tags: []string{"x"}},
		},
		{
			name:        "deep equal is unchanged",
			nextCfg:     &testCfg{Port: "8080", Model: "a", Tags: []string{"x"}},
			wantStatus:  ReloadUnchanged,
			wantCurrent: testCfg{Port: "8080", Model: "a", Tags: []string{"x"}},
		},
		{
			name:        "live field change is applied",
			nextCfg:     &testCfg{Port: "8080", Model: "b", Tags: []string{"x", "y"}},
			wantStatus:  ReloadApplied,
			wantCurrent: testCfg{Port: "8080", Model: "b", Tags: []string{"x", "y"}},
		},
		{
			name:        "nil restartRequired applies every change",
			nextCfg:     &testCfg{Port: "9090", Model: "a", Tags: []string{"x"}},
			wantStatus:  ReloadApplied,
			wantCurrent: testCfg{Port: "9090", Model: "a", Tags: []string{"x"}},
		},
		{
			name:           "restart field change is not swapped",
			nextCfg:        &testCfg{Port: "9090", Model: "b", Tags: []string{"x"}},
			restartCheck:   true,
			wantStatus:     ReloadRestarting,
			wantCurrent:    testCfg{Port: "8080", Model: "a", Tags: []string{"x"}},
			wantRestarting: true,
		},
		{
			name:         "restart check passes for live change",
			nextCfg:      &testCfg{Port: "8080", Model: "c", Tags: []string{"x"}},
			restartCheck: true,
			wantStatus:   ReloadApplied,
			wantCurrent:  testCfg{Port: "8080", Model: "c", Tags: []string{"x"}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var restartRequired func(old, next *testCfg) bool
			if tc.restartCheck {
				restartRequired = portChanged
			}
			seed := *initial
			r := NewReloadable(&seed, fixtureLoader(tc.nextCfg, tc.loadErr), restartRequired)

			status, err := r.Reload()
			if status != tc.wantStatus {
				t.Errorf("status = %q, want %q", status, tc.wantStatus)
			}
			if (err != nil) != tc.wantErr {
				t.Errorf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if got := r.Current(); !reflect.DeepEqual(*got, tc.wantCurrent) {
				t.Errorf("Current() = %+v, want %+v", *got, tc.wantCurrent)
			}
			select {
			case <-r.RestartRequested():
				if !tc.wantRestarting {
					t.Error("RestartRequested closed unexpectedly")
				}
			default:
				if tc.wantRestarting {
					t.Error("RestartRequested not closed")
				}
			}

			// A second identical reload must be idempotent: never double-close
			// the restart channel and never change the outcome class.
			status2, _ := r.Reload()
			if tc.wantStatus == ReloadApplied && status2 != ReloadUnchanged {
				t.Errorf("second reload status = %q, want %q", status2, ReloadUnchanged)
			}
			if tc.wantStatus != ReloadApplied && status2 != tc.wantStatus {
				t.Errorf("second reload status = %q, want %q", status2, tc.wantStatus)
			}
		})
	}
}

func TestReloadable_Handler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		method      string
		remoteAddr  string
		nextCfg     *testCfg
		loadErr     error
		wantCode    int
		wantStatus  string
		wantErrBody string
		wantPort    string
	}{
		{name: "ipv4 loopback applies", method: http.MethodPost, remoteAddr: "127.0.0.1:5555", nextCfg: &testCfg{Port: "8080", Model: "b"}, wantCode: http.StatusOK, wantStatus: "reloaded", wantPort: "8080"},
		{name: "ipv6 loopback unchanged", method: http.MethodPost, remoteAddr: "[::1]:5555", nextCfg: &testCfg{Port: "8080", Model: "a"}, wantCode: http.StatusOK, wantStatus: "unchanged", wantPort: "8080"},
		{name: "bad config still 200", method: http.MethodPost, remoteAddr: "127.0.0.1:5555", loadErr: errors.New("yaml: line 3: bad"), wantCode: http.StatusOK, wantStatus: "lkgc_retained", wantErrBody: "yaml: line 3: bad", wantPort: "8080"},
		{name: "restart field change", method: http.MethodPost, remoteAddr: "127.0.0.1:5555", nextCfg: &testCfg{Port: "9090", Model: "a"}, wantCode: http.StatusOK, wantStatus: "restarting", wantPort: "8080"},
		{name: "pod network rejected", method: http.MethodPost, remoteAddr: "10.42.0.9:5555", nextCfg: &testCfg{Port: "8080", Model: "b"}, wantCode: http.StatusForbidden, wantPort: "8080"},
		{name: "unparseable remote rejected", method: http.MethodPost, remoteAddr: "garbage", nextCfg: &testCfg{Port: "8080", Model: "b"}, wantCode: http.StatusForbidden, wantPort: "8080"},
		{name: "hostname remote rejected", method: http.MethodPost, remoteAddr: "localhost:5555", nextCfg: &testCfg{Port: "8080", Model: "b"}, wantCode: http.StatusForbidden, wantPort: "8080"},
		{name: "get not allowed", method: http.MethodGet, remoteAddr: "127.0.0.1:5555", nextCfg: &testCfg{Port: "8080", Model: "b"}, wantCode: http.StatusMethodNotAllowed, wantPort: "8080"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := NewReloadable(&testCfg{Port: "8080", Model: "a"}, fixtureLoader(tc.nextCfg, tc.loadErr), portChanged)
			req := httptest.NewRequest(tc.method, "/-/reload", nil)
			req.RemoteAddr = tc.remoteAddr
			rec := httptest.NewRecorder()

			r.Handler("testsvc").ServeHTTP(rec, req)

			if rec.Code != tc.wantCode {
				t.Fatalf("code = %d, want %d (body %q)", rec.Code, tc.wantCode, rec.Body.String())
			}
			if got := r.Current().Port; got != tc.wantPort {
				t.Errorf("Current().Port = %q, want %q", got, tc.wantPort)
			}
			if tc.wantCode != http.StatusOK {
				return
			}
			var body map[string]string
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode body %q: %v", rec.Body.String(), err)
			}
			if body["status"] != tc.wantStatus {
				t.Errorf("body status = %q, want %q", body["status"], tc.wantStatus)
			}
			errVal, hasErr := body["error"]
			if errVal != tc.wantErrBody || hasErr != (tc.wantErrBody != "") {
				t.Errorf("body error = %q (present %v), want %q", errVal, hasErr, tc.wantErrBody)
			}
		})
	}
}
