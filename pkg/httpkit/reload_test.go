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

	base := testCfg{Port: "8080", Model: "a", Tags: []string{"x"}}
	live := testCfg{Port: "8080", Model: "b", Tags: []string{"x", "y"}}
	port := testCfg{Port: "9090", Model: "a", Tags: []string{"x"}}
	modelC := testCfg{Port: "8080", Model: "c", Tags: []string{"x"}}
	badYAML := errors.New("yaml: bad indent")
	// Columns: name, nextCfg, loadErr, restartCheck (use portChanged), wantStatus, wantErr, wantCurrent, wantRestarting,
	// and wantSecond (status of an immediate second Reload with the same loader).
	tests := []struct {
		name           string
		nextCfg        *testCfg
		loadErr        error
		restartCheck   bool
		wantStatus     ReloadStatus
		wantErr        bool
		wantCurrent    testCfg
		wantRestarting bool
		wantSecond     ReloadStatus
	}{
		{"load error retains lkgc", nil, badYAML, false, ReloadLKGCRetained, true, base, false, ReloadLKGCRetained},
		{"nil snapshot retains lkgc", nil, nil, false, ReloadLKGCRetained, true, base, false, ReloadLKGCRetained},
		{"deep equal is unchanged", &testCfg{Port: "8080", Model: "a", Tags: []string{"x"}}, nil, false, ReloadUnchanged, false, base, false, ReloadUnchanged},
		{"live field change is applied", &live, nil, false, ReloadApplied, false, live, false, ReloadUnchanged},
		{"nil restartRequired applies every change", &port, nil, false, ReloadApplied, false, port, false, ReloadUnchanged},
		{"restart field change is not swapped", &testCfg{Port: "9090", Model: "b", Tags: []string{"x"}}, nil, true, ReloadRestarting, false, base, true, ReloadRestarting},
		{"restart check passes for live change", &modelC, nil, true, ReloadApplied, false, modelC, false, ReloadUnchanged},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var restartRequired func(old, next *testCfg) bool
			if tc.restartCheck {
				restartRequired = portChanged
			}
			seed := base
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
			restarting := false
			select {
			case <-r.RestartRequested():
				restarting = true
			default:
			}
			if restarting != tc.wantRestarting {
				t.Errorf("RestartRequested closed = %v, want %v", restarting, tc.wantRestarting)
			}
			if status2, err2 := r.Reload(); status2 != tc.wantSecond || (err2 != nil) != tc.wantErr {
				t.Errorf("second reload status = %q, err = %v; want status %q, wantErr %v", status2, err2, tc.wantSecond, tc.wantErr)
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
		{name: "pod network rejected", method: http.MethodPost, remoteAddr: "192.0.2.9:5555", nextCfg: &testCfg{Port: "8080", Model: "b"}, wantCode: http.StatusForbidden, wantPort: "8080"},
		{name: "get not allowed", method: http.MethodGet, remoteAddr: "127.0.0.1:5555", nextCfg: &testCfg{Port: "8080", Model: "b"}, wantCode: http.StatusMethodNotAllowed, wantPort: "8080"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := NewReloadable(&testCfg{Port: "8080", Model: "a"}, fixtureLoader(tc.nextCfg, tc.loadErr), portChanged)
			req := httptest.NewRequest(tc.method, "/-/reload", http.NoBody)
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

func TestLoopbackOnly(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		remoteAddr string
		wantCode   int
		wantBody   string
	}{
		{name: "ipv4 loopback passes", remoteAddr: "127.0.0.1:5555", wantCode: http.StatusTeapot, wantBody: "inner"},
		{name: "ipv4 loopback range passes", remoteAddr: "127.0.0.2:5555", wantCode: http.StatusTeapot, wantBody: "inner"},
		{name: "ipv6 loopback passes", remoteAddr: "[::1]:5555", wantCode: http.StatusTeapot, wantBody: "inner"},
		{name: "pod network rejected", remoteAddr: "192.0.2.9:5555", wantCode: http.StatusForbidden, wantBody: "forbidden\n"},
		{name: "ipv6 non-loopback rejected", remoteAddr: "[fd00::1]:5555", wantCode: http.StatusForbidden, wantBody: "forbidden\n"},
		{name: "unparseable remote rejected", remoteAddr: "garbage", wantCode: http.StatusForbidden, wantBody: "forbidden\n"},
		{name: "hostname remote rejected", remoteAddr: "localhost:5555", wantCode: http.StatusForbidden, wantBody: "forbidden\n"},
		{name: "empty remote rejected", remoteAddr: "", wantCode: http.StatusForbidden, wantBody: "forbidden\n"},
	}

	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write([]byte("inner"))
	})

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(http.MethodPost, "/-/reload", http.NoBody)
			req.RemoteAddr = tc.remoteAddr
			rec := httptest.NewRecorder()

			LoopbackOnly(inner).ServeHTTP(rec, req)

			if rec.Code != tc.wantCode {
				t.Errorf("code = %d, want %d", rec.Code, tc.wantCode)
			}
			if got := rec.Body.String(); got != tc.wantBody {
				t.Errorf("body = %q, want %q", got, tc.wantBody)
			}
		})
	}
}
