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

type reloadableTestCase struct {
	name           string
	nextCfg        *testCfg
	loadErr        error
	restartCheck   bool
	wantStatus     ReloadStatus
	wantErr        bool
	wantCurrent    testCfg
	wantRestarting bool
}

func runReloadableReloadCase(t *testing.T, tc reloadableTestCase, initial *testCfg) {
	t.Helper()
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
	assertRestartRequested(t, r, tc.wantRestarting)
	assertSecondReload(t, r, tc.wantStatus)
}

func assertRestartRequested(t *testing.T, r *Reloadable[testCfg], wantRestarting bool) {
	t.Helper()
	select {
	case <-r.RestartRequested():
		if !wantRestarting {
			t.Error("RestartRequested closed unexpectedly")
		}
	default:
		if wantRestarting {
			t.Error("RestartRequested not closed")
		}
	}
}

func assertSecondReload(t *testing.T, r *Reloadable[testCfg], wantStatus ReloadStatus) {
	t.Helper()
	status2, _ := r.Reload()
	wantSecond := wantStatus
	if wantStatus == ReloadApplied {
		wantSecond = ReloadUnchanged
	}
	if status2 != wantSecond {
		t.Errorf("second reload status = %q, want %q", status2, wantSecond)
	}
}

func TestReloadable_Reload(t *testing.T) {
	t.Parallel()

	initial := &testCfg{Port: "8080", Model: "a", Tags: []string{"x"}}
	tests := []reloadableTestCase{
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
			runReloadableReloadCase(t, tc, initial)
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
		{name: "pod network rejected", remoteAddr: "10.42.0.9:5555", wantCode: http.StatusForbidden, wantBody: "forbidden\n"},
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
			req := httptest.NewRequest(http.MethodPost, "/-/reload", nil)
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
