package githubapitest_test

import (
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/azylman/aerial/pkg/githubapitest"
)

type reply struct {
	Status     int
	Body       string
	Remaining  string
	RetryAfter string
}

func TestServer(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		routes map[string][]githubapitest.Response
		method string
		path   string
		calls  int
		want   []reply
	}{
		{name: "unknown route is 404", method: "GET", path: "/repos/o/r", calls: 1, want: []reply{{Status: 404, Body: `{"message":"Not Found"}`}}},
		{
			name: "query is part of the route", routes: map[string][]githubapitest.Response{"GET /repos/o/r/pulls?state=open": {{Body: "[]"}}}, method: "GET", path: "/repos/o/r/pulls?state=open", calls: 1,
			want: []reply{{Status: 200, Body: "[]"}},
		},
		{
			name: "queue advances and last repeats", routes: map[string][]githubapitest.Response{"PUT /repos/o/r/pulls/7/update-branch": {{Status: 403, Remaining: "0", RetryAfter: "60"}, {Status: 202, Body: "ok"}}},
			method: "PUT", path: "/repos/o/r/pulls/7/update-branch", calls: 3, want: []reply{{Status: 403, Remaining: "0", RetryAfter: "60"}, {Status: 202, Body: "ok"}, {Status: 202, Body: "ok"}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			api := githubapitest.New(t, tc.routes)
			var got []reply
			for range tc.calls {
				got = append(got, doRequest(t, api, tc.method, tc.path))
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("replies = %+v, want %+v", got, tc.want)
			}
			if err := api.Err(); err != nil {
				t.Errorf("Err() = %v, want nil", err)
			}
			reqs := api.Requests()
			if len(reqs) != tc.calls {
				t.Fatalf("Requests() len = %d, want %d", len(reqs), tc.calls)
			}
			wantReq := githubapitest.Request{Method: tc.method, Path: tc.path, Auth: "Bearer tok", Accept: "application/vnd.github+json", ContentType: "application/json", Body: "{}"}
			if reqs[0] != wantReq {
				t.Errorf("Requests()[0] = %+v, want %+v", reqs[0], wantReq)
			}
		})
	}
}

func TestServerDoesNotMutateRoutes(t *testing.T) {
	t.Parallel()
	routes := map[string][]githubapitest.Response{"GET /x": {{Status: 500}, {Status: 200}}}
	api := githubapitest.New(t, routes)
	doRequest(t, api, "GET", "/x")
	if len(routes["GET /x"]) != 2 {
		t.Errorf("caller routes mutated: %+v", routes)
	}
}

func TestServerClose(t *testing.T) {
	t.Parallel()
	api := githubapitest.New(t, nil)
	url := api.URL()
	api.Close()
	resp, err := http.Get(url + "/x")
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("GET after Close succeeded, want connection error")
	}
}

func doRequest(t *testing.T, api *githubapitest.Server, method, path string) reply {
	t.Helper()
	req, err := http.NewRequest(method, api.URL()+path, strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Authorization", "Bearer tok")
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("Content-Type", "application/json")
	resp, err := api.Client().Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	return reply{Status: resp.StatusCode, Body: string(body), Remaining: resp.Header.Get("X-Ratelimit-Remaining"), RetryAfter: resp.Header.Get("Retry-After")}
}
