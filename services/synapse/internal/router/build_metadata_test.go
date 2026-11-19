package router

import (
	"context"
	"testing"
)

func TestMetadataCheckRouting(t *testing.T) {
	t.Parallel()
	tests := []struct{ name, conclusion string }{{"build-metadata", "failure"}, {"build-metadata-result", "cancelled"}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := &RouterServer{}
			body := []byte(`{"action":"completed","repository":{"full_name":"owner/repo"},"check_run":{"name":"` + tc.name + `","status":"completed","conclusion":"` + tc.conclusion + `","head_sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","pull_requests":[{"number":1}]}}`)
			res, err := s.ProcessGitHubEvent(context.Background(), "check_run", "delivery", body)
			if err != nil {
				t.Fatal(err)
			}
			if res == nil {
				t.Fatal("expected non-nil resolved event")
			}
			if res.PRNumber != 0 {
				t.Fatal("transport check entered PR lifecycle routing")
			}
		})
	}
}
