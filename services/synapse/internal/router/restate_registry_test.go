package router

import (
	"maps"
	"testing"
)

func TestRegisterMetadataStrings(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		raw  string
		want map[string]string
	}{
		{name: "string keys forward trimmed", raw: `{"build_ref":" ref-1 ","widget_state":"deployed"}`, want: map[string]string{"build_ref": "ref-1", "widget_state": "deployed"}},
		{name: "non-string and blank values drop", raw: `{"build_ref":7,"widget_state":{"a":1},"agent":" "}`},
		{name: "malformed json drops", raw: `{nope`},
		{name: "empty metadata drops", raw: ``},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := registerMetadataStrings(tc.raw); !maps.Equal(got, tc.want) {
				t.Fatalf("registerMetadataStrings() = %v, want %v", got, tc.want)
			}
		})
	}
}
