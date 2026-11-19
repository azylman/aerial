package prstate

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
)

func wireKeys(t *testing.T, v any) string {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("json.Marshal() = %v, want nil", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("json.Unmarshal() = %v, want nil", err)
	}
	keys := make([]string, 0, len(decoded))
	for k := range decoded {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return strings.Join(keys, ",")
}

// TestContractWireFormat pins the snake_case wire format the synapse
// ingress invoker emits. Both sides assert the identical key set, so drift
// on either side fails loudly before consumer cutover.
func TestContractWireFormat(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		got  string
		want string
	}{
		{name: "pr event wire keys", got: wireKeys(t, PREvent{}), want: `branch,build_id,check_name,event,gate_generation,head_sha,job_name,merge_sha,pr_number,reopen_epoch,repo,title,unmanaged`},
		{name: "repo event wire keys", got: wireKeys(t, RepoEvent{}), want: `generation,head_sha,main_sha,pr_number,repo`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.got != tc.want {
				t.Errorf("wire keys = %q, want %q", tc.got, tc.want)
			}
		})
	}
}
