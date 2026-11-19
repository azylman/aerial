package kube

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/azylman/aerial/services/synapse/internal/model"
)

const (
	testStoppedParentJSON = `{"metadata":{"name":"worker","namespace":"work","uid":"parent","generation":3},"spec":{"replicas":0,"selector":{"matchLabels":{"app":"worker","role":"writer"},"matchExpressions":[{"key":"track","operator":"NotIn","values":["other"]}]}},"status":{"observedGeneration":3,"replicas":2,"readyReplicas":2,"availableReplicas":2}}`
	testStoppedRSJSON     = `{"metadata":{"name":"worker-old","namespace":"work","uid":"child","generation":2,"ownerReferences":[{"apiVersion":"restate.dev/v1beta1","kind":"RestateDeployment","name":"worker","uid":"parent","controller":true}]},"spec":{"replicas":0},"status":{"observedGeneration":2}}`
)

type mockKubeServer struct {
	mu     sync.Mutex
	routes map[string]struct {
		status int
		body   string
	}
}

func newMockKubeServer(routes map[string]struct {
	status int
	body   string
},
) *httptest.Server {
	m := &mockKubeServer{routes: routes}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		key := r.Method + " " + r.URL.Path
		resp, ok := m.routes[key]
		m.mu.Unlock()
		if !ok {
			resp = struct {
				status int
				body   string
			}{status: http.StatusNotFound, body: `{"kind":"Status","reason":"NotFound"}`}
		}
		status := resp.status
		if status == 0 {
			status = http.StatusOK
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(resp.body))
	}))
}

func TestStoppedWorkloadEvidence(t *testing.T) {
	t.Parallel()

	emptyList := `{"items":[]}`

	tests := []struct {
		name, rs, pods, parent, initial string
		status, podStatus, parentStatus int
		wantDone, wantErr               bool
	}{
		{name: "stale parent counters proved stopped", rs: `{"items":[` + testStoppedRSJSON + `]}`, pods: emptyList, wantDone: true},
		{name: "empty inventories", rs: emptyList, pods: emptyList, wantDone: true},
		{name: "active RS", rs: `{"items":[` + strings.Replace(testStoppedRSJSON, `"replicas":0`, `"replicas":1`, 1) + `]}`, pods: emptyList},
		{name: "stale RS", rs: `{"items":[` + strings.Replace(testStoppedRSJSON, `"observedGeneration":2`, `"observedGeneration":1`, 1) + `]}`, pods: emptyList},
		{name: "residual RS counter", rs: `{"items":[` + strings.Replace(testStoppedRSJSON, `"observedGeneration":2`, `"observedGeneration":2,"replicas":1`, 1) + `]}`, pods: emptyList},
		{name: "malformed owner", rs: `{"items":[` + strings.Replace(testStoppedRSJSON, `"uid":"parent"`, `"uid":""`, 1) + `]}`, pods: emptyList},
		{name: "foreign UID does not prove ownership", rs: `{"items":[` + strings.Replace(testStoppedRSJSON, `"uid":"parent"`, `"uid":"foreign"`, 1) + `]}`, pods: emptyList},
		{name: "owned Pod", rs: `{"items":[` + testStoppedRSJSON + `]}`, pods: `{"items":[{"metadata":{"namespace":"work","ownerReferences":[{"kind":"ReplicaSet","name":"worker-old","uid":"child","controller":true}]}}]}`},
		{name: "terminating orphan full selector", rs: emptyList, pods: `{"items":[{"metadata":{"namespace":"work","labels":{"app":"worker","role":"writer"},"deletionTimestamp":"2026-01-01T00:00:00Z"}}]}`},
		{name: "partial selector unrelated", rs: emptyList, pods: `{"items":[{"metadata":{"namespace":"work","labels":{"app":"worker"}}}]}`, wantDone: true},
		{name: "expression excludes unrelated", rs: emptyList, pods: `{"items":[{"metadata":{"namespace":"work","labels":{"app":"worker","role":"writer","track":"other"}}}]}`, wantDone: true},
		{name: "pagination blocked", rs: `{"metadata":{"continue":"next"},"items":[]}`, pods: emptyList, wantErr: true},
		{name: "malformed JSON", rs: `{`, pods: emptyList, wantErr: true},
		{name: "missing inventory", rs: `{}`, pods: emptyList, wantErr: true},
		{name: "forbidden", status: http.StatusForbidden, rs: emptyList, pods: emptyList, wantErr: true},
		{name: "Pod list forbidden", rs: emptyList, pods: `forbidden`, podStatus: http.StatusForbidden, wantErr: true},
		{name: "Pod pagination", rs: emptyList, pods: `{"metadata":{"continue":"more"},"items":[]}`, wantErr: true},
		{name: "Pod malformed", rs: emptyList, pods: `{`, wantErr: true},
		{name: "parent forbidden", rs: emptyList, pods: emptyList, parentStatus: http.StatusForbidden, wantErr: true},
		{name: "parent malformed", rs: emptyList, pods: emptyList, parent: `{`, wantErr: true},
		{name: "missing selector", rs: emptyList, pods: emptyList, initial: strings.Replace(testStoppedParentJSON, `"matchLabels":{"app":"worker","role":"writer"},"matchExpressions":[{"key":"track","operator":"NotIn","values":["other"]}]`, ``, 1)},
		{name: "missing RS desired defaults active", rs: `{"items":[` + strings.Replace(testStoppedRSJSON, `"replicas":0`, ``, 1) + `]}`, pods: emptyList},
		{name: "non controller owner", rs: `{"items":[` + strings.Replace(testStoppedRSJSON, `"controller":true`, `"controller":false`, 1) + `]}`, pods: emptyList},
		{name: "wrong namespace child", rs: `{"items":[` + strings.Replace(testStoppedRSJSON, `"namespace":"work"`, `"namespace":"other"`, 1) + `]}`, pods: emptyList},
		{name: "extra controller", rs: `{"items":[` + strings.Replace(testStoppedRSJSON, `"controller":true}]`, `"controller":true},{"kind":"Other","controller":true}]`, 1) + `]}`, pods: emptyList},
		{name: "parent stale observed", rs: emptyList, pods: emptyList, parent: strings.Replace(testStoppedParentJSON, `"observedGeneration":3`, `"observedGeneration":2`, 1), wantErr: true},
		{name: "parent UID changed", rs: emptyList, pods: emptyList, parent: strings.Replace(testStoppedParentJSON, `"uid":"parent"`, `"uid":"replacement"`, 1), wantErr: true},
		{name: "parent generation changed", rs: emptyList, pods: emptyList, parent: strings.Replace(testStoppedParentJSON, `"generation":3`, `"generation":4`, 1), wantErr: true},
		{name: "parent reactivated", rs: emptyList, pods: emptyList, parent: strings.Replace(testStoppedParentJSON, `"replicas":0`, `"replicas":1`, 1), wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			initial := tc.initial
			if initial == "" {
				initial = testStoppedParentJSON
			}
			parent := tc.parent
			if parent == "" {
				parent = initial
			}
			routes := map[string]struct {
				status int
				body   string
			}{
				"GET /apis/apps/v1/deployments":                                           {body: emptyList},
				"GET /apis/apps/v1/statefulsets":                                          {body: emptyList},
				"GET /apis/apps/v1/daemonsets":                                            {body: emptyList},
				"GET /apis/restate.dev/v1beta1/restatedeployments":                        {body: `{"items":[` + initial + `]}`},
				"GET /apis/restate.dev/v1/restateclusters":                                {body: emptyList},
				"GET /apis/apps/v1/namespaces/work/replicasets":                           {status: tc.status, body: tc.rs},
				"GET /api/v1/namespaces/work/pods":                                        {status: tc.podStatus, body: tc.pods},
				"GET /apis/restate.dev/v1beta1/namespaces/work/restatedeployments/worker": {status: tc.parentStatus, body: parent},
			}
			ts := newMockKubeServer(routes)
			defer ts.Close()

			c := NewClient(ts.Client())
			c.SetKubeConfig(ts.URL, "")
			ws, err := c.ListKustomizationWorkloads(context.Background(), model.FluxKustomizationRef{Name: "core", Namespace: "flux-system"})
			if (err != nil) != tc.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tc.wantErr)
			}
			if err != nil {
				return
			}
			if len(ws) != 1 {
				t.Fatalf("workloads = %v", ws)
			}
			done, failed, _ := StoppedRestateDeploymentComplete(ws[0])
			if done != tc.wantDone || failed {
				t.Fatalf("done=%v failed=%v, want done %v", done, failed, tc.wantDone)
			}
		})
	}
}

func TestMatchesSelector(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, selector       string
		labels               map[string]string
		wantMatch, wantValid bool
	}{
		{"nil", `null`, nil, false, false},
		{"empty", `{}`, nil, false, false},
		{"empty label key", `{"matchLabels":{"":"x"}}`, nil, false, false},
		{"present empty label", `{"matchLabels":{"a":""}}`, map[string]string{"a": ""}, true, true},
		{"absent empty label", `{"matchLabels":{"a":""}}`, nil, false, true},
		{"in", `{"matchExpressions":[{"key":"a","operator":"In","values":["b"]}]}`, map[string]string{"a": "b"}, true, true},
		{"in missing", `{"matchExpressions":[{"key":"a","operator":"In","values":["b"]}]}`, nil, false, true},
		{"not in absent", `{"matchExpressions":[{"key":"a","operator":"NotIn","values":["b"]}]}`, nil, true, true},
		{"exists", `{"matchExpressions":[{"key":"a","operator":"Exists"}]}`, map[string]string{"a": ""}, true, true},
		{"exists absent", `{"matchExpressions":[{"key":"a","operator":"Exists"}]}`, nil, false, true},
		{"does not exist", `{"matchExpressions":[{"key":"a","operator":"DoesNotExist"}]}`, nil, true, true},
		{"does not exist present", `{"matchExpressions":[{"key":"a","operator":"DoesNotExist"}]}`, map[string]string{"a": ""}, false, true},
		{"unknown operator", `{"matchExpressions":[{"key":"a","operator":"Invalid"}]}`, nil, false, false},
		{"empty expression key", `{"matchExpressions":[{"key":"","operator":"Exists"}]}`, nil, false, false},
		{"in no values", `{"matchExpressions":[{"key":"a","operator":"In"}]}`, nil, false, false},
		{"exists with values", `{"matchExpressions":[{"key":"a","operator":"Exists","values":["b"]}]}`, nil, false, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var selector *model.KubeLabelSelector
			if err := json.Unmarshal([]byte(tc.selector), &selector); err != nil {
				t.Fatal(err)
			}
			match, valid := MatchesSelector(selector, tc.labels)
			if match != tc.wantMatch || valid != tc.wantValid {
				t.Fatalf("match=%v valid=%v", match, valid)
			}
		})
	}
}

func TestStoppedPodRelevant(t *testing.T) {
	t.Parallel()

	parent := model.WorkloadRollout{
		UID:      "parent-uid",
		Name:     "parent-svc",
		Selector: &model.KubeLabelSelector{MatchLabels: map[string]string{"app": "svc"}},
	}
	tests := []struct {
		name  string
		meta  model.KubeObjectMeta
		owned map[string]string
		want  bool
	}{
		{
			name: "matches label selector",
			meta: model.KubeObjectMeta{Labels: map[string]string{"app": "svc"}},
			want: true,
		},
		{
			name:  "matches owned uid",
			meta:  model.KubeObjectMeta{OwnerReferences: []model.KubeOwnerReference{{UID: "rs-1"}}},
			owned: map[string]string{"rs-1": "rs-name"},
			want:  true,
		},
		{
			name: "matches parent uid",
			meta: model.KubeObjectMeta{OwnerReferences: []model.KubeOwnerReference{{UID: "parent-uid"}}},
			want: true,
		},
		{
			name: "matches restate deployment name",
			meta: model.KubeObjectMeta{OwnerReferences: []model.KubeOwnerReference{{Kind: kindRestateDeployment, Name: "parent-svc"}}},
			want: true,
		},
		{
			name:  "matches owned rs name",
			meta:  model.KubeObjectMeta{OwnerReferences: []model.KubeOwnerReference{{Kind: "ReplicaSet", Name: "rs-name"}}},
			owned: map[string]string{"rs-1": "rs-name"},
			want:  true,
		},
		{
			name: "unrelated pod",
			meta: model.KubeObjectMeta{Labels: map[string]string{"app": "other"}},
			want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := StoppedPodRelevant(parent, tc.meta, tc.owned)
			if got != tc.want {
				t.Fatalf("StoppedPodRelevant() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestStoppedChildrenComplete_MissingChildrenAndErrors(t *testing.T) {
	t.Parallel()

	// nil StoppedChildren
	if StoppedChildrenComplete(model.WorkloadRollout{UID: "u1", Namespace: "n", Name: "w", Generation: 1}) {
		t.Errorf("expected false for nil StoppedChildren")
	}

	// invalid selector
	invalidSel := model.WorkloadRollout{
		UID:        "u1",
		Namespace:  "n",
		Name:       "w",
		Generation: 1,
		Selector: &model.KubeLabelSelector{MatchExpressions: []struct {
			Key      string   `json:"key"`
			Operator string   `json:"operator"`
			Values   []string `json:"values"`
		}{{Key: "k", Operator: "Invalid"}}},
		StoppedChildren: &model.StoppedChildObservation{},
	}
	if StoppedChildrenComplete(invalidSel) {
		t.Errorf("expected false for invalid selector")
	}

	// duplicate child UID
	zero := int32(0)
	rs := model.StoppedReplicaSet{
		Metadata: model.KubeObjectMeta{
			Name: "rs1", UID: "uid1", Namespace: "n", Generation: 1,
			OwnerReferences: []model.KubeOwnerReference{{Kind: "RestateDeployment", Name: "w", UID: "u1", APIVersion: "restate.dev/v1beta1", Controller: true}},
		},
		Spec: struct {
			Replicas *int32 `json:"replicas"`
		}{Replicas: &zero},
		Status: struct {
			ObservedGeneration   int64 `json:"observedGeneration"`
			Replicas             int32 `json:"replicas"`
			ReadyReplicas        int32 `json:"readyReplicas"`
			AvailableReplicas    int32 `json:"availableReplicas"`
			FullyLabeledReplicas int32 `json:"fullyLabeledReplicas"`
		}{ObservedGeneration: 1},
	}
	dupChild := model.WorkloadRollout{
		UID:             "u1",
		Namespace:       "n",
		Name:            "w",
		Generation:      1,
		Selector:        &model.KubeLabelSelector{MatchLabels: map[string]string{"app": "test"}},
		StoppedChildren: &model.StoppedChildObservation{ReplicaSets: []model.StoppedReplicaSet{rs, rs}},
	}
	if StoppedChildrenComplete(dupChild) {
		t.Errorf("expected false for duplicate child UID")
	}
}
