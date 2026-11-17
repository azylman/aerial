package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

const (
	stoppedParentJSON = `{"metadata":{"name":"worker","namespace":"work","uid":"parent","generation":3},"spec":{"replicas":0,"selector":{"matchLabels":{"app":"worker","role":"writer"},"matchExpressions":[{"key":"track","operator":"NotIn","values":["other"]}]}},"status":{"observedGeneration":3,"replicas":2,"readyReplicas":2,"availableReplicas":2}}`
	stoppedRSJSON     = `{"metadata":{"name":"worker-old","namespace":"work","uid":"child","generation":2,"ownerReferences":[{"apiVersion":"restate.dev/v1beta1","kind":"RestateDeployment","name":"worker","uid":"parent","controller":true}]},"spec":{"replicas":0},"status":{"observedGeneration":2}}`
)

func TestStoppedWorkloadEvidence(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, rs, pods, parent, initial string
		status, podStatus, parentStatus int
		wantDone, wantErr               bool
	}{
		{name: "stale parent counters proved stopped", rs: `{"items":[` + stoppedRSJSON + `]}`, pods: emptyKubeList, wantDone: true},
		{name: "empty inventories", rs: emptyKubeList, pods: emptyKubeList, wantDone: true},
		{name: "active RS", rs: `{"items":[` + strings.Replace(stoppedRSJSON, `"replicas":0`, `"replicas":1`, 1) + `]}`, pods: emptyKubeList},
		{name: "stale RS", rs: `{"items":[` + strings.Replace(stoppedRSJSON, `"observedGeneration":2`, `"observedGeneration":1`, 1) + `]}`, pods: emptyKubeList},
		{name: "residual RS counter", rs: `{"items":[` + strings.Replace(stoppedRSJSON, `"observedGeneration":2`, `"observedGeneration":2,"replicas":1`, 1) + `]}`, pods: emptyKubeList},
		{name: "malformed owner", rs: `{"items":[` + strings.Replace(stoppedRSJSON, `"uid":"parent"`, `"uid":""`, 1) + `]}`, pods: emptyKubeList},
		{name: "foreign UID does not prove ownership", rs: `{"items":[` + strings.Replace(stoppedRSJSON, `"uid":"parent"`, `"uid":"foreign"`, 1) + `]}`, pods: emptyKubeList},
		{name: "owned Pod", rs: `{"items":[` + stoppedRSJSON + `]}`, pods: `{"items":[{"metadata":{"namespace":"work","ownerReferences":[{"kind":"ReplicaSet","name":"worker-old","uid":"child","controller":true}]}}]}`},
		{name: "terminating orphan full selector", rs: emptyKubeList, pods: `{"items":[{"metadata":{"namespace":"work","labels":{"app":"worker","role":"writer"},"deletionTimestamp":"2026-01-01T00:00:00Z"}}]}`},
		{name: "partial selector unrelated", rs: emptyKubeList, pods: `{"items":[{"metadata":{"namespace":"work","labels":{"app":"worker"}}}]}`, wantDone: true},
		{name: "expression excludes unrelated", rs: emptyKubeList, pods: `{"items":[{"metadata":{"namespace":"work","labels":{"app":"worker","role":"writer","track":"other"}}}]}`, wantDone: true},
		{name: "pagination blocked", rs: `{"metadata":{"continue":"next"},"items":[]}`, pods: emptyKubeList, wantErr: true},
		{name: "malformed JSON", rs: `{`, pods: emptyKubeList, wantErr: true},
		{name: "missing inventory", rs: `{}`, pods: emptyKubeList, wantErr: true},
		{name: "forbidden", status: http.StatusForbidden, rs: emptyKubeList, pods: emptyKubeList, wantErr: true},
		{name: "Pod list forbidden", rs: emptyKubeList, pods: `forbidden`, podStatus: http.StatusForbidden, wantErr: true},
		{name: "Pod pagination", rs: emptyKubeList, pods: `{"metadata":{"continue":"more"},"items":[]}`, wantErr: true},
		{name: "Pod malformed", rs: emptyKubeList, pods: `{`, wantErr: true},
		{name: "parent forbidden", rs: emptyKubeList, pods: emptyKubeList, parentStatus: http.StatusForbidden, wantErr: true},
		{name: "parent malformed", rs: emptyKubeList, pods: emptyKubeList, parent: `{`, wantErr: true},
		{name: "missing selector", rs: emptyKubeList, pods: emptyKubeList, initial: strings.Replace(stoppedParentJSON, `"matchLabels":{"app":"worker","role":"writer"},"matchExpressions":[{"key":"track","operator":"NotIn","values":["other"]}]`, ``, 1)},
		{name: "missing RS desired defaults active", rs: `{"items":[` + strings.Replace(stoppedRSJSON, `"replicas":0`, ``, 1) + `]}`, pods: emptyKubeList},
		{name: "non controller owner", rs: `{"items":[` + strings.Replace(stoppedRSJSON, `"controller":true`, `"controller":false`, 1) + `]}`, pods: emptyKubeList},
		{name: "wrong namespace child", rs: `{"items":[` + strings.Replace(stoppedRSJSON, `"namespace":"work"`, `"namespace":"other"`, 1) + `]}`, pods: emptyKubeList},
		{name: "extra controller", rs: `{"items":[` + strings.Replace(stoppedRSJSON, `"controller":true}]`, `"controller":true},{"kind":"Other","controller":true}]`, 1) + `]}`, pods: emptyKubeList},
		{name: "parent stale observed", rs: emptyKubeList, pods: emptyKubeList, parent: strings.Replace(stoppedParentJSON, `"observedGeneration":3`, `"observedGeneration":2`, 1), wantErr: true},
		{name: "parent UID changed", rs: emptyKubeList, pods: emptyKubeList, parent: strings.Replace(stoppedParentJSON, `"uid":"parent"`, `"uid":"replacement"`, 1), wantErr: true},
		{name: "parent generation changed", rs: emptyKubeList, pods: emptyKubeList, parent: strings.Replace(stoppedParentJSON, `"generation":3`, `"generation":4`, 1), wantErr: true},
		{name: "parent reactivated", rs: emptyKubeList, pods: emptyKubeList, parent: strings.Replace(stoppedParentJSON, `"replicas":0`, `"replicas":1`, 1), wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			initial := tc.initial
			if initial == "" {
				initial = stoppedParentJSON
			}
			parent := tc.parent
			if parent == "" {
				parent = initial
			}
			_, ts := newKubeFixture(t, kubeWorkloadRoutes(map[string]kubeRoute{
				kubeRestateDeploymentsPath:                                                {body: `{"items":[` + initial + `]}`},
				"GET /apis/apps/v1/namespaces/work/replicasets":                           {status: tc.status, body: tc.rs},
				"GET /api/v1/namespaces/work/pods":                                        {status: tc.podStatus, body: tc.pods},
				"GET /apis/restate.dev/v1beta1/namespaces/work/restatedeployments/worker": {status: tc.parentStatus, body: parent},
			}))
			d := NewDefaultOutboundDispatcher("", ts.Client())
			d.SetKubeConfig(ts.URL, "")
			ws, err := d.ListKustomizationWorkloads(context.Background(), fluxKustomizationRef{Name: "core", Namespace: "flux-system"})
			if (err != nil) != tc.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tc.wantErr)
			}
			if err != nil {
				return
			}
			if len(ws) != 1 {
				t.Fatalf("workloads = %v", ws)
			}
			done, failed, _ := workloadRolloutComplete(ws[0])
			if done != tc.wantDone || failed {
				t.Fatalf("done=%v failed=%v, want done %v", done, failed, tc.wantDone)
			}
		})
	}
}

func TestKubeLabelSelectorMatches(t *testing.T) {
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
			var selector *kubeLabelSelector
			if err := json.Unmarshal([]byte(tc.selector), &selector); err != nil {
				t.Fatal(err)
			}
			match, valid := selector.matches(tc.labels)
			if match != tc.wantMatch || valid != tc.wantValid {
				t.Fatalf("match=%v valid=%v", match, valid)
			}
		})
	}
}
