package kube

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"

	"github.com/azylman/aerial/services/synapse/internal/model"
)

const kindRestateDeployment = "RestateDeployment"

// MatchesSelector evaluates the complete label selector against labels; invalid or empty selectors return false, false.
func MatchesSelector(s *model.KubeLabelSelector, labels map[string]string) (bool, bool) {
	if s == nil || len(s.MatchLabels)+len(s.MatchExpressions) == 0 {
		return false, false
	}
	matched := true
	for k, v := range s.MatchLabels {
		if k == "" {
			return false, false
		}
		actual, ok := labels[k]
		matched = matched && ok && actual == v
	}
	for _, e := range s.MatchExpressions {
		match, valid := SelectorExpressionMatches(e.Key, e.Operator, e.Values, labels)
		if !valid {
			return false, false
		}
		matched = matched && match
	}
	return matched, true
}

// SelectorExpressionMatches evaluates one matchExpression against labels.
func SelectorExpressionMatches(key, operator string, values []string, labels map[string]string) (bool, bool) {
	if key == "" {
		return false, false
	}
	value, exists := labels[key]
	switch operator {
	case "In":
		return exists && slices.Contains(values, value), len(values) > 0
	case "NotIn":
		return !exists || !slices.Contains(values, value), len(values) > 0
	case "Exists":
		return exists, len(values) == 0
	case "DoesNotExist":
		return !exists, len(values) == 0
	default:
		return false, false
	}
}

// CollectStoppedChildren queries replica sets, pods, and parent workload to verify stopped state.
func (c *Client) CollectStoppedChildren(ctx context.Context, w model.WorkloadRollout) (*model.StoppedChildObservation, error) {
	observation := &model.StoppedChildObservation{}
	if err := c.StoppedInventory(ctx, "/apis/apps/v1/namespaces/"+url.PathEscape(w.Namespace)+"/replicasets", &observation.ReplicaSets); err != nil {
		return nil, err
	}
	if err := c.StoppedInventory(ctx, "/api/v1/namespaces/"+url.PathEscape(w.Namespace)+"/pods", &observation.Pods); err != nil {
		return nil, err
	}
	body, err := c.KubeJSON(ctx, http.MethodGet, "/apis/restate.dev/v1beta1/namespaces/"+url.PathEscape(w.Namespace)+"/restatedeployments/"+url.PathEscape(w.Name), "", nil)
	if err != nil {
		return nil, err
	}
	fresh, err := DecodeWorkloadRollouts(kindRestateDeployment, append(append([]byte(`{"items":[`), body...), []byte(`]}`)...))
	if err != nil {
		return nil, err
	}
	if len(fresh) != 1 || fresh[0].UID == "" || fresh[0].Namespace != w.Namespace || fresh[0].Name != w.Name || fresh[0].Generation <= 0 || fresh[0].UID != w.UID || fresh[0].Generation != w.Generation || fresh[0].Replicas != w.Replicas || fresh[0].ObservedGeneration < w.Generation {
		return nil, fmt.Errorf("stopped parent changed or unobserved: %s/%s", w.Namespace, w.Name)
	}
	return observation, nil
}

// StoppedInventory reads and unmarshals an inventory of resources, failing on pagination continue tokens.
func (c *Client) StoppedInventory(ctx context.Context, path string, items any) error {
	body, err := c.KubeJSON(ctx, http.MethodGet, path, "", nil)
	if err != nil {
		return err
	}
	var list struct {
		Metadata struct {
			Continue string `json:"continue"`
		} `json:"metadata"`
		Items json.RawMessage `json:"items"`
	}
	if err := json.Unmarshal(body, &list); err != nil {
		return err
	}
	if list.Metadata.Continue != "" || len(list.Items) == 0 || string(list.Items) == "null" {
		return fmt.Errorf("incomplete stopped inventory: %s", path)
	}
	return json.Unmarshal(list.Items, items)
}

// StoppedChildrenComplete checks whether all observed child replica sets and pods have cleanly terminated.
func StoppedChildrenComplete(w model.WorkloadRollout) bool {
	if w.StoppedChildren == nil || w.UID == "" || w.Namespace == "" || w.Name == "" || w.Generation <= 0 {
		return false
	}
	if _, valid := MatchesSelector(w.Selector, nil); !valid {
		return false
	}
	owned := map[string]string{}
	for _, rs := range w.StoppedChildren.ReplicaSets {
		relevant, _ := MatchesSelector(w.Selector, rs.Metadata.Labels)
		ownedByParent, ownerValid := StoppedReplicaSetOwner(w, rs.Metadata)
		relevant = relevant || ownedByParent
		if !ownerValid {
			return false
		}
		if !relevant {
			continue
		}
		if !ownedByParent || !StoppedReplicaSetComplete(w, rs) || owned[rs.Metadata.UID] != "" {
			return false
		}
		owned[rs.Metadata.UID] = rs.Metadata.Name
	}
	for _, p := range w.StoppedChildren.Pods {
		if StoppedPodRelevant(w, p.Metadata, owned) {
			return false
		}
	}
	return true
}

// StoppedReplicaSetOwner determines whether the replica set is owned by the parent workload rollout.
func StoppedReplicaSetOwner(w model.WorkloadRollout, m model.KubeObjectMeta) (bool, bool) {
	relevant := false
	controllerCount := 0
	for _, o := range m.OwnerReferences {
		if o.Controller {
			controllerCount++
		}
		if o.UID == w.UID || (o.Kind == kindRestateDeployment && o.Name == w.Name) {
			if relevant || !o.Controller || o.Kind != kindRestateDeployment || o.Name != w.Name || o.UID != w.UID || o.APIVersion != "restate.dev/v1beta1" {
				return true, false
			}
			relevant = true
		}
	}
	return relevant, !relevant || controllerCount == 1
}

// StoppedReplicaSetComplete asserts that a child replica set has reached 0 desired and observed replicas.
func StoppedReplicaSetComplete(w model.WorkloadRollout, rs model.StoppedReplicaSet) bool {
	return rs.Metadata.Name != "" && rs.Metadata.UID != "" && rs.Metadata.Namespace == w.Namespace && rs.Metadata.Generation > 0 && rs.Status.ObservedGeneration >= rs.Metadata.Generation && rs.Spec.Replicas != nil && *rs.Spec.Replicas == 0 && rs.Status.Replicas == 0 && rs.Status.ReadyReplicas == 0 && rs.Status.AvailableReplicas == 0 && rs.Status.FullyLabeledReplicas == 0
}

// StoppedPodRelevant reports whether a pod matches the parent workload or its owned replica sets.
func StoppedPodRelevant(w model.WorkloadRollout, m model.KubeObjectMeta, owned map[string]string) bool {
	matches, _ := MatchesSelector(w.Selector, m.Labels)
	if matches {
		return true
	}
	for _, o := range m.OwnerReferences {
		if _, ok := owned[o.UID]; ok {
			return true
		}
		if o.UID == w.UID || (o.Kind == kindRestateDeployment && o.Name == w.Name) {
			return true
		}
		for _, name := range owned {
			if o.Kind == "ReplicaSet" && o.Name == name {
				return true
			}
		}
	}
	return false
}

// StoppedRestateDeploymentComplete reports whether a zero-replica RestateDeployment has completely stopped.
func StoppedRestateDeploymentComplete(w model.WorkloadRollout) (bool, bool, string) {
	if StoppedChildrenComplete(w) {
		return true, false, ""
	}
	return false, false, "waiting for stopped child evidence"
}
