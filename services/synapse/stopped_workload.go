package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"
)

type kubeOwnerReference struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Name       string `json:"name"`
	UID        string `json:"uid"`
	Controller bool   `json:"controller"`
}

type kubeLabelSelector struct {
	MatchLabels      map[string]string `json:"matchLabels"`
	MatchExpressions []struct {
		Key      string   `json:"key"`
		Operator string   `json:"operator"`
		Values   []string `json:"values"`
	} `json:"matchExpressions"`
}

// matches evaluates the complete selector; invalid or empty selectors are unknown.
func (s *kubeLabelSelector) matches(labels map[string]string) (bool, bool) {
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
		match, valid := selectorExpressionMatches(e.Key, e.Operator, e.Values, labels)
		if !valid {
			return false, false
		}
		matched = matched && match
	}

	return matched, true
}

func selectorExpressionMatches(key, operator string, values []string, labels map[string]string) (bool, bool) {
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

type stoppedReplicaSet struct {
	Metadata kubeObjectMeta `json:"metadata"`
	Spec     struct {
		Replicas *int32 `json:"replicas"`
	} `json:"spec"`
	Status struct {
		ObservedGeneration   int64 `json:"observedGeneration"`
		Replicas             int32 `json:"replicas"`
		ReadyReplicas        int32 `json:"readyReplicas"`
		AvailableReplicas    int32 `json:"availableReplicas"`
		FullyLabeledReplicas int32 `json:"fullyLabeledReplicas"`
	} `json:"status"`
}

type stoppedPod struct {
	Metadata kubeObjectMeta `json:"metadata"`
}

// A non-nil observation proves both inventories were read completely and the parent
// was re-read afterwards. An empty successful inventory differs from unknown evidence.
type stoppedChildObservation struct {
	ReplicaSets []stoppedReplicaSet
	Pods        []stoppedPod
}

func (d *DefaultOutboundDispatcher) collectStoppedChildren(ctx context.Context, w workloadRollout) (*stoppedChildObservation, error) {
	observation := &stoppedChildObservation{}
	if err := d.stoppedInventory(ctx, "/apis/apps/v1/namespaces/"+url.PathEscape(w.Namespace)+"/replicasets", &observation.ReplicaSets); err != nil {
		return nil, err
	}
	if err := d.stoppedInventory(ctx, "/api/v1/namespaces/"+url.PathEscape(w.Namespace)+"/pods", &observation.Pods); err != nil {
		return nil, err
	}
	body, err := d.kubeJSON(ctx, http.MethodGet, "/apis/restate.dev/v1beta1/namespaces/"+url.PathEscape(w.Namespace)+"/restatedeployments/"+url.PathEscape(w.Name), "", nil)
	if err != nil {
		return nil, err
	}
	// Reuse the canonical desired-replica defaulting when decoding the fresh object.
	fresh, err := decodeWorkloadRollouts(kindRestateDeployment, append(append([]byte(`{"items":[`), body...), []byte(`]}`)...))
	if err != nil {
		return nil, err
	}
	if len(fresh) != 1 || fresh[0].UID == "" || fresh[0].Namespace != w.Namespace || fresh[0].Name != w.Name || fresh[0].Generation <= 0 || fresh[0].UID != w.UID || fresh[0].Generation != w.Generation || fresh[0].Replicas != w.Replicas || fresh[0].ObservedGeneration < w.Generation {
		return nil, fmt.Errorf("stopped parent changed or unobserved: %s", w)
	}
	return observation, nil
}

func (d *DefaultOutboundDispatcher) stoppedInventory(ctx context.Context, path string, items any) error {
	body, err := d.kubeJSON(ctx, http.MethodGet, path, "", nil)
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
	// Never accept a truncated inventory. Retry on a later watcher reconciliation.
	if list.Metadata.Continue != "" || len(list.Items) == 0 || string(list.Items) == "null" {
		return fmt.Errorf("incomplete stopped inventory: %s", path)
	}
	return json.Unmarshal(list.Items, items)
}

func stoppedChildrenComplete(w workloadRollout) bool {
	if w.StoppedChildren == nil || w.UID == "" || w.Namespace == "" || w.Name == "" || w.Generation <= 0 {
		return false
	}
	if _, valid := w.Selector.matches(nil); !valid {
		return false
	}
	owned := map[string]string{}
	for _, rs := range w.StoppedChildren.ReplicaSets {
		relevant, _ := w.Selector.matches(rs.Metadata.Labels)
		ownedByParent, ownerValid := stoppedReplicaSetOwner(w, rs.Metadata)
		relevant = relevant || ownedByParent
		if !ownerValid {
			return false
		}
		if !relevant {
			continue
		}
		if !ownedByParent || !stoppedReplicaSetComplete(w, rs) || owned[rs.Metadata.UID] != "" {
			return false
		}

		owned[rs.Metadata.UID] = rs.Metadata.Name
	}
	for _, p := range w.StoppedChildren.Pods {
		if stoppedPodRelevant(w, p.Metadata, owned) {
			return false
		}
	}
	return true
}

func stoppedReplicaSetOwner(w workloadRollout, m kubeObjectMeta) (bool, bool) {
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

func stoppedReplicaSetComplete(w workloadRollout, rs stoppedReplicaSet) bool {
	return rs.Metadata.Name != "" && rs.Metadata.UID != "" && rs.Metadata.Namespace == w.Namespace && rs.Metadata.Generation > 0 && rs.Status.ObservedGeneration >= rs.Metadata.Generation && rs.Spec.Replicas != nil && *rs.Spec.Replicas == 0 && rs.Status.Replicas == 0 && rs.Status.ReadyReplicas == 0 && rs.Status.AvailableReplicas == 0 && rs.Status.FullyLabeledReplicas == 0
}

func stoppedPodRelevant(w workloadRollout, m kubeObjectMeta, owned map[string]string) bool {
	matches, _ := w.Selector.matches(m.Labels)
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
		// Missing or incorrect UID must not hide a pod naming an owned ReplicaSet.
		for _, name := range owned {
			if o.Kind == "ReplicaSet" && o.Name == name {
				return true
			}
		}
	}
	return false
}

func stoppedRestateDeploymentComplete(w workloadRollout) (bool, bool, string) {
	if stoppedChildrenComplete(w) {
		return true, false, ""
	}
	return false, false, "waiting for stopped child evidence"
}
