package main

import (
	"github.com/azylman/aerial/services/synapse/internal/model"
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

type stoppedChildObservation struct {
	ReplicaSets []stoppedReplicaSet
	Pods        []stoppedPod
}

func toModelKubeObjectMeta(m kubeObjectMeta) model.KubeObjectMeta {
	refs := make([]model.KubeOwnerReference, len(m.OwnerReferences))
	for i, r := range m.OwnerReferences {
		refs[i] = model.KubeOwnerReference{
			APIVersion: r.APIVersion,
			Kind:       r.Kind,
			Name:       r.Name,
			UID:        r.UID,
			Controller: r.Controller,
		}
	}
	fields := make([]model.KubeManagedField, len(m.ManagedFields))
	for i, f := range m.ManagedFields {
		fields[i] = model.KubeManagedField{Time: f.Time, Subresource: f.Subresource}
	}
	return model.KubeObjectMeta{
		UID:             m.UID,
		Labels:          m.Labels,
		OwnerReferences: refs,
		Name:            m.Name,
		Namespace:       m.Namespace,
		Generation:      m.Generation,
		Annotations:     m.Annotations,
		ManagedFields:   fields,
	}
}

func toModelStoppedObservation(o *stoppedChildObservation) *model.StoppedChildObservation {
	if o == nil {
		return nil
	}
	rs := make([]model.StoppedReplicaSet, len(o.ReplicaSets))
	for i, r := range o.ReplicaSets {
		rs[i] = model.StoppedReplicaSet{
			Metadata: toModelKubeObjectMeta(r.Metadata),
			Spec:     r.Spec,
			Status:   r.Status,
		}
	}
	pods := make([]model.StoppedPod, len(o.Pods))
	for i, p := range o.Pods {
		pods[i] = model.StoppedPod{
			Metadata: toModelKubeObjectMeta(p.Metadata),
		}
	}
	return &model.StoppedChildObservation{
		ReplicaSets: rs,
		Pods:        pods,
	}
}

func fromModelStoppedObservation(o *model.StoppedChildObservation) *stoppedChildObservation {
	if o == nil {
		return nil
	}
	rs := make([]stoppedReplicaSet, len(o.ReplicaSets))
	for i, r := range o.ReplicaSets {
		rs[i] = stoppedReplicaSet{
			Metadata: fromModelKubeObjectMeta(r.Metadata),
			Spec:     r.Spec,
			Status:   r.Status,
		}
	}
	pods := make([]stoppedPod, len(o.Pods))
	for i, p := range o.Pods {
		pods[i] = stoppedPod{
			Metadata: fromModelKubeObjectMeta(p.Metadata),
		}
	}
	return &stoppedChildObservation{
		ReplicaSets: rs,
		Pods:        pods,
	}
}

func fromModelKubeObjectMeta(m model.KubeObjectMeta) kubeObjectMeta {
	refs := make([]kubeOwnerReference, len(m.OwnerReferences))
	for i, r := range m.OwnerReferences {
		refs[i] = kubeOwnerReference{
			APIVersion: r.APIVersion,
			Kind:       r.Kind,
			Name:       r.Name,
			UID:        r.UID,
			Controller: r.Controller,
		}
	}
	fields := make([]kubeManagedField, len(m.ManagedFields))
	for i, f := range m.ManagedFields {
		fields[i] = kubeManagedField{Time: f.Time, Subresource: f.Subresource}
	}
	return kubeObjectMeta{
		UID:             m.UID,
		Labels:          m.Labels,
		OwnerReferences: refs,
		Name:            m.Name,
		Namespace:       m.Namespace,
		Generation:      m.Generation,
		Annotations:     m.Annotations,
		ManagedFields:   fields,
	}
}

func toModelWorkloadRollouts(ws []workloadRollout) []model.WorkloadRollout {
	if ws == nil {
		return nil
	}
	res := make([]model.WorkloadRollout, len(ws))
	for i, w := range ws {
		res[i] = toModelWorkloadRollout(w)
	}
	return res
}

func fromModelWorkloadRollouts(ws []model.WorkloadRollout) []workloadRollout {
	if ws == nil {
		return nil
	}
	res := make([]workloadRollout, len(ws))
	for i, w := range ws {
		res[i] = fromModelWorkloadRollout(w)
	}
	return res
}

func toModelWorkloadRollout(w workloadRollout) model.WorkloadRollout {
	var selector *model.KubeLabelSelector
	if w.Selector != nil {
		selector = &model.KubeLabelSelector{
			MatchLabels:      w.Selector.MatchLabels,
			MatchExpressions: w.Selector.MatchExpressions,
		}
	}
	return model.WorkloadRollout{
		UID:                      w.UID,
		Selector:                 selector,
		StoppedChildren:          toModelStoppedObservation(w.StoppedChildren),
		Kind:                     w.Kind,
		Namespace:                w.Namespace,
		Name:                     w.Name,
		Images:                   w.Images,
		ChangedAt:                w.ChangedAt,
		Annotations:              w.Annotations,
		ConfigMapRefs:            w.ConfigMapRefs,
		Generation:               w.Generation,
		ObservedGeneration:       w.ObservedGeneration,
		Replicas:                 w.Replicas,
		UpdatedReplicas:          w.UpdatedReplicas,
		AvailableReplicas:        w.AvailableReplicas,
		StatusReplicas:           w.StatusReplicas,
		UpdateRevision:           w.UpdateRevision,
		CurrentRevision:          w.CurrentRevision,
		DesiredNumberScheduled:   w.DesiredNumberScheduled,
		UpdatedNumberScheduled:   w.UpdatedNumberScheduled,
		NumberAvailable:          w.NumberAvailable,
		ProgressDeadlineExceeded: w.ProgressDeadlineExceeded,
		ReadyReplicas:            w.ReadyReplicas,
		DeploymentID:             w.DeploymentID,
		ReadyConditionTrue:       w.ReadyConditionTrue,
		ManagedWorkloads:         toModelWorkloadRollouts(w.ManagedWorkloads),
		Env:                      w.Env,
		Resources:                model.KubeResourceRequirements{Limits: w.Resources.Limits, Requests: w.Resources.Requests},
	}
}

func fromModelWorkloadRollout(m model.WorkloadRollout) workloadRollout {
	var selector *kubeLabelSelector
	if m.Selector != nil {
		selector = &kubeLabelSelector{
			MatchLabels:      m.Selector.MatchLabels,
			MatchExpressions: m.Selector.MatchExpressions,
		}
	}
	return workloadRollout{
		UID:                      m.UID,
		Selector:                 selector,
		StoppedChildren:          fromModelStoppedObservation(m.StoppedChildren),
		Kind:                     m.Kind,
		Namespace:                m.Namespace,
		Name:                     m.Name,
		Images:                   m.Images,
		ChangedAt:                m.ChangedAt,
		Annotations:              m.Annotations,
		ConfigMapRefs:            m.ConfigMapRefs,
		Generation:               m.Generation,
		ObservedGeneration:       m.ObservedGeneration,
		Replicas:                 m.Replicas,
		UpdatedReplicas:          m.UpdatedReplicas,
		AvailableReplicas:        m.AvailableReplicas,
		StatusReplicas:           m.StatusReplicas,
		UpdateRevision:           m.UpdateRevision,
		CurrentRevision:          m.CurrentRevision,
		DesiredNumberScheduled:   m.DesiredNumberScheduled,
		UpdatedNumberScheduled:   m.UpdatedNumberScheduled,
		NumberAvailable:          m.NumberAvailable,
		ProgressDeadlineExceeded: m.ProgressDeadlineExceeded,
		ReadyReplicas:            m.ReadyReplicas,
		DeploymentID:             m.DeploymentID,
		ReadyConditionTrue:       m.ReadyConditionTrue,
		ManagedWorkloads:         fromModelWorkloadRollouts(m.ManagedWorkloads),
		Env:                      m.Env,
		Resources:                kubeResourceRequirements{Limits: m.Resources.Limits, Requests: m.Resources.Requests},
	}
}
