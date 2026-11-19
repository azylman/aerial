package alerts

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// Tracker tracks currently firing alerts by fingerprint to deduplicate alerts.
type Tracker struct {
	firing sync.Map
}

// MarkFiring attempts to mark an alert fingerprint as firing.
// It returns true if the alert was newly marked as firing (was not already firing),
// and false if the alert was already marked as firing.
func (t *Tracker) MarkFiring(fp string) bool {
	_, loaded := t.firing.LoadOrStore(fp, true)
	return !loaded
}

// ClearFiring removes an alert fingerprint from the firing set upon resolution or failure rollback.
func (t *Tracker) ClearFiring(fp string) {
	t.firing.Delete(fp)
}

// VMAlertNotification represents an Alertmanager v2-compatible alert notification payload.
type VMAlertNotification struct {
	Status       string            `json:"status,omitempty"`
	Labels       map[string]string `json:"labels"`
	Annotations  map[string]string `json:"annotations"`
	StartsAt     time.Time         `json:"startsAt"`
	EndsAt       time.Time         `json:"endsAt"`
	GeneratorURL string            `json:"generatorURL,omitempty"`
}

// VMAlertFingerprint computes a canonical, collision-resistant string fingerprint from alert labels.
func VMAlertFingerprint(labels map[string]string) string {
	if len(labels) == 0 {
		return "unknown"
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&sb, "%d:%s=%d:%s;", len(k), k, len(labels[k]), labels[k])
	}
	return sb.String()
}

// IsVMAlertResolved returns true if the alert status or timestamps indicate it is resolved.
func IsVMAlertResolved(a VMAlertNotification, now time.Time) bool {
	status := strings.TrimSpace(a.Status)
	if strings.EqualFold(status, "resolved") {
		return true
	}
	if status == "" && !a.EndsAt.IsZero() && (!a.EndsAt.After(a.StartsAt) || !a.EndsAt.After(now)) {
		return true
	}
	return false
}

// FormatVMAlertMessage produces a Discord markdown alert notification message.
func FormatVMAlertMessage(a VMAlertNotification) string {
	alertname := "UnnamedAlert"
	if a.Labels != nil {
		if name := strings.TrimSpace(a.Labels["alertname"]); name != "" {
			alertname = name
		}
	}
	severity := "warning"
	if a.Labels != nil {
		if sev := strings.TrimSpace(a.Labels["severity"]); sev != "" {
			severity = sev
		}
	}
	var details string
	if a.Annotations != nil {
		if summ := strings.TrimSpace(a.Annotations["summary"]); summ != "" {
			details = summ
		} else if desc := strings.TrimSpace(a.Annotations["description"]); desc != "" {
			details = desc
		}
	}
	if details == "" {
		return fmt.Sprintf("🚨 **[ALERT: %s]** (%s)", alertname, severity)
	}
	return fmt.Sprintf("🚨 **[ALERT: %s]** (%s)\n%s", alertname, severity, details)
}
