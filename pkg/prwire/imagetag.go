package prwire

import "strings"

// ExtractImageWorkloadName returns the workload name of a container image
// reference: the last path segment with any tag stripped.
func ExtractImageWorkloadName(imageRef string) string {
	ref := strings.TrimSpace(imageRef)
	if lastSlash := strings.LastIndex(ref, "/"); lastSlash != -1 {
		ref = ref[lastSlash+1:]
	}
	if colonIdx := strings.Index(ref, ":"); colonIdx != -1 {
		ref = ref[:colonIdx]
	}
	return strings.TrimSpace(ref)
}

// ResolveImageTag maps a repository and image reference to its
// aerial-image-tags ConfigMap key (TAG_<IMAGE>), canonical image name and
// workload name. Images named "none" or empty resolve to all-empty values.
func ResolveImageTag(repo, imageRef string) (tagKey, imageName, workloadName string) {
	workloadName = ExtractImageWorkloadName(imageRef)
	if workloadName == "" || strings.EqualFold(workloadName, "none") {
		return "", "", ""
	}
	switch repo[strings.LastIndex(repo, "/")+1:] {
	case "aerial":
		imageName = "aerial-" + strings.TrimPrefix(workloadName, "aerial-")
	case "aerial-sidecars":
		imageName = "aerial-sidecar-" + strings.TrimPrefix(workloadName, "aerial-sidecar-")
	case "mirrormere":
		if strings.HasPrefix(workloadName, "mirrormere") {
			imageName = workloadName
		} else {
			imageName = "mirrormere-" + workloadName
		}
	default:
		imageName = workloadName
	}
	tagKey = "TAG_" + strings.ToUpper(strings.ReplaceAll(imageName, "-", "_"))
	return tagKey, imageName, workloadName
}
