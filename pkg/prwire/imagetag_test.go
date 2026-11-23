package prwire

import "testing"

func TestExtractImageWorkloadName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		imageRef string
		want     string
	}{
		{name: "empty", imageRef: "", want: ""},
		{name: "bare name", imageRef: "engine", want: "engine"},
		{name: "registry path and tag", imageRef: "ghcr.io/acme/aerial-engine:v1", want: "aerial-engine"},
		{name: "surrounding spaces", imageRef: "  ghcr.io/acme/kiosk:sha123  ", want: "kiosk"},
		{name: "trailing slash", imageRef: "ghcr.io/acme/", want: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ExtractImageWorkloadName(tc.imageRef); got != tc.want {
				t.Errorf("ExtractImageWorkloadName(%q) = %q, want %q", tc.imageRef, got, tc.want)
			}
		})
	}
}

func TestResolveImageTag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		repo         string
		imageRef     string
		wantTagKey   string
		wantImage    string
		wantWorkload string
	}{
		{name: "empty ref", repo: "acme/aerial", imageRef: ""},
		{name: "none ref", repo: "acme/aerial", imageRef: "ghcr.io/acme/none:latest"},
		{name: "none ref any case", repo: "acme/aerial", imageRef: "NONE"},
		{name: "aerial bare service", repo: "acme/aerial", imageRef: "engine", wantTagKey: "TAG_AERIAL_ENGINE", wantImage: "aerial-engine", wantWorkload: "engine"},
		{name: "aerial already prefixed", repo: "acme/aerial", imageRef: "ghcr.io/acme/aerial-synapse:sha123", wantTagKey: "TAG_AERIAL_SYNAPSE", wantImage: "aerial-synapse", wantWorkload: "aerial-synapse"},
		{name: "bare aerial repo", repo: "aerial", imageRef: "haro", wantTagKey: "TAG_AERIAL_HARO", wantImage: "aerial-haro", wantWorkload: "haro"},
		{name: "sidecars bare service", repo: "acme/aerial-sidecars", imageRef: "camera", wantTagKey: "TAG_AERIAL_SIDECAR_CAMERA", wantImage: "aerial-sidecar-camera", wantWorkload: "camera"},
		{name: "sidecars already prefixed", repo: "acme/aerial-sidecars", imageRef: "ghcr.io/acme/aerial-sidecar-prospera:sha123", wantTagKey: "TAG_AERIAL_SIDECAR_PROSPERA", wantImage: "aerial-sidecar-prospera", wantWorkload: "aerial-sidecar-prospera"},
		{name: "bare sidecars repo", repo: "aerial-sidecars", imageRef: "ghcr.io/acme/migrator:sha123", wantTagKey: "TAG_AERIAL_SIDECAR_MIGRATOR", wantImage: "aerial-sidecar-migrator", wantWorkload: "migrator"},
		{name: "mirrormere bare service", repo: "acme/mirrormere", imageRef: "ghcr.io/acme/kiosk:sha123", wantTagKey: "TAG_MIRRORMERE_KIOSK", wantImage: "mirrormere-kiosk", wantWorkload: "kiosk"},
		{name: "mirrormere already prefixed", repo: "mirrormere", imageRef: "mirrormere-kiosk", wantTagKey: "TAG_MIRRORMERE_KIOSK", wantImage: "mirrormere-kiosk", wantWorkload: "mirrormere-kiosk"},
		{name: "other repo plain service", repo: "other-org/other-repo", imageRef: "ghcr.io/other/custom-service:sha123", wantTagKey: "TAG_CUSTOM_SERVICE", wantImage: "custom-service", wantWorkload: "custom-service"},
		{name: "other repo keeps workload", repo: "other-org/other-repo", imageRef: "ghcr.io/other/aerial-extra:sha123", wantTagKey: "TAG_AERIAL_EXTRA", wantImage: "aerial-extra", wantWorkload: "aerial-extra"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tagKey, image, workload := ResolveImageTag(tc.repo, tc.imageRef)
			if tagKey != tc.wantTagKey || image != tc.wantImage || workload != tc.wantWorkload {
				t.Errorf("ResolveImageTag(%q, %q) = (%q, %q, %q), want (%q, %q, %q)", tc.repo, tc.imageRef, tagKey, image, workload, tc.wantTagKey, tc.wantImage, tc.wantWorkload)
			}
		})
	}
}
