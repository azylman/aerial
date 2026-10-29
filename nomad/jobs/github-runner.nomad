variable "image_tag" {
  type    = string
  default = "latest"
}

variable "node_pool" {
  type    = string
  default = "calibarn"
}

job "github-runner" {
  datacenters = ["dc1"]
  type        = "service"
  node_pool   = var.node_pool

  update {
    max_parallel      = 1
    canary            = 0
    min_healthy_time  = "5s"
    healthy_deadline  = "1m"
    progress_deadline = "2m"
    auto_revert       = true
  }

  group "roborev-runners" {
    count = 1

    restart {
      attempts = 5
      interval = "15m"
      delay    = "15s"
      mode     = "fail"
    }

    task "runner-aerial" {
      driver = "docker"
      user   = "0:0"

      kill_timeout = "30s"

      config {
        image              = "ghcr.io/azylman/aerial-github-runner:${var.image_tag}"
        dns_servers        = ["${attr.unique.network.ip-address}"]
        dns_search_domains = ["aerial"]
      }

      env {
        TARGET_REPO   = "azylman/aerial"
        RUNNER_NAME   = "aerial-roborev-aerial"
        RUNNER_LABELS = "self-hosted,roborev"
      }

      template {
        data = <<EOH
{{ if nomadVarExists "nomad/jobs/shared" }}
{{ with nomadVar "nomad/jobs/shared" }}
{{ if .GITHUB_PAT }}GITHUB_PAT="{{ .GITHUB_PAT }}"{{ end }}
{{ end }}
{{ end }}
EOH
        destination = "secrets/runner.env"
        env         = true
        change_mode = "restart"
      }

      template {
        data = <<EOH
{{- if nomadVarExists "nomad/jobs/shared" -}}
{{- with nomadVar "nomad/jobs/shared" -}}
{{- if .ANTIGRAVITY_OAUTH_TOKEN -}}{{- .ANTIGRAVITY_OAUTH_TOKEN -}}{{- end -}}
{{- end -}}
{{- end -}}
EOH
        destination = "local/antigravity-oauth-token"
        perms       = "0600"
        change_mode = "restart"
      }

      resources {
        cpu        = 1000
        memory     = 1024
        memory_max = 3072
      }
    }

    task "runner-aerial-config" {
      driver = "docker"
      user   = "0:0"

      kill_timeout = "30s"

      config {
        image              = "ghcr.io/azylman/aerial-github-runner:${var.image_tag}"
        dns_servers        = ["${attr.unique.network.ip-address}"]
        dns_search_domains = ["aerial"]
      }

      env {
        TARGET_REPO   = "azylman/aerial-config"
        RUNNER_NAME   = "aerial-roborev-aerial-config"
        RUNNER_LABELS = "self-hosted,roborev"
      }

      template {
        data = <<EOH
{{ if nomadVarExists "nomad/jobs/shared" }}
{{ with nomadVar "nomad/jobs/shared" }}
{{ if .GITHUB_PAT }}GITHUB_PAT="{{ .GITHUB_PAT }}"{{ end }}
{{ end }}
{{ end }}
EOH
        destination = "secrets/runner.env"
        env         = true
        change_mode = "restart"
      }

      template {
        data = <<EOH
{{- if nomadVarExists "nomad/jobs/shared" -}}
{{- with nomadVar "nomad/jobs/shared" -}}
{{- if .ANTIGRAVITY_OAUTH_TOKEN -}}{{- .ANTIGRAVITY_OAUTH_TOKEN -}}{{- end -}}
{{- end -}}
{{- end -}}
EOH
        destination = "local/antigravity-oauth-token"
        perms       = "0600"
        change_mode = "restart"
      }

      resources {
        cpu        = 1000
        memory     = 1024
        memory_max = 3072
      }
    }

    task "runner-mirrormere" {
      driver = "docker"
      user   = "0:0"

      kill_timeout = "30s"

      config {
        image              = "ghcr.io/azylman/aerial-github-runner:${var.image_tag}"
        dns_servers        = ["${attr.unique.network.ip-address}"]
        dns_search_domains = ["aerial"]
      }

      env {
        TARGET_REPO   = "azylman/mirrormere"
        RUNNER_NAME   = "aerial-roborev-mirrormere"
        RUNNER_LABELS = "self-hosted,roborev"
      }

      template {
        data = <<EOH
{{ if nomadVarExists "nomad/jobs/shared" }}
{{ with nomadVar "nomad/jobs/shared" }}
{{ if .GITHUB_PAT }}GITHUB_PAT="{{ .GITHUB_PAT }}"{{ end }}
{{ end }}
{{ end }}
EOH
        destination = "secrets/runner.env"
        env         = true
        change_mode = "restart"
      }

      template {
        data = <<EOH
{{- if nomadVarExists "nomad/jobs/shared" -}}
{{- with nomadVar "nomad/jobs/shared" -}}
{{- if .ANTIGRAVITY_OAUTH_TOKEN -}}{{- .ANTIGRAVITY_OAUTH_TOKEN -}}{{- end -}}
{{- end -}}
{{- end -}}
EOH
        destination = "local/antigravity-oauth-token"
        perms       = "0600"
        change_mode = "restart"
      }

      resources {
        cpu        = 1000
        memory     = 1024
        memory_max = 3072
      }
    }

    task "runner-aerial-sidecars" {
      driver = "docker"
      user   = "0:0"

      kill_timeout = "30s"

      config {
        image              = "ghcr.io/azylman/aerial-github-runner:${var.image_tag}"
        dns_servers        = ["${attr.unique.network.ip-address}"]
        dns_search_domains = ["aerial"]
      }

      env {
        TARGET_REPO   = "azylman/aerial-sidecars"
        RUNNER_NAME   = "aerial-roborev-aerial-sidecars"
        RUNNER_LABELS = "self-hosted,roborev"
      }

      template {
        data = <<EOH
{{ if nomadVarExists "nomad/jobs/shared" }}
{{ with nomadVar "nomad/jobs/shared" }}
{{ if .GITHUB_PAT }}GITHUB_PAT="{{ .GITHUB_PAT }}"{{ end }}
{{ end }}
{{ end }}
EOH
        destination = "secrets/runner.env"
        env         = true
        change_mode = "restart"
      }

      template {
        data = <<EOH
{{- if nomadVarExists "nomad/jobs/shared" -}}
{{- with nomadVar "nomad/jobs/shared" -}}
{{- if .ANTIGRAVITY_OAUTH_TOKEN -}}{{- .ANTIGRAVITY_OAUTH_TOKEN -}}{{- end -}}
{{- end -}}
{{- end -}}
EOH
        destination = "local/antigravity-oauth-token"
        perms       = "0600"
        change_mode = "restart"
      }

      resources {
        cpu        = 1000
        memory     = 1024
        memory_max = 3072
      }
    }
  }

  group "ci-runners" {
    count = 1

    restart {
      attempts = 5
      interval = "15m"
      delay    = "15s"
      mode     = "fail"
    }

    # docker.sock is mounted strictly on ci-runners for container builds and integration testing,
    # while roborev-runners isolate untrusted code review execution without host docker access.

    task "ci-runner-aerial-1" {
      driver = "docker"
      user   = "0:0"

      kill_timeout = "30s"

      config {
        image              = "ghcr.io/azylman/aerial-github-runner:${var.image_tag}"
        dns_servers        = ["${attr.unique.network.ip-address}"]
        dns_search_domains = ["aerial"]
        volumes            = ["/var/run/docker.sock:/var/run/docker.sock"]
        mounts = [
          {
            type     = "volume"
            target   = "/cache"
            source   = "aerial-ci-cache"
            readonly = false
          }
        ]
      }

      env {
        TARGET_REPO   = "azylman/aerial"
        RUNNER_NAME   = "aerial-ci-aerial-1"
        RUNNER_LABELS = "self-hosted,ci"
      }

      template {
        data = <<EOH
{{ if nomadVarExists "nomad/jobs/shared" }}
{{ with nomadVar "nomad/jobs/shared" }}
{{ if .GITHUB_PAT }}GITHUB_PAT="{{ .GITHUB_PAT }}"{{ end }}
{{ end }}
{{ end }}
EOH
        destination = "secrets/runner.env"
        env         = true
        change_mode = "restart"
      }

      resources {
        cpu        = 2000
        memory     = 3072
        memory_max = 6144
      }
    }

    task "ci-runner-aerial-2" {
      driver = "docker"
      user   = "0:0"

      kill_timeout = "30s"

      config {
        image              = "ghcr.io/azylman/aerial-github-runner:${var.image_tag}"
        dns_servers        = ["${attr.unique.network.ip-address}"]
        dns_search_domains = ["aerial"]
        volumes            = ["/var/run/docker.sock:/var/run/docker.sock"]
        mounts = [
          {
            type     = "volume"
            target   = "/cache"
            source   = "aerial-ci-cache"
            readonly = false
          }
        ]
      }

      env {
        TARGET_REPO   = "azylman/aerial"
        RUNNER_NAME   = "aerial-ci-aerial-2"
        RUNNER_LABELS = "self-hosted,ci"
      }

      template {
        data = <<EOH
{{ if nomadVarExists "nomad/jobs/shared" }}
{{ with nomadVar "nomad/jobs/shared" }}
{{ if .GITHUB_PAT }}GITHUB_PAT="{{ .GITHUB_PAT }}"{{ end }}
{{ end }}
{{ end }}
EOH
        destination = "secrets/runner.env"
        env         = true
        change_mode = "restart"
      }

      resources {
        cpu        = 2000
        memory     = 3072
        memory_max = 6144
      }
    }

    task "ci-runner-aerial-config" {
      driver = "docker"
      user   = "0:0"

      kill_timeout = "30s"

      config {
        image              = "ghcr.io/azylman/aerial-github-runner:${var.image_tag}"
        dns_servers        = ["${attr.unique.network.ip-address}"]
        dns_search_domains = ["aerial"]
        volumes            = ["/var/run/docker.sock:/var/run/docker.sock"]
        mounts = [
          {
            type     = "volume"
            target   = "/cache"
            source   = "aerial-ci-cache"
            readonly = false
          }
        ]
      }

      env {
        TARGET_REPO   = "azylman/aerial-config"
        RUNNER_NAME   = "aerial-ci-aerial-config"
        RUNNER_LABELS = "self-hosted,ci"
      }

      template {
        data = <<EOH
{{ if nomadVarExists "nomad/jobs/shared" }}
{{ with nomadVar "nomad/jobs/shared" }}
{{ if .GITHUB_PAT }}GITHUB_PAT="{{ .GITHUB_PAT }}"{{ end }}
{{ end }}
{{ end }}
EOH
        destination = "secrets/runner.env"
        env         = true
        change_mode = "restart"
      }

      resources {
        cpu        = 2000
        memory     = 3072
        memory_max = 6144
      }
    }

    task "ci-runner-mirrormere" {
      driver = "docker"
      user   = "0:0"

      kill_timeout = "30s"

      config {
        image              = "ghcr.io/azylman/aerial-github-runner:${var.image_tag}"
        dns_servers        = ["${attr.unique.network.ip-address}"]
        dns_search_domains = ["aerial"]
        volumes            = ["/var/run/docker.sock:/var/run/docker.sock"]
        mounts = [
          {
            type     = "volume"
            target   = "/cache"
            source   = "aerial-ci-cache"
            readonly = false
          }
        ]
      }

      env {
        TARGET_REPO   = "azylman/mirrormere"
        RUNNER_NAME   = "aerial-ci-mirrormere"
        RUNNER_LABELS = "self-hosted,ci"
      }

      template {
        data = <<EOH
{{ if nomadVarExists "nomad/jobs/shared" }}
{{ with nomadVar "nomad/jobs/shared" }}
{{ if .GITHUB_PAT }}GITHUB_PAT="{{ .GITHUB_PAT }}"{{ end }}
{{ end }}
{{ end }}
EOH
        destination = "secrets/runner.env"
        env         = true
        change_mode = "restart"
      }

      resources {
        cpu        = 2000
        memory     = 3072
        memory_max = 6144
      }
    }

    task "ci-runner-aerial-sidecars" {
      driver = "docker"
      user   = "0:0"

      kill_timeout = "30s"

      config {
        image              = "ghcr.io/azylman/aerial-github-runner:${var.image_tag}"
        dns_servers        = ["${attr.unique.network.ip-address}"]
        dns_search_domains = ["aerial"]
        volumes            = ["/var/run/docker.sock:/var/run/docker.sock"]
        mounts = [
          {
            type     = "volume"
            target   = "/cache"
            source   = "aerial-ci-cache"
            readonly = false
          }
        ]
      }

      env {
        TARGET_REPO   = "azylman/aerial-sidecars"
        RUNNER_NAME   = "aerial-ci-aerial-sidecars"
        RUNNER_LABELS = "self-hosted,ci"
      }

      template {
        data = <<EOH
{{ if nomadVarExists "nomad/jobs/shared" }}
{{ with nomadVar "nomad/jobs/shared" }}
{{ if .GITHUB_PAT }}GITHUB_PAT="{{ .GITHUB_PAT }}"{{ end }}
{{ end }}
{{ end }}
EOH
        destination = "secrets/runner.env"
        env         = true
        change_mode = "restart"
      }

      resources {
        cpu        = 2000
        memory     = 3072
        memory_max = 6144
      }
    }
  }
}
