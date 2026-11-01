variable "image_tag" {
  type    = string
  default = "latest"
}

variable "node_pool" {
  type    = string
  default = "calibarn"
}

locals {
  repos = {
    "aerial"          = { slug = "azylman/aerial",          ci_count = 6 }
    "mirrormere"      = { slug = "azylman/mirrormere",      ci_count = 5 }
    "aerial-sidecars" = { slug = "azylman/aerial-sidecars", ci_count = 5 }
    "aerial-config"   = { slug = "azylman/aerial-config",   ci_count = 2 }
  }
}

job "github-runner" {
  datacenters = ["dc1"]
  type        = "service"
  node_pool   = var.node_pool

  update {
    max_parallel      = 2
    canary            = 0
    min_healthy_time  = "5s"
    healthy_deadline  = "1m"
    progress_deadline = "5m"
    auto_revert       = true
  }

  # group "roborev-runners" pool: 1 isolated review runner per repository without host docker access.
  dynamic "group" {
    for_each = local.repos
    labels   = ["roborev-${group.key}"]

    content {
      count = 1

      restart {
        attempts = 5
        interval = "15m"
        delay    = "15s"
        mode     = "fail"
      }

      task "runner" {
        driver = "docker"
        user   = "0:0"

        kill_timeout = "30s"

        config {
          image              = "ghcr.io/azylman/aerial-github-runner:${var.image_tag}"
          dns_servers        = ["${attr.unique.network.ip-address}"]
          dns_search_domains = ["aerial"]
        }

        env {
          TARGET_REPO   = group.value.slug
          RUNNER_NAME   = "aerial-roborev-${group.key}"
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
  }

  # group "ci-runners" pool: docker.sock and aerial-ci-cache:/cache are mounted strictly on ci-* groups
  # for container builds and integration testing, while roborev-* groups isolate untrusted code review
  # execution without host docker access.
  dynamic "group" {
    for_each = local.repos
    labels   = ["ci-${group.key}"]

    content {
      count = group.value.ci_count

      restart {
        attempts = 5
        interval = "15m"
        delay    = "15s"
        mode     = "fail"
      }

      task "runner" {
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
          TARGET_REPO   = group.value.slug
          RUNNER_NAME   = "aerial-ci-${group.key}-${NOMAD_ALLOC_INDEX}"
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
          cpu        = 500
          memory     = 512
          memory_max = 6144
        }
      }
    }
  }
}
