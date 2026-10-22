variable "image_tag" {
  type    = string
  default = "latest"
}

job "roborev-runner" {
  datacenters = ["dc1"]
  type        = "service"
  node_pool   = "default"

  update {
    max_parallel      = 1
    canary            = 0
    min_healthy_time  = "5s"
    healthy_deadline  = "1m"
    progress_deadline = "2m"
    auto_revert       = true
  }

  group "runners" {
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
        image              = "ghcr.io/azylman/aerial-roborev-runner:${var.image_tag}"
        dns_servers        = ["${attr.unique.network.ip-address}"]
        dns_search_domains = ["aerial"]
        mounts = [
          {
            type     = "volume"
            target   = "/mnt/gemini-auth"
            source   = "aerial-brain-gemini"
            readonly = true
          }
        ]
      }

      env {
        TARGET_REPO = "azylman/aerial"
        RUNNER_NAME = "aerial-roborev-aerial"
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
        cpu        = 100
        memory     = 768
        memory_max = 2048
      }
    }

    task "runner-aerial-config" {
      driver = "docker"
      user   = "0:0"

      kill_timeout = "30s"

      config {
        image              = "ghcr.io/azylman/aerial-roborev-runner:${var.image_tag}"
        dns_servers        = ["${attr.unique.network.ip-address}"]
        dns_search_domains = ["aerial"]
        mounts = [
          {
            type     = "volume"
            target   = "/mnt/gemini-auth"
            source   = "aerial-brain-gemini"
            readonly = true
          }
        ]
      }

      env {
        TARGET_REPO = "azylman/aerial-config"
        RUNNER_NAME = "aerial-roborev-aerial-config"
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
        cpu        = 100
        memory     = 768
        memory_max = 2048
      }
    }

    task "runner-mirrormere" {
      driver = "docker"
      user   = "0:0"

      kill_timeout = "30s"

      config {
        image              = "ghcr.io/azylman/aerial-roborev-runner:${var.image_tag}"
        dns_servers        = ["${attr.unique.network.ip-address}"]
        dns_search_domains = ["aerial"]
        mounts = [
          {
            type     = "volume"
            target   = "/mnt/gemini-auth"
            source   = "aerial-brain-gemini"
            readonly = true
          }
        ]
      }

      env {
        TARGET_REPO = "azylman/mirrormere"
        RUNNER_NAME = "aerial-roborev-mirrormere"
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
        cpu        = 100
        memory     = 768
        memory_max = 2048
      }
    }

    task "runner-aerial-sidecars" {
      driver = "docker"
      user   = "0:0"

      kill_timeout = "30s"

      config {
        image              = "ghcr.io/azylman/aerial-roborev-runner:${var.image_tag}"
        dns_servers        = ["${attr.unique.network.ip-address}"]
        dns_search_domains = ["aerial"]
        mounts = [
          {
            type     = "volume"
            target   = "/mnt/gemini-auth"
            source   = "aerial-brain-gemini"
            readonly = true
          }
        ]
      }

      env {
        TARGET_REPO = "azylman/aerial-sidecars"
        RUNNER_NAME = "aerial-roborev-aerial-sidecars"
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
        cpu        = 100
        memory     = 768
        memory_max = 2048
      }
    }
  }
}
