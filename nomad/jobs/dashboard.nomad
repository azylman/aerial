variable "image_tag" {
  type    = string
  default = "latest"
}

job "dashboard" {
  datacenters = ["dc1"]
  type        = "service"

  # Target quiet-zero core server node
  constraint {
    attribute = "${node.class}"
    operator  = "regexp"
    value     = "quiet-zero|haos"
  }

  update {
    max_parallel      = 1
    canary            = 0
    min_healthy_time  = "5s"
    healthy_deadline  = "1m"
    progress_deadline = "2m"
    auto_revert       = true
  }

  group "dashboard" {
    count = 1

    network {
      mode = "host"
      port "http" {
        static = 8084
      }
    }

    task "dashboard" {
      driver = "docker"

      config {
        dns_servers        = ["127.0.0.1"]
        dns_search_domains = ["aerial"]
        image        = "ghcr.io/azylman/aerial-dashboard:${var.image_tag}"
        force_pull   = true
        network_mode = "host"
        healthchecks {
          disable = true
        }
        volumes = [
          "/var/run/docker.sock:/var/run/docker.sock:ro",
          "/mnt/data/supervisor/share/aerial-config:/share/aerial-config:ro",
          "/mnt/data/supervisor/share/coverage/dashboard:/coverage:rw"
        ]
      }

      env {
        CONFIG_PATH = "/share/aerial-config/services/dashboard/dashboard.yaml"
        NOMAD_ADDR  = "http://127.0.0.1:4646"
        GOCOVERDIR  = "/coverage"
      }

      template {
        data = <<EOH
{{ if nomadVarExists "nomad/jobs/shared" }}
{{ with nomadVar "nomad/jobs/shared" }}
{{ if .GITHUB_PAT }}GITHUB_PAT="{{ .GITHUB_PAT }}"{{ end }}
{{ end }}
{{ end }}
EOH
        destination = "secrets/dashboard.env"
        env         = true
      }

      service {
        name     = "dashboard"
        port     = "http"
        provider = "nomad"

        check {
          name     = "dashboard-health"
          type     = "http"
          path     = "/health"
          interval = "15s"
          timeout  = "3s"
          check_restart {
            limit           = 3
            grace           = "60s"
            ignore_warnings = false
          }
        }
      }

      resources {
        cpu        = 100
        memory     = 128
        memory_max = 256
      }
    }
  }
}
