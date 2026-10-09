variable "image_tag" {
  type    = string
  default = "latest"
}

job "hangar" {
  datacenters = ["dc1"]
  type        = "service"

  node_pool   = "default"

  update {
    max_parallel      = 1
    canary            = 0
    min_healthy_time  = "10s"
    healthy_deadline  = "2m"
    progress_deadline = "3m"
    auto_revert       = true
  }

  group "hangar" {
    count = 1

    network {
      port "http" {
        to = 8087
      }
    }

    task "hangar" {
      driver = "docker"
      user   = "0:0"

      kill_timeout = "30s"

      config {
        dns_servers        = ["${attr.unique.network.ip-address}"]
        dns_search_domains = ["aerial"]
        image        = "ghcr.io/azylman/aerial-hangar:${var.image_tag}"
        ports        = ["http"]
        healthchecks {
          disable = true
        }
        volumes = [
          "/mnt/data/supervisor/share/aerial-config:/share/aerial-config:rw",
          "/mnt/data/supervisor/share/aerial:/share/aerial:rw",
          "/mnt/data/supervisor/share/mirrormere:/share/mirrormere:rw",
          "/mnt/data/supervisor/share/coverage/hangar:/coverage:rw",
          "/var/run/docker.sock:/var/run/docker.sock:rw"
        ]
      }

      env {
        CONFIG_PATH        = "/share/aerial-config/services/hangar/hangar.yaml"
        NOMAD_ADDR         = "http://${attr.unique.network.ip-address}:4646"
        BRAIN_INTERNAL_URL = "http://brain.aerial/internal/reload"
        PORT               = "8087"
        GOCOVERDIR         = "/coverage"
      }

      template {
        data = <<EOH
{{ if nomadVarExists "nomad/jobs/shared" }}
{{ with nomadVar "nomad/jobs/shared" }}
{{ if .GITHUB_PAT }}GITHUB_PAT="{{ .GITHUB_PAT }}"{{ end }}
{{ if .DISCORD_BOT_TOKEN }}DISCORD_BOT_TOKEN="{{ .DISCORD_BOT_TOKEN }}"{{ end }}
{{ end }}
{{ end }}
EOH
        destination = "secrets/hangar.env"
        env         = true
      }

      service {
        name     = "hangar"
        port     = "http"
        provider = "nomad"

        check {
          name     = "hangar-http"
          type     = "http"
          port     = "http"
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
        memory     = 256
        memory_max = 512
      }
    }
  }
}
