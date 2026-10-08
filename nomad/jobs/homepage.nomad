job "homepage" {
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

  group "homepage" {
    count = 1

    network {
      mode = "host"
      port "http" {
        static = 3001
      }
    }

    task "homepage" {
      driver = "docker"

      config {
        dns_servers        = ["127.0.0.1"]
        dns_search_domains = ["aerial"]
        image        = "ghcr.io/gethomepage/homepage:v2.4.0"
        network_mode = "host"
        healthchecks {
          disable = true
        }
        entrypoint   = ["/bin/sh", "/app/core-homepage/entrypoint.sh"]
        volumes = [
          "/var/run/docker.sock:/var/run/docker.sock:ro",
          "/mnt/data/supervisor/share/aerial/homepage:/app/core-homepage:ro",
          "/mnt/data/supervisor/share/aerial-config:/share/aerial-config:ro"
        ]
      }

      env {
        CONFIG_PATH = "/local/homepage.yaml"
      }

      template {
        data = <<EOH
{{ if nomadVarExists "nomad/jobs/homepage" }}
{{ with nomadVar "nomad/jobs/homepage" }}
# Config Hash: {{ .CONFIG_HASH }}
{{ if .HOMEPAGE_YAML }}{{ .HOMEPAGE_YAML }}{{ end }}
{{ end }}
{{ end }}
EOH
        destination = "local/homepage.yaml"
        change_mode = "restart"
      }

      template {
        data = <<EOH
{{ if nomadVarExists "nomad/jobs/shared" }}
{{ with nomadVar "nomad/jobs/shared" }}
{{ if .HA_METRICS_TOKEN }}HOMEPAGE_VAR_HA_TOKEN="{{ .HA_METRICS_TOKEN }}"
{{ else if .HA_TOKEN }}HOMEPAGE_VAR_HA_TOKEN="{{ .HA_TOKEN }}"
{{ end }}
{{ if .UNIFI_PASS }}HOMEPAGE_VAR_UNIFI_PASS="{{ .UNIFI_PASS }}"{{ end }}
{{ if .QNAP_PASS }}HOMEPAGE_VAR_QNAP_PASS="{{ .QNAP_PASS }}"{{ end }}
{{ end }}
{{ end }}
EOH
        destination = "secrets/homepage.env"
        env         = true
      }

      service {
        name     = "homepage"
        port     = "http"
        provider = "nomad"

        check {
          name     = "homepage-health"
          type     = "http"
          path     = "/api/healthcheck"
          interval = "15s"
          timeout  = "5s"
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
