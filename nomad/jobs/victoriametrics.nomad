job "victoriametrics" {
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

  group "victoriametrics" {
    count = 1

    network {
      mode = "host"
      port "http" {
        static = 8428
      }
    }

    task "victoriametrics" {
      driver = "docker"

      config {
        dns_servers        = ["127.0.0.1"]
        dns_search_domains = ["aerial"]
        image        = "victoriametrics/victoria-metrics:v1.101.0"
        network_mode = "host"
        security_opt = ["no-new-privileges:true"]
        mounts = [
          {
            type     = "volume"
            target   = "/victoria-metrics-data"
            source   = "aerial-victoriametrics-data"
            readonly = false
          }
        ]
        volumes = [
          "/mnt/data/supervisor/share/aerial:/share/aerial:ro",
          "/mnt/data/supervisor/share/aerial-config:/share/aerial-config:ro"
        ]
        args = [
          "-storageDataPath=/victoria-metrics-data",
          "-promscrape.config=/share/aerial/victoriametrics/scrape.yml",
          "-promscrape.configCheckInterval=15s",
          "-retentionPeriod=5y",
          "-loggerFormat=json"
        ]
      }

      template {
        data = <<EOH
{{ if nomadVarExists "nomad/jobs/shared" }}
{{ with nomadVar "nomad/jobs/shared" }}
{{ if .HA_METRICS_TOKEN }}HA_METRICS_TOKEN="{{ .HA_METRICS_TOKEN }}"{{ end }}
{{ end }}
{{ end }}
EOH
        destination = "secrets/ha.env"
        env         = true
      }

      service {
        name     = "victoriametrics"
        port     = "http"
        provider = "nomad"

        check {
          name     = "victoriametrics-health"
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
        cpu        = 200
        memory     = 512
        memory_max = 1024
      }
    }
  }
}
