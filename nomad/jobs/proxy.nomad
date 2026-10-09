variable "image_tag" {
  type    = string
  default = "latest"
}

job "proxy" {
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

  group "proxy" {
    count = 1

    network {
      mode = "host"
      port "http" {
        static = 80
      }
    }

    task "proxy" {
      driver = "docker"

      config {
        dns_servers        = ["127.0.0.1"]
        dns_search_domains = ["aerial"]
        image        = "ghcr.io/azylman/aerial-proxy:${var.image_tag}"
        network_mode = "host"
        healthchecks {
          disable = true
        }
        volumes = [
          "/mnt/data/supervisor/share/aerial-config/proxy/default.conf:/etc/nginx/conf.d/default.conf:ro",
          "/mnt/data/supervisor/share/aerial/proxy/grafana-cyberpunk.css:/etc/nginx/grafana-cyberpunk.css:ro",
          "/mnt/data/supervisor/share/aerial/proxy/agentsview-cyberpunk.css:/etc/nginx/agentsview-cyberpunk.css:ro",
          "/mnt/data/supervisor/share/aerial/proxy/openobserve-cyberpunk.css:/etc/nginx/openobserve-cyberpunk.css:ro"
        ]
      }

      template {
        data = <<EOH
# Dynamic Nomad service upstreams rendered by Nomad template
{{ range nomadServices }}
{{ if ne .Name "proxy" }}
upstream {{ .Name }} {
{{ range nomadService .Name }}
    server {{ .Address }}:{{ .Port }};
{{ else }}
    server 127.0.0.1:65535 down;
{{ end }}
    keepalive 16;
}
{{ end }}
{{ end }}
EOH
        destination   = "local/upstreams.conf"
        change_mode   = "signal"
        change_signal = "SIGHUP"
      }

      service {
        name     = "proxy"
        port     = "http"
        provider = "nomad"

        check {
          name     = "proxy-health"
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
