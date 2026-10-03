job "agentsview" {
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

  group "agentsview" {
    count = 1

    network {
      mode = "host"
      port "http" {
        static = 8082
      }
    }

    task "agentsview" {
      driver = "docker"

      config {
        dns_servers        = ["127.0.0.1"]
        dns_search_domains = ["aerial"]
        image        = "ghcr.io/azylman/agentsview:latest"
        network_mode = "host"
        command      = "/bin/sh"
        args         = ["/local/run-agentsview.sh"]
        mounts = [
          {
            type     = "volume"
            target   = "/agentsview-data"
            source   = "aerial-agentsview-data"
            readonly = false
          },
          {
            type     = "volume"
            target   = "/data"
            source   = "aerial-brain-data"
            readonly = true
          },
          {
            type     = "volume"
            target   = "/root/.gemini"
            source   = "aerial-brain-gemini"
            readonly = true
          }
        ]
      }

      template {
        data = <<EOH
#!/bin/sh
PUBLIC_URL="{{ if nomadVarExists "nomad/jobs/agentsview" }}{{ with nomadVar "nomad/jobs/agentsview" }}{{ if .PUBLIC_URL }}{{ .PUBLIC_URL }}{{ else }}http://localhost:8089{{ end }}{{ end }}{{ else }}http://localhost:8089{{ end }}"
PUBLIC_ORIGIN="{{ if nomadVarExists "nomad/jobs/agentsview" }}{{ with nomadVar "nomad/jobs/agentsview" }}{{ if .PUBLIC_ORIGIN }}{{ .PUBLIC_ORIGIN }}{{ else }}http://localhost:8089,http://127.0.0.1:8089{{ end }}{{ end }}{{ else }}http://localhost:8089,http://127.0.0.1:8089{{ end }}"

exec agentsview serve \
  --host=0.0.0.0 \
  --port=8082 \
  --base-path=/agentsview \
  --public-origin="$PUBLIC_ORIGIN" \
  --public-url="$PUBLIC_URL"
EOH
        destination = "local/run-agentsview.sh"
        perms       = "755"
      }

      env {
        AGENTSVIEW_DATA_DIR = "/agentsview-data"
      }

      service {
        name     = "agentsview"
        port     = "http"
        provider = "nomad"

        check {
          name     = "agentsview-http"
          type     = "http"
          path     = "/agentsview/"
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
        cpu    = 200
        memory = 512
      }
    }
  }
}
