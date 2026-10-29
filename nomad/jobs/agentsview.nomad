variable "image_tag" {
  type    = string
  default = "latest"
}

variable "node_pool" {
  type    = string
  default = "calibarn"
}

job "agentsview" {
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

  group "agentsview" {
    count = 1

    network {
      port "http" {
        to = 8082
      }
    }

    task "agentsview" {
      driver = "docker"

      config {
        dns_servers        = ["${attr.unique.network.ip-address}"]
        dns_search_domains = ["aerial"]
        image        = "ghcr.io/azylman/agentsview:${var.image_tag}"
        ports        = ["http"]
        healthchecks {
          disable = true
        }
        entrypoint   = ["/bin/sh", "/local/run-agentsview.sh"]
        mounts = [
          {
            type     = "volume"
            target   = "/data"
            source   = "aerial-brain-data"
            readonly = false
          }
        ]
      }

      template {
        data = <<EOH
#!/bin/sh
mkdir -p /data/agentsview /root/.gemini/antigravity-cli
ln -sfn /data/brain /root/.gemini/antigravity-cli/brain
ln -sfn /data/conversations /root/.gemini/antigravity-cli/conversations
ln -sfn /data/runtimes/discord/.gemini/antigravity-cli/implicit /root/.gemini/antigravity-cli/implicit
ln -sfn /data/runtimes/discord/.gemini/antigravity-cli/cache /root/.gemini/antigravity-cli/cache
ln -sfn /data/runtimes/discord/.gemini/antigravity-cli/annotations /root/.gemini/antigravity-cli/annotations
ln -sfn /data/runtimes/discord/.gemini/antigravity-cli/conversation_summaries.db /root/.gemini/antigravity-cli/conversation_summaries.db
ln -sfn /data/runtimes/discord/.gemini/antigravity-cli/conversation_summaries.db-wal /root/.gemini/antigravity-cli/conversation_summaries.db-wal
ln -sfn /data/runtimes/discord/.gemini/antigravity-cli/conversation_summaries.db-shm /root/.gemini/antigravity-cli/conversation_summaries.db-shm

PUBLIC_URL="{{ if nomadVarExists "nomad/jobs/agentsview" }}{{ with nomadVar "nomad/jobs/agentsview" }}{{ if .PUBLIC_URL }}{{ .PUBLIC_URL }}{{ else }}http://localhost:8089{{ end }}{{ end }}{{ else }}http://localhost:8089{{ end }}"
PUBLIC_ORIGIN="{{ if nomadVarExists "nomad/jobs/agentsview" }}{{ with nomadVar "nomad/jobs/agentsview" }}{{ if .PUBLIC_ORIGIN }}{{ .PUBLIC_ORIGIN }}{{ else }}http://localhost:8089,http://127.0.0.1:8089{{ end }}{{ end }}{{ else }}http://localhost:8089,http://127.0.0.1:8089{{ end }}"

exec agentsview serve \
  --host=0.0.0.0 \
  --port=8082 \
  --base-path=/agentsview \
  --write-timeout=2m \
  --public-origin="$PUBLIC_ORIGIN" \
  --public-url="$PUBLIC_URL"
EOH
        destination = "local/run-agentsview.sh"
        perms       = "755"
      }

      env {
        AGENTSVIEW_DATA_DIR = "/data/agentsview"
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
        cpu        = 2000
        memory     = 1024
        memory_max = 2048
      }
    }
  }
}
