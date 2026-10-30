variable "image_tag" {
  type    = string
  default = "latest"
}

job "docs" {
  datacenters = ["dc1"]
  type        = "service"

  node_pool   = "default"

  update {
    max_parallel      = 1
    canary            = 1
    auto_promote      = true
    min_healthy_time  = "5s"
    healthy_deadline  = "1m"
    progress_deadline = "2m"
    auto_revert       = true
  }

  group "docs" {
    count = 1

    network {
      port "http" {
        to = 80
      }
    }

    task "docs" {
      driver = "docker"

      config {
        dns_servers        = ["${attr.unique.network.ip-address}"]
        dns_search_domains = ["aerial"]
        image              = "ghcr.io/azylman/aerial-docs:${var.image_tag}"
        ports              = ["http"]
        healthchecks {
          disable = true
        }
        entrypoint         = ["/bin/sh", "-c", "mkdir -p /share/aerial-config/docs && cp -a /local/docs/app/. /usr/share/nginx/html/ && cp -a /local/docs/content/. /share/aerial-config/docs/ && exec /docker-entrypoint.sh nginx -g 'daemon off;'"]
      }

      artifact {
        source      = "http://hangar.aerial/internal/artifacts/docs.tar.gz"
        destination = "local/docs"
      }

      template {
        data = <<EOH
{{- if nomadVarExists "nomad/jobs/docs" -}}
{{- with nomadVar "nomad/jobs/docs" -}}
{{- if .ASSET_HASH -}}{{- .ASSET_HASH -}}{{- end -}}
{{- end -}}
{{- end -}}
EOH
        destination = "local/assets.sha256"
        change_mode = "script"
        change_script {
          command       = "/bin/sh"
          args          = ["-c", "rm -rf /tmp/docs-stage && mkdir -p /tmp/docs-stage && wget -qO /tmp/docs.tar.gz http://hangar.aerial/internal/artifacts/docs.tar.gz && tar -xzf /tmp/docs.tar.gz -C /tmp/docs-stage && cp -a /tmp/docs-stage/app/. /usr/share/nginx/html/ && rm -rf /share/aerial-config/docs/* && cp -a /tmp/docs-stage/content/. /share/aerial-config/docs/ && rm -rf /tmp/docs-stage /tmp/docs.tar.gz"]
          timeout       = "30s"
          fail_on_error = false
        }
      }

      service {
        name     = "docs"
        port     = "http"
        provider = "nomad"

        check {
          name            = "docs-http"
          type            = "http"
          path            = "/health"
          interval        = "15s"
          timeout         = "3s"
          check_restart {
            limit           = 3
            grace           = "60s"
            ignore_warnings = false
          }
        }
      }

      resources {
        cpu        = 50
        memory     = 64
        memory_max = 128
      }
    }
  }
}
