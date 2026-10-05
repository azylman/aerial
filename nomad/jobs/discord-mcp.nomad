variable "image_tag" {
  type    = string
  default = "latest"
}

job "discord-mcp" {
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

  group "discord-mcp" {
    count = 1

    network {
      mode = "host"
      port "http" {
        static = 4001
      }
    }

    task "discord-mcp" {
      driver = "docker"

      config {
        dns_servers        = ["127.0.0.1"]
        dns_search_domains = ["aerial"]
        image        = "ghcr.io/azylman/aerial-discord-mcp:${var.image_tag}"
        force_pull   = true
        network_mode = "host"
        healthchecks {
          disable = true
        }
        volumes = [
          "/mnt/data/supervisor/share/coverage/discord-mcp:/coverage:rw",
        ]
      }

      env {
        GOCOVERDIR = "/coverage"
      }

      template {
        data = <<EOH
{{- if nomadVarExists "nomad/jobs/discord-mcp" -}}
{{- with nomadVar "nomad/jobs/discord-mcp" -}}
{{- if .CONFIG_YAML -}}{{- .CONFIG_YAML -}}{{- end -}}
{{- end -}}
{{- end -}}
EOH
        destination = "local/config.yaml"
        change_mode = "restart"
      }

      template {
        data = <<EOH
{{ if nomadVarExists "nomad/jobs/shared" }}
{{ with nomadVar "nomad/jobs/shared" }}
{{ if .DISCORD_BOT_TOKEN }}DISCORD_TOKEN="{{ .DISCORD_BOT_TOKEN }}"
DISCORD_BOT_TOKEN="{{ .DISCORD_BOT_TOKEN }}"
{{ else if .DISCORD_TOKEN }}DISCORD_TOKEN="{{ .DISCORD_TOKEN }}"
DISCORD_BOT_TOKEN="{{ .DISCORD_TOKEN }}"
{{ end }}
{{ end }}
{{ end }}
EOH
        destination = "secrets/discord.env"
        env         = true
      }

      service {
        name     = "discord-mcp"
        port     = "http"
        provider = "nomad"

        check {
          name     = "discord-mcp-health"
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
        memory     = 256
        memory_max = 512
      }
    }
  }
}
