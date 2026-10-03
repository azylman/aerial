job "github-mcp" {
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

  group "github-mcp" {
    count = 1

    network {
      mode = "host"
      port "mcp" {
        static = 4003
      }
    }

    task "github-mcp" {
      driver = "docker"

      config {
        dns_servers        = ["127.0.0.1"]
        dns_search_domains = ["aerial"]
        image        = "ghcr.io/azylman/aerial-github-mcp:latest"
        force_pull   = true
        network_mode = "host"
      }

      template {
        data = <<EOH
{{- if nomadVarExists "nomad/jobs/github-mcp" -}}
{{- with nomadVar "nomad/jobs/github-mcp" -}}
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
{{ if .GITHUB_PAT }}GITHUB_PERSONAL_ACCESS_TOKEN="{{ .GITHUB_PAT }}"
GITHUB_PAT="{{ .GITHUB_PAT }}"
{{ else if .GITHUB_PERSONAL_ACCESS_TOKEN }}GITHUB_PERSONAL_ACCESS_TOKEN="{{ .GITHUB_PERSONAL_ACCESS_TOKEN }}"
GITHUB_PAT="{{ .GITHUB_PERSONAL_ACCESS_TOKEN }}"
{{ end }}
{{ end }}
{{ end }}
EOH
        destination = "secrets/github.env"
        env         = true
      }

      service {
        name     = "github-mcp"
        port     = "mcp"
        provider = "nomad"

        check {
          name     = "github-mcp-tcp"
          type     = "tcp"
          port     = "mcp"
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
