job "brain" {
  datacenters = ["dc1"]
  type        = "service"

  # Target quiet-zero core server node where persistent volumes and NAS mounts reside
  constraint {
    attribute = "${node.class}"
    operator  = "regexp"
    value     = "quiet-zero|haos"
  }

  update {
    max_parallel      = 1
    canary            = 0
    min_healthy_time  = "10s"
    healthy_deadline  = "3m"
    progress_deadline = "5m"
    auto_revert       = true
  }

  group "brain" {
    count = 1

    restart {
      attempts = 10
      interval = "15m"
      delay    = "10s"
      mode     = "delay"
    }

    reschedule {
      delay          = "10s"
      delay_function = "exponential"
      max_delay      = "1m"
      unlimited      = true
    }

    network {
      mode = "host"
      port "http" {
        static = 8088
      }
    }

    task "brain" {
      driver = "docker"
      user   = "0:0"

      kill_timeout = "60s"

      config {
        dns_servers        = ["127.0.0.1"]
        dns_search_domains = ["aerial"]
        image        = "ghcr.io/azylman/aerial-brain:latest"
        network_mode = "host"
        shm_size     = 536870912

        mounts = [
          {
            type     = "volume"
            target   = "/data"
            source   = "aerial-brain-data"
            readonly = false
          },
          {
            type     = "volume"
            target   = "/root/.gemini"
            source   = "aerial-brain-gemini"
            readonly = false
          },
          {
            type     = "bind"
            target   = "/var/run/docker.sock"
            source   = "/var/run/docker.sock"
            readonly = false
          },
          {
            type     = "bind"
            target   = "/share/aerial-config"
            source   = "/mnt/data/supervisor/share/aerial-config"
            readonly = true
          },
          {
            type     = "bind"
            target   = "/share/aerial"
            source   = "/mnt/data/supervisor/share/aerial"
            readonly = true
          },
          {
            type     = "bind"
            target   = "/mnt/nas"
            source   = "/mnt/data/supervisor/share/NAS"
            readonly = true
          },
          {
            type     = "bind"
            target   = "/mnt/nas-scratch"
            source   = "/mnt/data/supervisor/share/Aerial"
            readonly = false
          }
        ]
      }

      env {
        PORT                        = "8088"
        AERIAL_HANGAR_URL           = "http://hangar:8087/sync"
        AGY_MODEL                   = ""
        DEFAULT_TIMEZONE            = "America/Los_Angeles"
        TZ                          = "America/Los_Angeles"
        GIT_TERMINAL_PROMPT         = "0"
        GOCACHE                     = "/data/cache/go-build"
        GOPATH                      = "/data/cache/go"
        GOMODCACHE                  = "/data/cache/go/pkg/mod"
        GOLANGCI_LINT_CACHE         = "/data/cache/golangci-lint"
      }

      template {
        data = <<EOH
{{ if nomadVarExists "nomad/jobs/brain" }}
{{ with nomadVar "nomad/jobs/brain" }}
{{ if .AERIAL_CONFIG_REPO_URL }}AERIAL_CONFIG_REPO_URL="{{ .AERIAL_CONFIG_REPO_URL }}"{{ end }}
{{ end }}
{{ end }}
{{ if nomadVarExists "nomad/jobs/shared" }}
{{ with nomadVar "nomad/jobs/shared" }}
{{ if .DISCORD_BOT_TOKEN }}DISCORD_BOT_TOKEN="{{ .DISCORD_BOT_TOKEN }}"
DISCORD_TOKEN="{{ .DISCORD_BOT_TOKEN }}"{{ end }}
{{ if .GITHUB_PAT }}GITHUB_PAT="{{ .GITHUB_PAT }}"{{ end }}
{{ if .HA_TOKEN }}HA_TOKEN="{{ .HA_TOKEN }}"{{ end }}
{{ if .GEMINI_HARNESS_API_KEY }}GEMINI_HARNESS_API_KEY="{{ .GEMINI_HARNESS_API_KEY }}"{{ end }}
{{ if .AGY_GEMINI_API_KEY }}AGY_GEMINI_API_KEY="{{ .AGY_GEMINI_API_KEY }}"
{{ else if .GEMINI_HARNESS_API_KEY }}AGY_GEMINI_API_KEY="{{ .GEMINI_HARNESS_API_KEY }}"{{ end }}
{{ if .POSTGRES_PASSWORD }}DATABASE_URL="postgres://aerial:{{ .POSTGRES_PASSWORD }}@postgres:5432/aerial?sslmode=disable"{{ end }}
{{ if .OPENOBSERVE_ROOT_USER_PASSWORD }}OPENOBSERVE_ROOT_USER_PASSWORD="{{ .OPENOBSERVE_ROOT_USER_PASSWORD }}"{{ end }}
{{ end }}
{{ end }}
EOH
        destination = "secrets/brain.env"
        env         = true
      }

      service {
        name     = "brain"
        port     = "http"
        provider = "nomad"

        check {
          name     = "brain-health"
          type     = "http"
          port     = "http"
          path     = "/health"
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
        cpu    = 2000
        memory = 2048
      }
    }
  }
}
