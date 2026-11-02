variable "image_tag" {
  type    = string
  default = "latest"
}

variable "extra_mounts" {
  type = list(object({
    type     = string
    target   = string
    source   = string
    readonly = bool
    volume_options = list(object({
      driver_config = list(object({
        name    = string
        options = list(map(string))
      }))
    }))
  }))
  default = []
}

variable "extra_env_template" {
  type    = string
  default = ""
}

variable "node_pool" {
  type    = string
  default = "calibarn"
}

job "eri" {
  datacenters = ["dc1"]
  type        = "service"

  node_pool   = var.node_pool

  update {
    max_parallel      = 1
    canary            = 1
    auto_promote      = true
    min_healthy_time  = "10s"
    healthy_deadline  = "3m"
    progress_deadline = "5m"
    auto_revert       = true
  }

  group "eri" {
    count = 1

    restart {
      attempts = 10
      interval = "15m"
      delay    = "10s"
      mode     = "fail"
    }

    reschedule {
      delay          = "10s"
      delay_function = "exponential"
      max_delay      = "1m"
      unlimited      = true
    }

    network {
      port "http" {
        to = 8088
      }
    }

    task "eri" {
      driver = "docker"
      user   = "0:0"

      kill_timeout = "60s"

      config {
        dns_servers        = ["${attr.unique.network.ip-address}"]
        dns_search_domains = ["aerial"]
        image        = "ghcr.io/azylman/aerial-eri:${var.image_tag}"
        init         = true
        ports        = ["http"]
        shm_size     = 1073741824
        healthchecks {
          disable = true
        }

        mounts = concat([
          {
            type     = "volume"
            target   = "/data"
            source   = "aerial-brain-data"
            readonly = false
          },
          {
            type     = "volume"
            target   = "/share"
            source   = "aerial-share"
            readonly = true
          }
        ], var.extra_mounts)
      }

      env {
        PORT                = "8088"
        AERIAL_HANGAR_URL   = "http://hangar.aerial/sync"
        ERI_URL             = "http://eri.aerial"
        GIT_TERMINAL_PROMPT = "0"
        GOCACHE             = "/data/cache/go-build"
        GOPATH              = "/data/cache/go"
        GOMODCACHE          = "/data/cache/go/pkg/mod"
        GOLANGCI_LINT_CACHE = "/data/cache/golangci-lint"
        GEMINI_HOME_DIR     = "/data"
      }

      template {
        data = <<EOH
{{- if nomadVarExists "nomad/jobs/shared" -}}
{{- with nomadVar "nomad/jobs/shared" -}}
{{- if .ANTIGRAVITY_OAUTH_TOKEN -}}{{- .ANTIGRAVITY_OAUTH_TOKEN -}}{{- end -}}
{{- end -}}
{{- end -}}
EOH
        destination = "local/antigravity-oauth-token"
        perms       = "0600"
        change_mode = "noop"
      }

      template {
        data = <<EOH
{{- if nomadVarExists "nomad/jobs/shared" -}}
{{- with nomadVar "nomad/jobs/shared" -}}
{{ . | toJSON }}
{{- end -}}
{{- else -}}
{}
{{- end -}}
EOH
        destination   = "secrets/shared-secrets.json"
        perms         = "0600"
        change_mode   = "signal"
        change_signal = "SIGHUP"
      }

      template {
        data = <<EOH
${var.extra_env_template}
EOH
        destination   = "secrets/eri.env"
        env           = true
        change_mode   = "signal"
        change_signal = "SIGHUP"
      }

      template {
        data = <<EOH
{{ if nomadVarExists "nomad/jobs/eri" }}
{{ with nomadVar "nomad/jobs/eri" }}
# Asset Hash: {{ if .ASSET_HASH }}{{ .ASSET_HASH }}{{ end }}
{{ if .CONFIG_YAML }}{{ .CONFIG_YAML }}{{ end }}
{{ end }}
{{ end }}
EOH
        destination   = "local/config.yaml"
        change_mode   = "signal"
        change_signal = "SIGHUP"
      }

      service {
        name     = "eri"
        port     = "http"
        provider = "nomad"

        check {
          name     = "eri-health"
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
        cpu        = 3000
        memory     = 8192
        memory_max = 16384
      }
    }
  }
}
