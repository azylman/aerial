job "vector" {
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

  group "vector" {
    count = 1

    network {
      port "api" {
        to = 8686
      }
    }

    task "vector" {
      driver = "docker"

      config {
        dns_servers        = ["${attr.unique.network.ip-address}"]
        dns_search_domains = ["aerial"]
        image        = "timberio/vector:0.58.0-alpine"
        ports        = ["api"]
        healthchecks {
          disable = true
        }
        mounts = [
          {
            type     = "volume"
            target   = "/var/lib/vector"
            source   = "aerial-vector-data"
            readonly = false
          }
        ]
        volumes = [
          "/var/run/docker.sock:/var/run/docker.sock:ro",
          "/var/lib/docker/containers:/var/lib/docker/containers:ro",
          "/mnt/data/supervisor/share/aerial/vector:/etc/vector:ro"
        ]
      }

      env {
        VECTOR_LOG_FORMAT                             = "json"
        VECTOR_DANGEROUSLY_ALLOW_ENV_VAR_INTERPOLATION = "true"
        VECTOR_WATCH_CONFIG                           = "true"
        OPENOBSERVE_ROOT_USER_EMAIL                   = "admin@aerial.local"
        OPENOBSERVE_ENDPOINT                          = "http://openobserve.aerial"
      }

      template {
        data = <<EOH
{{ if nomadVarExists "nomad/jobs/shared" }}
{{ with nomadVar "nomad/jobs/shared" }}
{{ if .OPENOBSERVE_ROOT_USER_PASSWORD }}OPENOBSERVE_ROOT_USER_PASSWORD="{{ .OPENOBSERVE_ROOT_USER_PASSWORD }}"{{ end }}
{{ end }}
{{ end }}
EOH
        destination = "secrets/vector.env"
        env         = true
      }

      service {
        name     = "vector"
        port     = "api"
        provider = "nomad"

        check {
          name     = "vector-health"
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
