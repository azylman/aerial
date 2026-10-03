job "aerial-core-db-migrate" {
  datacenters = ["dc1"]
  type        = "batch"

  # Target quiet-zero core server node
  constraint {
    attribute = "${node.class}"
    operator  = "regexp"
    value     = "quiet-zero|haos"
  }

  reschedule {
    attempts = 0
  }

  group "migrate" {
    count = 1

    network {
      mode = "host"
    }

    restart {
      attempts = 2
      interval = "5m"
      delay    = "15s"
      mode     = "fail"
    }

    task "atlas" {
      driver = "docker"

      config {
        image        = "arigaio/atlas:latest"
        network_mode = "host"
        healthchecks {
          disable = true
        }
        command      = "migrate"
        args = [
          "apply",
          "--env", "core",
          "--config", "file:///local/atlas.hcl"
        ]
        mount {
          type     = "bind"
          target   = "/migrations"
          source   = "/mnt/data/supervisor/share/aerial/brain/pkg/db/migrations"
          readonly = true
        }
      }

      template {
        data = <<EOH
variable "postgres_url" {
  type    = string
  default = "{{ if nomadVarExists "nomad/jobs/aerial-core-db-migrate" }}{{ with nomadVar "nomad/jobs/aerial-core-db-migrate" }}{{ if .POSTGRES_URL }}{{ .POSTGRES_URL }}{{ else }}postgres://aerial:aerial_secure_pass@127.0.0.1:5432/aerial?sslmode=disable{{ end }}{{ end }}{{ else }}postgres://aerial:aerial_secure_pass@127.0.0.1:5432/aerial?sslmode=disable{{ end }}"
}

env "core" {
  url = var.postgres_url
  migration {
    dir = "file:///migrations"
  }
}
EOH
        destination = "local/atlas.hcl"
      }

      resources {
        cpu        = 100
        memory     = 256
        memory_max = 512
      }
    }
  }
}
