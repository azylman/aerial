job "coredns" {
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

  group "coredns" {
    count = 1

    network {
      mode = "host"
      port "dns" {
        static = 53
      }
    }

    task "coredns" {
      driver = "docker"

      user         = "0:0"
      kill_timeout = "10s"

      config {
        image        = "coredns/coredns:latest"
        network_mode = "host"
        args         = ["-conf", "/local/Corefile"]
      }

      template {
        data = <<EOH
.:53 {
    hosts /local/hosts {
        reload 5s
        fallthrough
    }
    forward . {{ if nomadVarExists "nomad/jobs/coredns" }}{{ with nomadVar "nomad/jobs/coredns" }}{{ if .UPSTREAM_DNS }}{{ .UPSTREAM_DNS }}{{ else }}1.1.1.1 8.8.8.8{{ end }}{{ end }}{{ else }}1.1.1.1 8.8.8.8{{ end }} {
        except aerial lan
    }
    cache 30 {
        success 1024
        denial 512
    }
    errors
}
EOH
        destination = "local/Corefile"
      }

      template {
        data = <<EOH
{{ range nomadServices }}
{{ range nomadService .Name }}
{{ .Address }} {{ .Name }} {{ .Name }}.lan {{ .Name }}.aerial
{{ end }}
{{ end }}
EOH
        destination = "local/hosts"
        change_mode = "noop"
      }

      service {
        name     = "coredns"
        port     = "dns"
        provider = "nomad"

        check {
          name     = "coredns-tcp"
          type     = "tcp"
          port     = "dns"
          interval = "15s"
          timeout  = "3s"

          check_restart {
            limit           = 3
            grace           = "30s"
            ignore_warnings = false
          }
        }
      }

      resources {
        cpu    = 100
        memory = 64
      }
    }
  }
}
