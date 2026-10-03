job "cadvisor" {
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

  group "cadvisor" {
    count = 1

    network {
      mode = "host"
      port "metrics" {
        static = 8083
      }
    }

    task "cadvisor" {
      driver = "docker"

      config {
        dns_servers        = ["127.0.0.1"]
        dns_search_domains = ["aerial"]
        image        = "gcr.io/cadvisor/cadvisor:v0.49.1"
        network_mode = "host"
        security_opt = ["no-new-privileges:true"]
        volumes = [
          "/:/rootfs:ro",
          "/var/run:/var/run:ro",
          "/sys:/sys:ro",
          "/sys/fs/cgroup:/sys/fs/cgroup:ro",
          "/var/lib/docker/:/var/lib/docker:ro",
          "/dev/disk/:/dev/disk:ro",
          "/var/run/docker.sock:/var/run/docker.sock:ro"
        ]
        args = [
          "--port=8083",
          "--docker_only=true",
          "--housekeeping_interval=15s",
          "--disable_metrics=percpu,sched,tcp,udp,advtcp,process,referenced_memory,hugetlb,memory_numa,cpu_topology,resctrl,cpuset",
          "--store_container_labels=false"
        ]
      }

      service {
        name     = "cadvisor"
        port     = "metrics"
        provider = "nomad"

        check {
          name     = "cadvisor-http"
          type     = "http"
          path     = "/healthz"
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
        memory     = 128
        memory_max = 256
      }
    }
  }
}
