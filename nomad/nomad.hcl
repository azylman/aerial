# HashiCorp Nomad Cluster Server & HAOS Client Configuration
data_dir   = "/mnt/data/nomad/client"
bind_addr  = "0.0.0.0"
name       = "haos"
datacenter = "dc1"

server {
  enabled          = true
  bootstrap_expect = 1
  data_dir         = "/nomad/data"
}

client {
  enabled                 = true
  node_class              = "quiet-zero"
  cpu_total_compute       = 16000
  gc_interval             = "10m"
  gc_disk_usage_threshold = 90
  options = {
    "fingerprint.blacklist" = "env_aws,env_gce,env_azure,env_digitalocean"
  }
}

telemetry {
  collection_interval        = "15s"
  disable_hostname           = true
  prometheus_metrics         = true
  publish_allocation_metrics = true
  publish_node_metrics       = true
}

plugin "docker" {
  config {
    allow_privileged = true
    volumes {
      enabled = true
    }
    auth {
      config = "/mnt/data/nomad/client/docker-auth.json"
    }
    gc {
      image       = true
      image_delay = "168h"
      container   = true
    }
  }
}
