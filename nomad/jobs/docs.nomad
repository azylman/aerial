job "docs" {
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

  group "docs" {
    count = 1

    network {
      mode = "host"
      port "http" {
        static = 8090
      }
    }

    task "docs" {
      driver = "docker"

      config {
        dns_servers        = ["127.0.0.1"]
        dns_search_domains = ["aerial"]
        image        = "ghcr.io/azylman/aerial-docs:latest"
        network_mode = "host"
        command      = "nginx"
        args         = ["-g", "daemon off;", "-c", "/local/nginx.conf"]
        volumes = [
          "/mnt/data/supervisor/share/aerial-config:/share/aerial-config:ro",
          "/mnt/data/supervisor/share/aerial/docs-service/app:/usr/share/nginx/html:ro"
        ]
      }

      template {
        destination = "local/nginx.conf"
        change_mode = "restart"
        perms       = "0644"
        data        = <<EOH
events {
    worker_connections 1024;
}

http {
    include /etc/nginx/mime.types;
    default_type application/octet-stream;
    sendfile on;
    server_tokens off;

    server {
        listen 8090;
        server_name _;

        location = /health {
            access_log off;
            return 200 "healthy\n";
        }

        location ~* \.css$ {
            root /usr/share/nginx/html;
            add_header Cache-Control "no-cache, must-revalidate";
            add_header X-Content-Type-Options "nosniff" always;
        }

        location /assets/ {
            alias /usr/share/nginx/html/assets/;
            expires 7d;
            add_header Cache-Control "public, immutable";
            add_header X-Content-Type-Options "nosniff" always;
        }

        location = /index.html {
            root /usr/share/nginx/html;
            add_header Cache-Control "no-cache, must-revalidate";
            add_header X-Content-Type-Options "nosniff" always;
        }

        location = /README.md {
            root /share/aerial-config/docs;
            types { text/markdown md; }
            try_files /README.md /usr/share/nginx/html/fallback/README.md;
            add_header Cache-Control "no-cache, must-revalidate";
        }

        location = /_sidebar.md {
            root /share/aerial-config/docs;
            types { text/markdown md; }
            try_files /_sidebar.md /usr/share/nginx/html/fallback/_sidebar.md =404;
            add_header Cache-Control "no-cache, must-revalidate";
        }

        location = /_404.md {
            root /share/aerial-config/docs;
            types { text/markdown md; }
            try_files /_404.md /usr/share/nginx/html/fallback/_404.md;
            add_header Cache-Control "no-cache, must-revalidate";
        }

        location / {
            root /share/aerial-config/docs;
            types {
                text/markdown md;
                image/png png;
                image/jpeg jpg jpeg;
                image/svg+xml svg;
                image/webp webp;
                image/gif gif;
                application/json json;
                text/css css;
                application/javascript js;
            }
            try_files $uri /index.html;
            add_header Cache-Control "no-cache, must-revalidate";
            add_header X-Content-Type-Options "nosniff" always;
        }
    }
}
EOH
      }

      service {
        name     = "docs"
        port     = "http"
        provider = "nomad"

        check {
          name     = "docs-http"
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
        cpu    = 100
        memory = 128
      }
    }
  }
}
