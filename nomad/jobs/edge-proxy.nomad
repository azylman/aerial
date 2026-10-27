job "edge-proxy" {
  datacenters = ["dc1"]
  type        = "service"

  # Target quiet-zero core server node
  node_pool   = "default"

  update {
    max_parallel      = 1
    canary            = 0
    min_healthy_time  = "5s"
    healthy_deadline  = "1m"
    progress_deadline = "2m"
    auto_revert       = true
  }

  group "edge-proxy" {
    count = 1

    network {
      mode = "host"
      port "http" {
        static = 4021
      }
    }

    task "edge-proxy" {
      driver = "docker"

      config {
        healthchecks {
          disable = true
        }
        dns_servers        = ["127.0.0.1"]
        dns_search_domains = ["lan", "aerial"]
        image        = "nginx:1.31-alpine"
        network_mode = "host"
        command      = "nginx"
        args         = ["-g", "daemon off;", "-c", "/local/nginx.conf"]
      }

      template {
        data = <<EOH
load_module modules/ngx_http_js_module.so;

events {
    worker_connections 1024;
}

http {
    include       /etc/nginx/mime.types;
    default_type  application/octet-stream;
    sendfile        on;
    keepalive_timeout  65;
    server_tokens   off;

    # Restore client IP from Cloudflare Tunnel
    set_real_ip_from 127.0.0.1;
    set_real_ip_from 10.0.0.0/8;
    set_real_ip_from 172.16.0.0/12;
    set_real_ip_from {{ "192." }}{{ "168.0.0/16" }};
{{- if nomadVarExists "nomad/jobs/edge-proxy" -}}
  {{- with nomadVar "nomad/jobs/edge-proxy" -}}
    {{- if .REAL_IP_FROM.Value }}
    {{ .REAL_IP_FROM.Value }}
    {{- end }}
  {{- end -}}
{{- else if nomadVarExists "nomad/jobs/aerial-edge-proxy" -}}
  {{- with nomadVar "nomad/jobs/aerial-edge-proxy" -}}
    {{- if .REAL_IP_FROM.Value }}
    {{ .REAL_IP_FROM.Value }}
    {{- end }}
  {{- end -}}
{{- end }}
    real_ip_header CF-Connecting-IP;

    # Connection upgrade mapping for WebSockets
    map $http_upgrade $connection_upgrade {
        default upgrade;
        ''      close;
    }

    # Forwarded protocol resolution for Cloudflare Tunnel
    map $http_x_forwarded_proto $forwarded_proto {
        default $http_x_forwarded_proto;
        ""      $scheme;
    }

    # DNS resolver for internal Nomad services via CoreDNS
    resolver 127.0.0.1 valid=10s ipv6=off;
    resolver_timeout 5s;

    js_import auth from /local/auth.js;
    include /local/upstreams.conf;

    # 1. Public GitHub Webhooks Entrypoint
    server {
        listen 4021 default_server;
        server_name {{ if nomadVarExists "nomad/jobs/edge-proxy" }}{{ with nomadVar "nomad/jobs/edge-proxy" }}{{ if .webhooks_domain.Value }}{{ .webhooks_domain.Value }}{{ else }}webhooks.*{{ end }}{{ end }}{{ else if nomadVarExists "nomad/jobs/aerial-edge-proxy" }}{{ with nomadVar "nomad/jobs/aerial-edge-proxy" }}{{ if .webhooks_domain.Value }}{{ .webhooks_domain.Value }}{{ else }}webhooks.*{{ end }}{{ end }}{{ else }}webhooks.*{{ end }};

        # Healthcheck endpoint for Nomad
        location = /health {
            return 200 '{"status":"healthy","service":"edge-proxy"}\n';
            add_header Content-Type application/json;
        }

        # Public GitHub webhook entrypoint
        location = /api/webhooks/github {
            # GitHub Webhook Official IP CIDRs
            allow 192.30.252.0/22;
            allow 185.199.108.0/22;
            allow 140.82.112.0/20;
            allow 143.55.64.0/20;
            allow 2a0a:a440::/29;
            allow 2606:50c0::/32;
            # Allow LAN / local testing
            allow 127.0.0.1;
            allow 10.0.0.0/8;
            allow 172.16.0.0/12;
            allow {{ "192." }}{{ "168.0.0/16" }};
{{- if nomadVarExists "nomad/jobs/edge-proxy" -}}
  {{- with nomadVar "nomad/jobs/edge-proxy" -}}
    {{- if .ALLOW_CIDRS.Value }}
            {{ .ALLOW_CIDRS.Value }}
    {{- end }}
  {{- end -}}
{{- else if nomadVarExists "nomad/jobs/aerial-edge-proxy" -}}
  {{- with nomadVar "nomad/jobs/aerial-edge-proxy" -}}
    {{- if .ALLOW_CIDRS.Value }}
            {{ .ALLOW_CIDRS.Value }}
    {{- end }}
  {{- end -}}
{{- end }}
            deny all;

            client_max_body_size 25m;
            client_body_buffer_size 16m;
            js_content auth.verifyAndProxy;
        }

        # Upstream proxy target for validated GitHub webhooks
        location @backend {
            proxy_pass http://webhooks_router;
            proxy_http_version 1.1;
            proxy_set_header Host webhooks-router.aerial;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $forwarded_proto;
            proxy_set_header X-GitHub-Delivery $http_x_github_delivery;
            proxy_set_header X-GitHub-Event $http_x_github_event;
        }

        location ^~ /api/webhooks/twilio/ {
            proxy_pass http://prospera;
            proxy_http_version 1.1;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $forwarded_proto;
            proxy_set_header X-Forwarded-Host $host;
            proxy_set_header Upgrade $http_upgrade;
            proxy_set_header Connection $connection_upgrade;
            proxy_buffering off;
            proxy_read_timeout 960s;
        }

        # Hard 404 for all other endpoints to prevent any route leakage
        location / {
            return 404 '{"error":"not found"}\n';
            add_header Content-Type application/json;
        }
    }

    # 2. Public Documentation Entrypoint (Reverse proxy to docs.aerial via mesh-proxy)
    server {
        listen 4021;
        server_name {{ if nomadVarExists "nomad/jobs/edge-proxy" }}{{ with nomadVar "nomad/jobs/edge-proxy" }}{{ if .docs_domain.Value }}{{ .docs_domain.Value }}{{ else }}docs.*{{ end }}{{ end }}{{ else if nomadVarExists "nomad/jobs/aerial-edge-proxy" }}{{ with nomadVar "nomad/jobs/aerial-edge-proxy" }}{{ if .docs_domain.Value }}{{ .docs_domain.Value }}{{ else }}docs.*{{ end }}{{ end }}{{ else }}docs.*{{ end }};

        location / {
            proxy_pass http://mesh_proxy;
            proxy_http_version 1.1;
            proxy_set_header Host docs.aerial;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $forwarded_proto;
            proxy_set_header X-Forwarded-Host $host;
            proxy_buffering off;
        }
    }

    # 3. Public Web Application Entrypoint (Reverse proxy to mesh-proxy)
    server {
        listen 4021;
        server_name {{ if nomadVarExists "nomad/jobs/edge-proxy" }}{{ with nomadVar "nomad/jobs/edge-proxy" }}{{ if .app_domain.Value }}{{ .app_domain.Value }}{{ else }}aerial.*{{ end }}{{ end }}{{ else if nomadVarExists "nomad/jobs/aerial-edge-proxy" }}{{ with nomadVar "nomad/jobs/aerial-edge-proxy" }}{{ if .app_domain.Value }}{{ .app_domain.Value }}{{ else }}aerial.*{{ end }}{{ end }}{{ else }}aerial.*{{ end }};

        location / {
            proxy_pass http://mesh_proxy;
            proxy_http_version 1.1;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $forwarded_proto;
            proxy_set_header X-Forwarded-Host $host;

            # WebSocket & SSE Support
            proxy_set_header Upgrade $http_upgrade;
            proxy_set_header Connection $connection_upgrade;
            proxy_buffering off;
            proxy_read_timeout 86400s;
        }
    }

    # 4. Public Browser Entrypoint (Reverse proxy to browser.aerial via mesh-proxy)
    server {
        listen 4021;
        server_name {{ if nomadVarExists "nomad/jobs/edge-proxy" }}{{ with nomadVar "nomad/jobs/edge-proxy" }}{{ if .browser_domain.Value }}{{ .browser_domain.Value }}{{ else }}browser.*{{ end }}{{ end }}{{ else if nomadVarExists "nomad/jobs/aerial-edge-proxy" }}{{ with nomadVar "nomad/jobs/aerial-edge-proxy" }}{{ if .browser_domain.Value }}{{ .browser_domain.Value }}{{ else }}browser.*{{ end }}{{ end }}{{ else }}browser.*{{ end }};

        location / {
            proxy_pass http://mesh_proxy;
            proxy_http_version 1.1;
            proxy_set_header Host browser.aerial;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $forwarded_proto;
            proxy_set_header X-Forwarded-Host $host;

            # WebSocket & SSE Support for live CDP and session viewer
            proxy_set_header Upgrade $http_upgrade;
            proxy_set_header Connection $connection_upgrade;
            proxy_buffering off;
            proxy_read_timeout 86400s;
        }
    }
}
EOH
        destination = "local/nginx.conf"
      }

      template {
        data = <<EOH
# Dynamic Nomad service upstreams rendered by Nomad template
upstream mesh_proxy {
{{- range nomadService "mesh-proxy" }}
    server {{ if contains ":" .Address }}[{{ .Address }}]{{ else }}{{ .Address }}{{ end }}:{{ .Port }};
{{- else }}
    server 127.0.0.1:65535 down;
{{- end }}
    keepalive 16;
}

upstream webhooks_router {
{{- range nomadService "webhooks-router" }}
    server {{ if contains ":" .Address }}[{{ .Address }}]{{ else }}{{ .Address }}{{ end }}:{{ .Port }};
{{- else }}
    server 127.0.0.1:65535 down;
{{- end }}
    keepalive 16;
}

upstream prospera {
{{- range nomadService "prospera" }}
    server {{ if contains ":" .Address }}[{{ .Address }}]{{ else }}{{ .Address }}{{ end }}:{{ .Port }};
{{- else }}
    server 127.0.0.1:65535 down;
{{- end }}
    keepalive 16;
}
EOH
        destination   = "local/upstreams.conf"
        change_mode   = "signal"
        change_signal = "SIGHUP"
      }

      template {
        data = <<EOH
import crypto from "crypto";

function timingSafeEqual(a, b) {
    if (!a || !b || a.length !== b.length) {
        return false;
    }
    let diff = 0;
    for (let i = 0; i < a.length; i++) {
        diff |= (a.charCodeAt(i) ^ b.charCodeAt(i));
    }
    return diff === 0;
}

function verifyAndProxy(r) {
    const signature = r.headersIn["X-Hub-Signature-256"];
    if (!signature) {
        r.return(401, '{"error":"missing X-Hub-Signature-256 header"}\n');
        return;
    }

    const secret = "{{ if nomadVarExists "nomad/jobs/edge-proxy" }}{{ with nomadVar "nomad/jobs/edge-proxy" }}{{ .hmac_key }}{{ end }}{{ else if nomadVarExists "nomad/jobs/aerial-edge-proxy" }}{{ with nomadVar "nomad/jobs/aerial-edge-proxy" }}{{ .hmac_key }}{{ end }}{{ else if nomadVarExists "nomad/jobs/webhooks-edge" }}{{ with nomadVar "nomad/jobs/webhooks-edge" }}{{ .hmac_key }}{{ end }}{{ end }}";
    if (!secret) {
        r.return(500, '{"error":"HMAC key not configured in nomad variables"}\n');
        return;
    }

    const hmac = crypto.createHmac("sha256", secret);
    hmac.update(r.requestText || "");
    const expected = "sha256=" + hmac.digest("hex");

    if (!timingSafeEqual(signature, expected)) {
        r.return(403, '{"error":"invalid HMAC signature"}\n');
        return;
    }

    r.internalRedirect("@backend");
}

export default { verifyAndProxy };
EOH
        destination = "local/auth.js"
      }

      service {
        name     = "edge-proxy"
        port     = "http"
        provider = "nomad"

        check {
          name     = "edge-proxy-health"
          type     = "http"
          path     = "/health"
          port     = "http"
          interval = "15s"
          timeout  = "3s"
          check_restart {
            limit           = 3
            grace           = "60s"
            ignore_warnings = false
          }
        }
      }

      # Backwards compatibility aliases for services discovering legacy service names
      service {
        name     = "aerial-edge-proxy"
        port     = "http"
        provider = "nomad"
      }

      service {
        name     = "webhooks-edge"
        port     = "http"
        provider = "nomad"
      }

      resources {
        cpu        = 100
        memory     = 128
        memory_max = 256
      }
    }
  }
}
