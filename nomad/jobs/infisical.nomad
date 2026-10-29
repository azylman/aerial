job "infisical" {
  datacenters = ["dc1"]
  type        = "service"

  node_pool   = "default"

  update {
    max_parallel      = 1
    canary            = 0
    min_healthy_time  = "10s"
    healthy_deadline  = "3m"
    progress_deadline = "5m"
    auto_revert       = true
  }

  group "infisical" {
    count = 1

    network {
      mode = "host"
      port "http" {}
      port "redis" {}
    }

    restart {
      attempts = 5
      delay    = "15s"
      interval = "10m"
      mode     = "fail"
    }

    # Task 1: Redis cache & session backend
    task "infisical-redis" {
      driver = "docker"

      lifecycle {
        hook    = "prestart"
        sidecar = true
      }

      config {
        dns_servers        = ["127.0.0.1"]
        dns_search_domains = ["aerial"]
        image        = "valkey/valkey:9-alpine"
        network_mode = "host"
        healthchecks {
          disable = true
        }
        args         = ["--port", "${NOMAD_PORT_redis}", "--bind", "0.0.0.0", "--protected-mode", "no", "--save", ""]
      }

      service {
        name     = "infisical-redis"
        port     = "redis"
        provider = "nomad"

        check {
          name     = "redis-tcp-ping"
          type     = "tcp"
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
        cpu        = 100
        memory     = 64
        memory_max = 128
      }
    }

    # Task 2: Infisical secret management core server
    task "infisical" {
      driver = "docker"

      config {
        dns_servers        = ["127.0.0.1"]
        dns_search_domains = ["aerial"]
        image        = "infisical/infisical:v0.167.4"
        network_mode = "host"
        healthchecks {
          disable = true
        }
      }

      env {
        PORT                          = "${NOMAD_PORT_http}"
        HOST                          = "0.0.0.0"
        REDIS_URL                     = "redis://127.0.0.1:${NOMAD_PORT_redis}"
        SITE_URL                      = "http://infisical.aerial"
        ALLOW_INTERNAL_IP_CONNECTIONS = "true"
        NODE_OPTIONS                  = "--import /local/auto-auth.mjs"
      }

      template {
        data = <<EOH
import { registerHooks } from "node:module";
import fs from "node:fs";
import { fileURLToPath } from "node:url";

registerHooks({
  load(url, context, nextLoad) {
    if (url.endsWith("/server/routes/v1/auth-router.mjs")) {
      const orig = fs.readFileSync(fileURLToPath(url), "utf8");
      const target = `\t\t\t} catch (err) {
\t\t\t\tres.cookie("jid", "", {
\t\t\t\t\thttpOnly: true,
\t\t\t\t\tpath: "/api",
\t\t\t\t\tsameSite: "strict",
\t\t\t\t\tsecure: appCfg.HTTPS_ENABLED,
\t\t\t\t\tmaxAge: 0
\t\t\t\t});
\t\t\t\tthrow err;
\t\t\t}`;
      const replacement = `\t\t\t} catch (err) {
\t\t\t\tconst adminUser = await server.store.user.findOne({ superAdmin: true });
\t\t\t\tconst userOrgs = adminUser ? await server.services.org.findAllOrganizationOfUser(adminUser.id) : [];
\t\t\t\tif (!adminUser || !userOrgs.length) {
\t\t\t\t\tres.cookie("jid", "", {
\t\t\t\t\t\thttpOnly: true,
\t\t\t\t\t\tpath: "/api",
\t\t\t\t\t\tsameSite: "strict",
\t\t\t\t\t\tsecure: appCfg.HTTPS_ENABLED,
\t\t\t\t\t\tmaxAge: 0
\t\t\t\t\t});
\t\t\t\t\tthrow err;
\t\t\t\t}
\t\t\t\ttokenVersion = await server.services.authToken.getUserTokenSession({
\t\t\t\t\tuserId: adminUser.id,
\t\t\t\t\tip: req.realIp || "127.0.0.1",
\t\t\t\t\tuserAgent: req.headers["user-agent"] || "infisical-lan"
\t\t\t\t});
\t\t\t\tdecodedToken = {
\t\t\t\t\tauthMethod: "email",
\t\t\t\t\tauthTokenType: "refreshToken",
\t\t\t\t\tuserId: adminUser.id,
\t\t\t\t\ttokenVersionId: tokenVersion.id,
\t\t\t\t\trefreshVersion: tokenVersion.refreshVersion,
\t\t\t\t\torganizationId: userOrgs[0].id,
\t\t\t\t\tisMfaVerified: true
\t\t\t\t};
\t\t\t\tisGraceHit = false;
\t\t\t}`;
      if (orig.includes(target)) {
        return { format: "module", shortCircuit: true, source: orig.replace(target, replacement) };
      }
    }
    return nextLoad(url, context);
  }
});
EOH
        destination = "local/auto-auth.mjs"
      }

      template {
        data = <<EOH
{{ if nomadVarExists "nomad/jobs/infisical" }}
{{ with nomadVar "nomad/jobs/infisical" }}
ENCRYPTION_KEY="{{ .ENCRYPTION_KEY }}"
AUTH_SECRET="{{ .AUTH_SECRET }}"
DB_CONNECTION_URI="postgresql://aerial:{{ .DB_PASSWORD }}@postgres:5432/infisical?sslmode=disable"
{{ end }}
{{ end }}
EOH
        destination = "secrets/infisical.env"
        env         = true
      }

      service {
        name     = "infisical"
        port     = "http"
        provider = "nomad"

        check {
          name     = "infisical-health"
          type     = "http"
          path     = "/api/status"
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
        cpu        = 500
        memory     = 1024
        memory_max = 2048
      }
    }
  }
}
