{ config, lib, pkgs, ... }:
let
  cfg = config.aerial.k3s;
in
{
  options.aerial.k3s = {
    enable = lib.mkEnableOption "Aerial k3s configuration";
    role = lib.mkOption {
      type = lib.types.enum [ "server" "agent" ];
      default = "agent";
      description = "K3s node role (server or agent)";
    };
    serverAddr = lib.mkOption {
      type = lib.types.str;
      default = "https://k3s-server.lan:6443";
      description = "Address of the k3s server";
    };
    tokenFile = lib.mkOption {
      type = lib.types.path;
      default = "/etc/rancher/k3s/cluster-token";
      description = "Path to the k3s cluster join token file";
    };
    embeddedRegistry = lib.mkOption {
      type = lib.types.bool;
      default = true;
      description = "Enable Spegel embedded P2P OCI registry mirror (server only)";
    };
    nodeLabels = lib.mkOption {
      type = lib.types.listOf lib.types.str;
      default = [ ];
      description = "Node labels in key=value format";
    };
    nodeTaints = lib.mkOption {
      type = lib.types.listOf lib.types.str;
      default = [ ];
      description = "Node taints in key=value:effect format";
    };
    extraFlags = lib.mkOption {
      type = lib.types.listOf lib.types.str;
      default = [ ];
      description = "Additional CLI flags to pass to k3s";
    };
  };

  config = lib.mkIf cfg.enable {
    services.k3s = {
      enable = true;
      role = cfg.role;
      serverAddr = if cfg.role == "agent" then cfg.serverAddr else "";
      tokenFile = cfg.tokenFile;
      extraFlags = lib.concatStringsSep " " (
        (lib.optional (cfg.role == "server") "--disable=traefik")
        ++ (lib.optional (cfg.role == "server" && cfg.embeddedRegistry) "--embedded-registry")
        ++ (map (l: "--node-label ${l}") cfg.nodeLabels)
        ++ (map (t: "--node-taint ${t}") cfg.nodeTaints)
        ++ cfg.extraFlags
      );
    };
  };
}
