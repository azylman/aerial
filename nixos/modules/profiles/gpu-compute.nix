{ config, lib, pkgs, ... }:
{
  nixpkgs.config.allowUnfree = true;
  hardware.graphics.enable = true;
  services.xserver.videoDrivers = [ "nvidia" ];
  hardware.nvidia = {
    modesetting.enable = true;
    open = false;
    nvidiaSettings = false;
    package = config.boot.kernelPackages.nvidiaPackages.stable;
  };
  hardware.nvidia-container-toolkit.enable = true;

  virtualisation.docker = {
    enable = true;
    enableOnBoot = true;
    autoPrune.enable = true;
    logDriver = "json-file";
    daemon.settings = {
      "log-opts" = {
        "max-size" = "10m";
        "max-file" = "3";
      };
      runtimes.nvidia = {
        path = "${pkgs.nvidia-container-toolkit.tools}/bin/nvidia-container-runtime.cdi";
      };
    };
  };

  systemd.services.docker.path = with pkgs; [
    cifs-utils
    iptables
    nftables
    nvidia-container-toolkit.tools
  ];

  aerial.node.extraGroups = [ "docker" "video" "render" ];

  aerial.k3s = {
    enable = lib.mkDefault true;
    embeddedRegistry = lib.mkDefault true;
    nodeLabels = [
      "compute-tier=performance"
      "accelerator=nvidia-gpu"
    ];
  };
}
