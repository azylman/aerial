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

  systemd.services.k3s.path = with pkgs; [
    nvidia-container-toolkit.tools
    runc
  ];

  systemd.tmpfiles.rules = [
    "d /var/lib/rancher/k3s/agent/etc/containerd/config-v3.toml.d 0755 root root -"
    "L+ /var/lib/rancher/k3s/agent/etc/containerd/config-v3.toml.d/nvidia.toml - - - - ${pkgs.writeText "k3s-containerd-nvidia.toml" ''
      [plugins."io.containerd.cri.v1.runtime"]
        enable_cdi = true
        cdi_spec_dirs = ["/etc/cdi", "/var/run/cdi"]

        [plugins."io.containerd.cri.v1.runtime".containerd.runtimes.nvidia]
          runtime_type = "io.containerd.runc.v2"
          [plugins."io.containerd.cri.v1.runtime".containerd.runtimes.nvidia.options]
            BinaryName = "${pkgs.nvidia-container-toolkit.tools}/bin/nvidia-container-runtime.cdi"
            SystemdCgroup = true
    ''}"
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
