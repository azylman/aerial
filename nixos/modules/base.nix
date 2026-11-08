{ config, lib, pkgs, sshKey ? null, ... }:
let
  cfg = config.aerial.node;
in
{
  options.aerial.diskDevice = lib.mkOption {
    type = lib.types.str;
    default = "/dev/sda";
    description = "Target block device for disko partitioning and GRUB installation";
  };

  options.aerial.node = {
    hostName = lib.mkOption {
      type = lib.types.str;
      default = "";
      description = "Node hostname";
    };

    timeZone = lib.mkOption {
      type = lib.types.str;
      default = "UTC";
      description = "Node timezone";
    };

    domain = lib.mkOption {
      type = lib.types.str;
      default = "lan";
      description = "Node domain";
    };

    stateVersion = lib.mkOption {
      type = lib.types.str;
      default = "26.05";
      description = "NixOS release state version";
    };

    adminUser = lib.mkOption {
      type = lib.types.str;
      default = "aerial";
      description = "Primary admin username";
    };

    extraGroups = lib.mkOption {
      type = lib.types.listOf lib.types.str;
      default = [ ];
      description = "Additional groups for primary admin user";
    };

    sshAuthorizedKeys = lib.mkOption {
      type = lib.types.listOf lib.types.str;
      default = lib.optional (sshKey != null) sshKey;
      description = "SSH authorized public keys for admin user and root";
    };

    bootloader = lib.mkOption {
      type = lib.types.enum [ "grub" "systemd-boot" "none" ];
      default = "grub";
      description = "Bootloader backend for node";
    };
  };

  config = {
    time.timeZone = cfg.timeZone;

    networking = {
      hostName = lib.mkIf (cfg.hostName != "") cfg.hostName;
      domain = cfg.domain;
      useDHCP = lib.mkDefault true;
      firewall.enable = false;
    };
    services.resolved.enable = false;

    services.openssh = {
      enable = true;
      settings = {
        PermitRootLogin = "prohibit-password";
        PasswordAuthentication = false;
      };
    };

    security.sudo.wheelNeedsPassword = false;

    boot.loader = {
      grub = lib.mkIf (cfg.bootloader == "grub") {
        enable = true;
        device = config.aerial.diskDevice;
        efiSupport = true;
        efiInstallAsRemovable = true;
      };
      systemd-boot = lib.mkIf (cfg.bootloader == "systemd-boot") {
        enable = true;
      };
      efi = lib.mkIf (cfg.bootloader == "systemd-boot") {
        canTouchEfiVariables = true;
      };
    };

    users.users = {
      ${cfg.adminUser} = {
        isNormalUser = true;
        extraGroups = lib.unique ([ "wheel" ] ++ cfg.extraGroups);
        openssh.authorizedKeys.keys = cfg.sshAuthorizedKeys;
      };
      root = {
        openssh.authorizedKeys.keys = cfg.sshAuthorizedKeys;
      };
    };

    environment.systemPackages = with pkgs; [
      cifs-utils
      curl
      git
      htop
      jq
      pciutils
      rsync
      vim
    ];

    system.stateVersion = cfg.stateVersion;
  };
}
