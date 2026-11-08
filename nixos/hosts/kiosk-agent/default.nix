{ lib, ... }:
{
  imports = [
    ../../modules/base.nix
    ../../modules/k3s.nix
    ../../modules/disko.nix
    ../../modules/profiles/kiosk.nix
  ] ++ lib.optional (builtins.pathExists ./hardware-configuration.nix) ./hardware-configuration.nix;

  aerial.node = {
    hostName = "kiosk-agent";
    bootloader = "systemd-boot";
  };

  aerial.diskDevice = "/dev/nvme0n1";
}
