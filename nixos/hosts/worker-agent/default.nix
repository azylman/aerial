{ lib, ... }:
{
  imports = [
    ../../modules/base.nix
    ../../modules/k3s.nix
    ../../modules/disko.nix
    ../../modules/profiles/standard-agent.nix
  ] ++ lib.optional (builtins.pathExists ./hardware-configuration.nix) ./hardware-configuration.nix;

  aerial.node = {
    hostName = "worker-agent";
    bootloader = "grub";
  };

  aerial.diskDevice = "/dev/sda";
}
