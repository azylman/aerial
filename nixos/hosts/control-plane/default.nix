{ lib, ... }:
{
  imports = [
    ../../modules/base.nix
    ../../modules/k3s.nix
    ../../modules/disko.nix
    ../../modules/profiles/gpu-compute.nix
  ] ++ lib.optional (builtins.pathExists ./hardware-configuration.nix) ./hardware-configuration.nix;

  aerial.node = {
    hostName = "control-plane";
    bootloader = "grub";
  };

  aerial.diskDevice = "/dev/sda";

  aerial.k3s = {
    role = "server";
  };
}
