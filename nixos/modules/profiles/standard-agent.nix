{ lib, ... }:
{
  aerial.k3s = {
    enable = lib.mkDefault true;
    role = lib.mkDefault "agent";
    embeddedRegistry = lib.mkDefault true;
    nodeLabels = [
      "compute-tier=standard"
      "storage-tier=nvme"
    ];
  };
}
