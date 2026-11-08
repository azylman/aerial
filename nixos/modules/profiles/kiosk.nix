{ lib, ... }:
{
  hardware.graphics.enable = true;
  hardware.enableRedistributableFirmware = true;
  boot.kernelModules = [ "i2c-dev" ];
  boot.extraModprobeConfig = "options snd_hda_intel power_save=0 power_save_controller=N\n";

  systemd.services."getty@tty1".enable = false;

  hardware.bluetooth = {
    enable = true;
    settings.General = {
      ControllerMode = "le";
      Privacy = "device";
      JustWorksRepairing = "always";
      DiscoverableTimeout = 0;
    };
  };

  aerial.node.extraGroups = [ "video" "render" "audio" "input" ];

  aerial.k3s = {
    enable = lib.mkDefault true;
    role = lib.mkDefault "agent";
    embeddedRegistry = lib.mkDefault true;
    nodeLabels = [
      "role=kiosk"
    ];
    nodeTaints = [
      "role=kiosk:NoSchedule"
    ];
  };
}
