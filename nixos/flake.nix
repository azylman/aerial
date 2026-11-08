{
  description = "Aerial Multi-Host NixOS Flake & Fleet Deployment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";
    disko = {
      url = "github:nix-community/disko";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    colmena = {
      url = "github:zhaofengli/colmena";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, disko, colmena, ... }@inputs:
  let
    defaultSystem = "x86_64-linux";
    sshKey = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIPlaceholderKeyForAerialClusterNodes00000000 aerial@cluster";
    specialArgs = { inherit inputs sshKey; };

    nodes = {
      control-plane = {
        targetHost = "192.0.2.10";
        system = "x86_64-linux";
        modules = [ ./hosts/control-plane ];
      };

      worker-agent = {
        targetHost = "192.0.2.11";
        system = "x86_64-linux";
        modules = [ ./hosts/worker-agent ];
      };

      kiosk-agent = {
        targetHost = "192.0.2.12";
        system = "x86_64-linux";
        modules = [ ./hosts/kiosk-agent ];
      };
    };

    hostNixosConfigs = nixpkgs.lib.mapAttrs (name: node:
      nixpkgs.lib.nixosSystem {
        system = node.system or defaultSystem;
        inherit specialArgs;
        modules = [
          disko.nixosModules.disko
        ] ++ node.modules;
      }
    ) nodes;

    installerConfigs = {
      netboot = nixpkgs.lib.nixosSystem {
        system = defaultSystem;
        inherit specialArgs;
        modules = [
          "${nixpkgs}/nixos/modules/installer/netboot/netboot-minimal.nix"
          ./modules/base.nix
          {
            aerial.node.bootloader = "none";
            hardware.enableRedistributableFirmware = true;
            networking.wireless.enable = false;
            networking.wireless.iwd.enable = false;
            networking.networkmanager.enable = false;
            networking.wireless.userControlled.enable = false;
            users.users.root.openssh.authorizedKeys.keys = [ sshKey ];
          }
        ];
      };

      cockpit-kexec = nixpkgs.lib.nixosSystem {
        system = defaultSystem;
        inherit specialArgs;
        modules = [
          "${nixpkgs}/nixos/modules/installer/netboot/netboot-minimal.nix"
          ./modules/base.nix
          {
            aerial.node.bootloader = "none";
            hardware.enableRedistributableFirmware = true;
            networking.wireless.enable = true;
            users.users.root.openssh.authorizedKeys.keys = [ sshKey ];
          }
        ];
      };
    };

    nodeNixpkgs = nixpkgs.lib.mapAttrs (name: node:
      import nixpkgs {
        system = node.system or defaultSystem;
        config.allowUnfree = true;
      }
    ) nodes;

    colmenaNodes = nixpkgs.lib.mapAttrs (name: node: {
      deployment.targetHost = node.targetHost;
      deployment.targetUser = "root";
      imports = [
        disko.nixosModules.disko
      ] ++ node.modules;
    }) nodes;
  in
  {
    nixosConfigurations = hostNixosConfigs // installerConfigs;

    colmenaHive = colmena.lib.makeHive ({
      meta = {
        nixpkgs = import nixpkgs {
          system = defaultSystem;
          config.allowUnfree = true;
        };
        inherit nodeNixpkgs specialArgs;
      };
    } // colmenaNodes);
  };
}
