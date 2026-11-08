# Aerial NixOS Infrastructure & Starter Flake

This directory provides the generic, zero-PII NixOS starter flake, parameterized modules, composable hardware and role profiles, and example host configurations for deploying and managing cluster nodes.

---

## Directory Layout

```
nixos/
├── flake.nix                  # Unified nodes map generating nixosConfigurations & colmenaHive
├── README.md                  # Bootstrapping & management guide
├── modules/
│   ├── base.nix               # Base system options (options.aerial.node, options.aerial.diskDevice)
│   ├── k3s.nix                # K3s cluster node options (options.aerial.k3s)
│   ├── disko.nix              # Declarative GPT partition scheme (EF02 + 512M ESP + ext4 root)
│   └── profiles/
│       ├── standard-agent.nix # Standard K3s agent (NVMe storage, standard compute tier)
│       ├── gpu-compute.nix    # NVIDIA GPU compute node, Docker + CDI container runtime
│       └── kiosk.nix          # Touchscreen kiosk agent (audio, Bluetooth, Wayland display)
└── hosts/
    ├── control-plane/         # Example GPU compute K3s server
    ├── worker-agent/          # Example standard NVMe K3s agent
    └── kiosk-agent/           # Example touch kiosk K3s agent
```

---

## Declaring Nodes in `flake.nix`

All cluster nodes are defined centrally in the `nodes` map inside `nixos/flake.nix`. Both `nixosConfigurations` and `colmenaHive` are derived automatically from this mapping.

### Adding a New Node

To define a new host (supporting `x86_64-linux` or `aarch64-linux`):

```nix
nodes = {
  my-new-node = {
    targetHost = "192.0.2.20";
    system = "x86_64-linux"; # or "aarch64-linux"
    modules = [ ./hosts/my-new-node ];
  };
};
```

Each host configuration under `./hosts/<hostname>/default.nix` composes the modules and profiles:

```nix
{ lib, ... }:
{
  imports = [
    ../../modules/base.nix
    ../../modules/k3s.nix
    ../../modules/disko.nix
    ../../modules/profiles/standard-agent.nix
  ] ++ lib.optional (builtins.pathExists ./hardware-configuration.nix) ./hardware-configuration.nix;

  aerial.node = {
    hostName = "my-new-node";
    bootloader = "grub"; # or "systemd-boot"
  };

  aerial.diskDevice = "/dev/sda";
}
```

---

## Declarative Module Options

### `options.aerial.node` (`modules/base.nix`)
- `hostName` (`str`, default `""`): Node hostname.
- `timeZone` (`str`, default `"UTC"`): System timezone.
- `domain` (`str`, default `"lan"`): Local network search domain.
- `stateVersion` (`str`, default `"26.05"`): NixOS state version.
- `adminUser` (`str`, default `"aerial"`): Primary administrative username.
- `extraGroups` (`listOf str`, default `[ ]`): Additional group memberships for `adminUser`.
- `sshAuthorizedKeys` (`listOf str`, default `[ ]`): SSH public keys authorized for `adminUser` and `root`.
- `bootloader` (`enum [ "grub" "systemd-boot" "none" ]`, default `"grub"`): Configures either GRUB on `aerial.diskDevice` or systemd-boot with EFI variable updates.

### `options.aerial.diskDevice` (`modules/base.nix`)
- `diskDevice` (`str`, default `"/dev/sda"`): Target block device used by disko and GRUB.

### `options.aerial.k3s` (`modules/k3s.nix`)
- `enable` (`bool`, default `false`): Enable K3s service.
- `role` (`enum [ "server" "agent" ]`, default `"agent"`): K3s node role.
- `serverAddr` (`str`, default `"https://k3s-server.lan:6443"`): Upstream K3s server endpoint for agents.
- `tokenFile` (`path`, default `"/etc/rancher/k3s/cluster-token"`): Cluster join token file.
- `embeddedRegistry` (`bool`, default `true`): Enable Spegel embedded P2P OCI registry mirror across both servers and agents.
- `nodeLabels` (`listOf str`, default `[ ]`): Node labels passed to K3s (`--node-label key=value`).
- `nodeTaints` (`listOf str`, default `[ ]`): Node taints passed to K3s (`--node-taint key=value:effect`).
- `extraFlags` (`listOf str`, default `[ ]`): Additional command-line flags for K3s.

---

## Composable Hardware and Role Profiles

Import profiles from `modules/profiles/` into host definitions:
- **`standard-agent.nix`**: K3s agent with embedded Spegel P2P registry, labeled `compute-tier=standard` and `storage-tier=nvme`.
- **`gpu-compute.nix`**: NVIDIA proprietary graphics drivers, Container Toolkit (CDI), Docker engine, and K3s node labeled `compute-tier=performance` and `accelerator=nvidia-gpu`.
- **`kiosk.nix`**: Hardware accelerated graphics, audio, Bluetooth LE, disabled getty on tty1, and K3s agent labeled `role=kiosk` with taint `role=kiosk:NoSchedule`.

---

## Bootstrapping & Provisioning

### 1. Partitioning with `disko` / `nixos-anywhere`

To partition and provision a bare-metal machine over SSH using `nixos-anywhere`:

```bash
# Provision control-plane node
nix run github:nix-community/nixos-anywhere -- \
  --flake .#control-plane \
  --target-host root@192.0.2.10
```

`disko` formats the target `aerial.diskDevice` with:
- 1M BIOS boot partition (`EF02`)
- 512M EFI System Partition (`EF00`) mounted at `/boot`
- 100% ext4 root partition mounted at `/` with `noatime`

### 2. Fleet Deployment with `colmena`

Manage and apply updates across the fleet using `colmena`:

```bash
# Check evaluation and build fleet configurations
colmena build

# Apply changes to all nodes
colmena apply

# Target a specific node
colmena apply --on worker-agent
```
