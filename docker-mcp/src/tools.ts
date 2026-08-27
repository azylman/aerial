import { docker } from './dockerClient.js';

function cleanDockerLogs(buffer: Buffer | string): string {
    if (typeof buffer === 'string') return buffer;
    let offset = 0;
    let output = '';
    while (offset < buffer.length) {
        if (offset + 8 > buffer.length) {
            output += buffer.subarray(offset).toString('utf8');
            break;
        }
        const size = buffer.readUInt32BE(offset + 4);
        const frame = buffer.subarray(offset + 8, offset + 8 + size);
        output += frame.toString('utf8');
        offset += 8 + size;
    }
    return output;
}

export async function listContainersHandler(args: { all?: boolean; limit?: number; filters?: any }) {
    try {
        const raw = await docker.listContainers({
            all: args.all ?? true,
            limit: args.limit,
            filters: args.filters ? (typeof args.filters === 'string' ? args.filters : JSON.stringify(args.filters)) : undefined
        });
        const containers = raw.map(c => ({
            id: c.Id.substring(0, 12),
            names: c.Names.map(n => n.startsWith('/') ? n.substring(1) : n),
            image: c.Image,
            state: c.State,
            status: c.Status,
            created: new Date(c.Created * 1000).toISOString(),
            ports: c.Ports?.map(p => ({
                ip: p.IP,
                privatePort: p.PrivatePort,
                publicPort: p.PublicPort,
                type: p.Type
            })) || [],
            mounts: c.Mounts?.map(m => ({
                source: m.Source,
                destination: m.Destination,
                mode: m.Mode,
                rw: m.RW
            })) || []
        }));
        return {
            content: [{ type: 'text' as const, text: JSON.stringify(containers, null, 2) }]
        };
    } catch (err: any) {
        return {
            isError: true,
            content: [{ type: 'text' as const, text: `Failed to list containers: ${err.message || String(err)}` }]
        };
    }
}

export async function inspectContainerHandler(args: { id: string }) {
    try {
        const container = docker.getContainer(args.id);
        const info = await container.inspect();
        return {
            content: [{ type: 'text' as const, text: JSON.stringify(info, null, 2) }]
        };
    } catch (err: any) {
        return {
            isError: true,
            content: [{ type: 'text' as const, text: `Failed to inspect container ${args.id}: ${err.message || String(err)}` }]
        };
    }
}

export async function getContainerLogsHandler(args: { id: string; tail?: number; timestamps?: boolean; since?: number }) {
    try {
        const container = docker.getContainer(args.id);
        const logStream = await container.logs({
            stdout: true,
            stderr: true,
            tail: args.tail ?? 100,
            timestamps: args.timestamps ?? false,
            since: args.since
        });
        const cleaned = cleanDockerLogs(logStream as unknown as Buffer);
        return {
            content: [{ type: 'text' as const, text: cleaned }]
        };
    } catch (err: any) {
        return {
            isError: true,
            content: [{ type: 'text' as const, text: `Failed to get logs for container ${args.id}: ${err.message || String(err)}` }]
        };
    }
}

export async function containerStatsHandler(args: { id: string }) {
    try {
        const container = docker.getContainer(args.id);
        const stats: any = await container.stats({ stream: false });
        
        let cpuPercent = 0.0;
        if (stats.cpu_stats && stats.precpu_stats) {
            const cpuDelta = stats.cpu_stats.cpu_usage.total_usage - (stats.precpu_stats.cpu_usage?.total_usage || 0);
            const systemDelta = stats.cpu_stats.system_cpu_usage - (stats.precpu_stats.system_cpu_usage || 0);
            const onlineCpus = stats.cpu_stats.online_cpus || stats.cpu_stats.cpu_usage.percpu_usage?.length || 1;
            if (systemDelta > 0 && cpuDelta > 0) {
                cpuPercent = (cpuDelta / systemDelta) * onlineCpus * 100.0;
            }
        }

        const memUsage = stats.memory_stats?.usage || 0;
        const memLimit = stats.memory_stats?.limit || 0;
        const memPercent = memLimit > 0 ? (memUsage / memLimit) * 100.0 : 0;

        const simplified = {
            id: stats.id?.substring(0, 12) || args.id,
            name: stats.name?.startsWith('/') ? stats.name.substring(1) : stats.name,
            read: stats.read,
            cpu: {
                usagePercent: Math.round(cpuPercent * 100) / 100
            },
            memory: {
                usageBytes: memUsage,
                usageFormatted: `${Math.round((memUsage / (1024 * 1024)) * 100) / 100} MB`,
                limitBytes: memLimit,
                limitFormatted: `${Math.round((memLimit / (1024 * 1024 * 1024)) * 100) / 100} GB`,
                usagePercent: Math.round(memPercent * 100) / 100
            },
            network: stats.networks
        };

        return {
            content: [{ type: 'text' as const, text: JSON.stringify(simplified, null, 2) }]
        };
    } catch (err: any) {
        return {
            isError: true,
            content: [{ type: 'text' as const, text: `Failed to get stats for container ${args.id}: ${err.message || String(err)}` }]
        };
    }
}

export async function listImagesHandler(args: { all?: boolean }) {
    try {
        const raw = await docker.listImages({ all: args.all ?? false });
        const images = raw.map(img => ({
            id: img.Id.substring(0, 19),
            repoTags: img.RepoTags,
            size: `${Math.round((img.Size / (1024 * 1024)) * 100) / 100} MB`,
            created: new Date(img.Created * 1000).toISOString()
        }));
        return {
            content: [{ type: 'text' as const, text: JSON.stringify(images, null, 2) }]
        };
    } catch (err: any) {
        return {
            isError: true,
            content: [{ type: 'text' as const, text: `Failed to list images: ${err.message || String(err)}` }]
        };
    }
}

export async function inspectImageHandler(args: { id: string }) {
    try {
        const image = docker.getImage(args.id);
        const info = await image.inspect();
        return {
            content: [{ type: 'text' as const, text: JSON.stringify(info, null, 2) }]
        };
    } catch (err: any) {
        return {
            isError: true,
            content: [{ type: 'text' as const, text: `Failed to inspect image ${args.id}: ${err.message || String(err)}` }]
        };
    }
}

export async function listNetworksHandler(_args: any) {
    try {
        const raw = await docker.listNetworks();
        const networks = raw.map(n => ({
            id: n.Id.substring(0, 12),
            name: n.Name,
            driver: n.Driver,
            scope: n.Scope,
            internal: n.Internal
        }));
        return {
            content: [{ type: 'text' as const, text: JSON.stringify(networks, null, 2) }]
        };
    } catch (err: any) {
        return {
            isError: true,
            content: [{ type: 'text' as const, text: `Failed to list networks: ${err.message || String(err)}` }]
        };
    }
}

export async function inspectNetworkHandler(args: { id: string }) {
    try {
        const network = docker.getNetwork(args.id);
        const info = await network.inspect();
        return {
            content: [{ type: 'text' as const, text: JSON.stringify(info, null, 2) }]
        };
    } catch (err: any) {
        return {
            isError: true,
            content: [{ type: 'text' as const, text: `Failed to inspect network ${args.id}: ${err.message || String(err)}` }]
        };
    }
}

export async function systemInfoHandler(_args: any) {
    try {
        const info: any = await docker.info();
        const summary = {
            serverVersion: info.ServerVersion,
            operatingSystem: info.OperatingSystem,
            osType: info.OSType,
            architecture: info.Architecture,
            kernelVersion: info.KernelVersion,
            ncpu: info.NCPU,
            totalMemory: `${Math.round((info.MemTotal / (1024 * 1024 * 1024)) * 100) / 100} GB`,
            containers: {
                total: info.Containers,
                running: info.ContainersRunning,
                paused: info.ContainersPaused,
                stopped: info.ContainersStopped
            },
            images: info.Images,
            dockerRootDir: info.DockerRootDir
        };
        return {
            content: [{ type: 'text' as const, text: JSON.stringify(summary, null, 2) }]
        };
    } catch (err: any) {
        return {
            isError: true,
            content: [{ type: 'text' as const, text: `Failed to get system info: ${err.message || String(err)}` }]
        };
    }
}

export async function systemDfHandler(_args: any) {
    try {
        const df: any = await docker.df();
        const summary = {
            imagesCount: df.Images?.length || 0,
            containersCount: df.Containers?.length || 0,
            volumesCount: df.Volumes?.length || 0,
            buildCacheCount: df.BuildCache?.length || 0
        };
        return {
            content: [{ type: 'text' as const, text: JSON.stringify(summary, null, 2) }]
        };
    } catch (err: any) {
        return {
            isError: true,
            content: [{ type: 'text' as const, text: `Failed to get system disk usage: ${err.message || String(err)}` }]
        };
    }
}

