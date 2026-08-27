import { z } from 'zod';

export const ListContainersSchema = z.object({
    all: z.boolean().optional().describe('If true, list all containers including stopped ones. Default true.'),
    limit: z.number().optional().describe('Maximum number of containers to return.'),
    filters: z.record(z.any()).optional().describe('Optional key-value filters e.g. {"status": ["running"]}')
});

export const InspectContainerSchema = z.object({
    id: z.string().describe('Container name or ID to inspect')
});

export const GetContainerLogsSchema = z.object({
    id: z.string().describe('Container name or ID to fetch logs from'),
    tail: z.number().optional().describe('Number of lines from end of logs. Default 100.'),
    timestamps: z.boolean().optional().describe('Include timestamps in log output. Default false.'),
    since: z.number().optional().describe('Only return logs since this UNIX timestamp.')
});

export const ContainerStatsSchema = z.object({
    id: z.string().describe('Container name or ID to inspect resource stats for')
});

export const ListImagesSchema = z.object({
    all: z.boolean().optional().describe('If true, list all images including intermediate layers. Default false.')
});

export const InspectImageSchema = z.object({
    id: z.string().describe('Image tag, name, or ID to inspect')
});

export const ListNetworksSchema = z.object({});

export const InspectNetworkSchema = z.object({
    id: z.string().describe('Network name or ID to inspect')
});

export const SystemInfoSchema = z.object({});

export const SystemDfSchema = z.object({});

