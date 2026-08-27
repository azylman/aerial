import { InMemoryEventStore } from '@modelcontextprotocol/sdk/examples/shared/inMemoryEventStore.js';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { isInitializeRequest } from '@modelcontextprotocol/sdk/types.js';
import { randomUUID } from 'crypto';
import express, { Request, Response } from 'express';
import * as schemas from './schemas.js';
import * as handlers from './tools.js';

function createMcpServer() {
    const server = new McpServer(
        { name: 'docker-mcp-server', version: '0.1.0' },
        { capabilities: { tools: {}, logging: {} } }
    );

    const toolMap = [
        { name: 'docker_list_containers', description: 'List Docker containers with names, status, state, image, and ports', schema: schemas.ListContainersSchema, handler: handlers.listContainersHandler },
        { name: 'docker_inspect_container', description: 'Inspect full container configuration, mounts, network settings, and state', schema: schemas.InspectContainerSchema, handler: handlers.inspectContainerHandler },
        { name: 'docker_get_container_logs', description: 'Fetch stdout and stderr logs for a Docker container', schema: schemas.GetContainerLogsSchema, handler: handlers.getContainerLogsHandler },
        { name: 'docker_container_stats', description: 'Fetch one-shot CPU, memory, and network I/O usage stats for a container', schema: schemas.ContainerStatsSchema, handler: handlers.containerStatsHandler },
        { name: 'docker_list_images', description: 'List local Docker images with repo tags, size, and creation date', schema: schemas.ListImagesSchema, handler: handlers.listImagesHandler },
        { name: 'docker_inspect_image', description: 'Inspect image metadata, layers, architecture, and environment configuration', schema: schemas.InspectImageSchema, handler: handlers.inspectImageHandler },
        { name: 'docker_list_networks', description: 'List Docker networks including bridge, host, and overlay networks', schema: schemas.ListNetworksSchema, handler: handlers.listNetworksHandler },
        { name: 'docker_inspect_network', description: 'Inspect network details including attached containers and IP assignments', schema: schemas.InspectNetworkSchema, handler: handlers.inspectNetworkHandler },
        { name: 'docker_system_info', description: 'Inspect Docker engine status, OS, server version, and total resources', schema: schemas.SystemInfoSchema, handler: handlers.systemInfoHandler },
        { name: 'docker_system_df', description: 'Inspect Docker disk usage across containers, images, volumes, and build cache', schema: schemas.SystemDfSchema, handler: handlers.systemDfHandler },
    ];

    for (const t of toolMap) {
        server.tool(
            t.name,
            t.description,
            t.schema.shape,
            async (args: any) => {
                return await t.handler(args);
            }
        );
    }

    return server;
}

const config = {
    TRANSPORT: (() => {
        const transportIndex = process.argv.indexOf('--transport');
        if (transportIndex !== -1 && transportIndex + 1 < process.argv.length) {
            return process.argv[transportIndex + 1];
        }
        return process.env.TRANSPORT || 'http';
    })(),
    HTTP_PORT: (() => {
        const portIndex = process.argv.indexOf('--port');
        if (portIndex !== -1 && portIndex + 1 < process.argv.length) {
            return parseInt(process.argv[portIndex + 1]);
        }
        return parseInt(process.env.PORT || '8080');
    })()
};

const app = express();
app.use(express.json());

const transports: { [sessionId: string]: StreamableHTTPServerTransport } = {};

const mcpPostHandler = async (req: Request, res: Response) => {
    const sessionId = req.headers['mcp-session-id'] as string;
    try {
        let transport: StreamableHTTPServerTransport;
        if (sessionId && transports[sessionId]) {
            transport = transports[sessionId];
        } else if (!sessionId && isInitializeRequest(req.body)) {
            const eventStore = new InMemoryEventStore();
            transport = new StreamableHTTPServerTransport({
                sessionIdGenerator: () => randomUUID(),
                eventStore,
                enableJsonResponse: true,
                onsessioninitialized: sid => {
                    transports[sid] = transport;
                    process.stderr.write(`Initialized new Docker MCP session: ${sid}\n`);
                }
            });

            transport.onclose = () => {
                const sid = transport.sessionId;
                if (sid && transports[sid]) {
                    delete transports[sid];
                    process.stderr.write(`Closed and removed Docker MCP session: ${sid}\n`);
                }
            };

            const sessionServer = createMcpServer();
            await sessionServer.connect(transport);
            await transport.handleRequest(req, res, req.body);
            return;
        } else {
            res.status(400).json({
                jsonrpc: "2.0",
                error: { code: -32000, message: "Invalid or missing MCP session ID." },
                id: req.body.id || null
            });
            return;
        }

        await transport.handleRequest(req, res, req.body);
    } catch (err) {
        process.stderr.write('Error handling Docker MCP request: ' + String(err) + '\n');
        if (!res.headersSent) {
            res.status(500).json({
                jsonrpc: "2.0",
                error: { code: -32000, message: "Internal server error: " + String(err) },
                id: req.body.id || null
            });
        }
    }
};

const mcpGetHandler = async (req: Request, res: Response) => {
    const sessionId = req.headers['mcp-session-id'] as string | undefined;
    if (!sessionId || !transports[sessionId]) {
        res.status(400).send('Invalid or missing session ID');
        return;
    }
    const transport = transports[sessionId];
    await transport.handleRequest(req, res);
};

const mcpDeleteHandler = async (req: Request, res: Response) => {
    const sessionId = req.headers['mcp-session-id'] as string | undefined;
    if (!sessionId || !transports[sessionId]) {
        res.status(400).send('Invalid or missing session ID');
        return;
    }
    try {
        const transport = transports[sessionId];
        await transport.handleRequest(req, res);
    } catch (error) {
        if (!res.headersSent) {
            res.status(500).send('Error processing session termination');
        }
    }
};

if (config.TRANSPORT.toLowerCase() === 'http') {
    app.post('/mcp', mcpPostHandler);
    app.get('/mcp', mcpGetHandler);
    app.delete('/mcp', mcpDeleteHandler);

    app.get('/health', (req, res) => {
        res.json({ status: 'ok', service: 'docker-mcp-server' });
    });

    const port = config.HTTP_PORT || 8080;
    app.listen(port, () => {
        process.stderr.write(`Docker MCP Server listening on port ${port}\n`);
    });
} else {
    const server = createMcpServer();
    const transport = new StdioServerTransport();
    server.connect(transport).then(() => {
        process.stderr.write('Docker MCP Stdio Server started.\n');
    });
}

