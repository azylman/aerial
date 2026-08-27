import { InMemoryEventStore } from '@modelcontextprotocol/sdk/examples/shared/inMemoryEventStore.js';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { isInitializeRequest, SetLevelRequestSchema } from '@modelcontextprotocol/sdk/types.js';
import { randomUUID } from 'crypto';
import { Client, GatewayIntentBits } from 'discord.js';
import { config as dotenvConfig } from 'dotenv';
import express, { Request, Response } from 'express';
import { info, Level, setLevel, warning } from './notifications.js';
import * as schemas from './schemas.js';
import * as handlers from './tools/tools.js';
import { createToolContext } from './tools/tools.js';

dotenvConfig();

function createMcpServer(client: Client) {
    const server = new McpServer(
        { name: 'discord-mcp-server', version: '0.3.0' },
        { capabilities: { tools: {}, logging: {} } }
    );
    const toolContext = createToolContext(client);
    (client as any).server = server.server;
    (server as any).discord = client;

    const toolMap: Array<{ name: string; schema?: any; handler: any }> = [
        { name: 'discord_login', schema: schemas.DiscordLoginSchema, handler: handlers.loginHandler },
        { name: 'discord_list_servers', schema: schemas.ListServersSchema, handler: handlers.listServersHandler },
        { name: 'discord_send', schema: schemas.SendMessageSchema, handler: handlers.sendMessageHandler },
        { name: 'discord_create_thread', schema: schemas.CreateThreadSchema, handler: handlers.createThreadHandler },
        { name: 'discord_get_server_info', schema: schemas.GetServerInfoSchema, handler: handlers.getServerInfoHandler },
        { name: 'discord_create_text_channel', schema: schemas.CreateTextChannelSchema, handler: handlers.createTextChannelHandler },
        { name: 'discord_delete_channel', schema: schemas.DeleteChannelSchema, handler: handlers.deleteChannelHandler },
        { name: 'discord_get_forum_channels', schema: schemas.GetForumChannelsSchema, handler: handlers.getForumChannelsHandler },
        { name: 'discord_create_forum_post', schema: schemas.CreateForumPostSchema, handler: handlers.createForumPostHandler },
        { name: 'discord_get_forum_post', schema: schemas.GetForumPostSchema, handler: handlers.getForumPostHandler },
        { name: 'discord_reply_to_forum', schema: schemas.ReplyToForumSchema, handler: handlers.replyToForumHandler },
        { name: 'discord_delete_forum_post', schema: schemas.DeleteForumPostSchema, handler: handlers.deleteForumPostHandler },
        { name: 'discord_search_messages', schema: schemas.SearchMessagesSchema, handler: handlers.searchMessagesHandler },
        { name: 'discord_read_messages', schema: schemas.ReadMessagesSchema, handler: handlers.readMessagesHandler },
        { name: 'discord_add_reaction', schema: schemas.AddReactionSchema, handler: handlers.addReactionHandler },
        { name: 'discord_add_multiple_reactions', schema: schemas.AddMultipleReactionsSchema, handler: handlers.addMultipleReactionsHandler },
        { name: 'discord_remove_reaction', schema: schemas.RemoveReactionSchema, handler: handlers.removeReactionHandler },
        { name: 'discord_get_reaction_users', schema: schemas.GetReactionUsersSchema, handler: handlers.getReactionUsersHandler },
        { name: 'discord_delete_message', schema: schemas.DeleteMessageSchema, handler: handlers.deleteMessageHandler },
        { name: 'discord_create_webhook', schema: schemas.CreateWebhookSchema, handler: handlers.createWebhookHandler },
        { name: 'discord_send_webhook_message', schema: schemas.SendWebhookMessageSchema, handler: handlers.sendWebhookMessageHandler },
        { name: 'discord_edit_webhook', schema: schemas.EditWebhookSchema, handler: handlers.editWebhookHandler },
        { name: 'discord_delete_webhook', schema: schemas.DeleteWebhookSchema, handler: handlers.deleteWebhookHandler },
        { name: 'discord_create_category', schema: schemas.CreateCategorySchema, handler: handlers.createCategoryHandler },
        { name: 'discord_edit_category', schema: schemas.EditCategorySchema, handler: handlers.editCategoryHandler },
        { name: 'discord_delete_category', schema: schemas.DeleteCategorySchema, handler: handlers.deleteCategoryHandler },
    ];

    for (const t of toolMap) {
        try {
            server.tool(
                t.name,
                t.schema ? t.schema.description ?? '' : '',
                t.schema ? t.schema.shape ?? t.schema : undefined, async (args: any) => {
                    return await t.handler(args, toolContext);
                });
        } catch (err) {
            warning(server.server, `Failed to register tool ${t.name}: ${String(err)}`);
        }
    }

    server.server.setRequestHandler(SetLevelRequestSchema, async (request) => {
        const levelParam = request.params.level as Level | undefined;
        const ok = setLevel(levelParam || 'info');
        if (!ok) {
            throw { code: -32602, message: 'Invalid log level' };
        }
        return {};
    });

    return server;
}

function createDiscordClient(token?: string) {
    const client = new Client({ intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildMessages, GatewayIntentBits.MessageContent] });
    if (token) {
        client.token = token;
        (async () => {
            try {
                await client.login(token);
                info((client as any).server, 'Successfully logged in to Discord');
            } catch (err: any) {
                warning((client as any).server, 'Discord login failed: ' + String(err));
            }
        })();
    }
    return client;
}

const config = {
    DISCORD_TOKEN: (() => {
        try {
            const tokenIndex = process.argv.indexOf('--config');
            if (tokenIndex !== -1 && tokenIndex + 1 < process.argv.length) {
                const configArg = process.argv[tokenIndex + 1];
                if (typeof configArg === 'string') {
                    try {
                        const parsedConfig = JSON.parse(configArg);
                        return parsedConfig.DISCORD_TOKEN;
                    } catch (err) {
                        return configArg;
                    }
                }
            }
            return process.env.DISCORD_TOKEN;
        } catch (err) {
            process.stderr.write('Error parsing configuration: ' + String(err) + '\n');
            return null;
        }
    })(),
    TRANSPORT: (() => {
        const transportIndex = process.argv.indexOf('--transport');
        if (transportIndex !== -1 && transportIndex + 1 < process.argv.length) {
            return process.argv[transportIndex + 1];
        }
        return 'stdio';
    })(),
    HTTP_PORT: (() => {
        const portIndex = process.argv.indexOf('--port');
        if (portIndex !== -1 && portIndex + 1 < process.argv.length) {
            return parseInt(process.argv[portIndex + 1]);
        }
        return 8080;
    })()
};

const discord = createDiscordClient(config.DISCORD_TOKEN);
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
                    process.stderr.write(`Initialized new MCP session: ${sid}\n`);
                }
            });

            transport.onclose = () => {
                const sid = transport.sessionId;
                if (sid && transports[sid]) {
                    delete transports[sid];
                    process.stderr.write(`Closed and removed MCP session: ${sid}\n`);
                }
            };

            // Multi-session support: create fresh McpServer instance connected to transport
            const sessionServer = createMcpServer(discord);
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
        process.stderr.write('Error handling MCP request: ' + String(err) + '\n');
        if (!res.headersSent) {
            res.status(500).json({
                jsonrpc: "2.0",
                error: { code: -32000, message: "Internal server error." },
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
        res.json({ status: 'ok', discordReady: discord.isReady() });
    });

    const port = config.HTTP_PORT || 8080;
    app.listen(port, () => {
        process.stderr.write(`Discord MCP Server listening on port ${port}\n`);
    });
} else {
    const server = createMcpServer(discord);
    const transport = new StdioServerTransport();
    server.connect(transport).then(() => {
        process.stderr.write('MCP Stdio Server started.\n');
    });
}

