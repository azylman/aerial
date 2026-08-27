import { DiscordLoginSchema } from '../schemas.js';
import { ToolHandler } from './types.js';
import { handleDiscordError } from "../errorHandler.js";
import { info, error } from '../logger.js';
import { Client } from 'discord.js';

async function waitForReady(client: Client, token: string, timeoutMs = 30000): Promise<Client> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error(`Client ready event timed out after ${timeoutMs}ms`));
    }, timeoutMs);
    
    if (client.isReady()) {
      clearTimeout(timeout);
      resolve(client);
      return;
    }
    
    const readyHandler = () => {
      info('Client ready event received');
      clearTimeout(timeout);
      resolve(client);
    };
    
    const errorHandler = (err: Error) => {
      clearTimeout(timeout);
      client.removeListener('ready', readyHandler);
      reject(err);
    };
    
    client.once('ready', readyHandler);
    client.once('error', errorHandler);
    
    info('Starting login process and waiting for ready event');
    client.login(token).catch((err: Error) => {
      clearTimeout(timeout);
      client.removeListener('ready', readyHandler);
      client.removeListener('error', errorHandler);
      reject(err);
    });
  });
}

export const loginHandler: ToolHandler = async (args, context) => {
  DiscordLoginSchema.parse(args);
  try {
        if (context.client.isReady()) {
          return {
            content: [{ type: "text", text: `Already logged in as: ${context.client.user?.tag}` }]
          };
        }
        
        const token = args.token || context.client.token;
        if (!token) {
          return {
            content: [{ type: "text", text: "Discord token not provided and not configured." }],
            isError: true
          };
        }
        
        if (args.token) {
          context.client.token = args.token;
        }
        
        const readyClient = await waitForReady(context.client, token);
        context.client = readyClient;
        return {
          content: [{ type: "text", text: `Successfully logged in to Discord: ${context.client.user?.tag}` }]
        };
  } catch (err) {
    error(`Error in login handler: ${err instanceof Error ? err.message : String(err)}`);
    return handleDiscordError(err);
  }
};

