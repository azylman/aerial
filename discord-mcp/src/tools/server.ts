import { ToolHandler } from './types.js';
import { handleDiscordError } from "../errorHandler.js";
import { GetServerInfoSchema, ListServersSchema, SearchMessagesSchema } from '../schemas.js';

export const getServerInfoHandler: ToolHandler = async (args, { client }) => {
  const { guildId } = GetServerInfoSchema.parse(args);
  try {
    if (!client.isReady()) {
      return { content: [{ type: "text", text: "Discord client not logged in." }], isError: true };
    }
    const guild = await client.guilds.fetch(guildId);
    const info = {
      id: guild.id,
      name: guild.name,
      memberCount: guild.memberCount,
      ownerId: guild.ownerId,
      channels: guild.channels.cache.size,
      roles: guild.roles.cache.size
    };
    return { content: [{ type: "text", text: JSON.stringify(info, null, 2) }] };
  } catch (error) {
    return handleDiscordError(error);
  }
};

export const listServersHandler: ToolHandler = async (args, { client }) => {
  const { limit } = ListServersSchema.parse(args);
  try {
    if (!client.isReady()) {
      return { content: [{ type: "text", text: "Discord client not logged in." }], isError: true };
    }
    const guilds = client.guilds.cache.map(g => ({ id: g.id, name: g.name, memberCount: g.memberCount })).slice(0, limit);
    return { content: [{ type: "text", text: JSON.stringify(guilds, null, 2) }] };
  } catch (error) {
    return handleDiscordError(error);
  }
};

export const searchMessagesHandler: ToolHandler = async (args, { client }) => {
  const { channelId, content, limit } = SearchMessagesSchema.parse(args);
  try {
    if (!client.isReady()) {
      return { content: [{ type: "text", text: "Discord client not logged in." }], isError: true };
    }
    if (!channelId) {
      return { content: [{ type: "text", text: "channelId is required to fetch messages" }], isError: true };
    }
    const channel = await client.channels.fetch(channelId);
    if (!channel || !channel.isTextBased() || !('messages' in channel)) {
      return { content: [{ type: "text", text: `Channel ${channelId} is not text-based` }], isError: true };
    }
    const messages = await channel.messages.fetch({ limit: limit || 25 });
    const filtered = messages.filter(m => !content || m.content.toLowerCase().includes(content.toLowerCase()));
    const result = filtered.map(m => ({ id: m.id, author: m.author.tag, content: m.content, createdAt: m.createdAt }));
    return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
  } catch (error) {
    return handleDiscordError(error);
  }
};

