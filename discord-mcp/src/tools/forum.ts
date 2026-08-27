import { ToolHandler } from './types.js';
import { handleDiscordError } from "../errorHandler.js";
import {
  GetForumChannelsSchema,
  CreateForumPostSchema,
  GetForumPostSchema,
  ListForumThreadsSchema,
  ReplyToForumSchema,
  DeleteForumPostSchema,
  GetForumTagsSchema,
  SetForumTagsSchema,
  UpdateForumPostSchema
} from '../schemas.js';
import { ChannelType } from 'discord.js';

export const getForumChannelsHandler: ToolHandler = async (args, { client }) => {
  const { guildId } = GetForumChannelsSchema.parse(args);
  try {
    if (!client.isReady()) return { content: [{ type: "text", text: "Discord client not logged in." }], isError: true };
    const guild = await client.guilds.fetch(guildId);
    const forums = guild.channels.cache.filter(c => c.type === ChannelType.GuildForum).map(c => ({ id: c.id, name: c.name }));
    return { content: [{ type: "text", text: JSON.stringify(forums, null, 2) }] };
  } catch (error) { return handleDiscordError(error); }
};

export const createForumPostHandler: ToolHandler = async (args, { client }) => {
  const { forumChannelId, title, content, tags } = CreateForumPostSchema.parse(args);
  try {
    if (!client.isReady()) return { content: [{ type: "text", text: "Discord client not logged in." }], isError: true };
    const channel = await client.channels.fetch(forumChannelId);
    if (!channel || channel.type !== ChannelType.GuildForum) return { content: [{ type: "text", text: `Channel ${forumChannelId} is not a forum channel` }], isError: true };
    const thread = await (channel as any).threads.create({ name: title, message: { content }, appliedTags: tags });
    return { content: [{ type: "text", text: `Created forum post "${thread.name}" (ID: ${thread.id})` }] };
  } catch (error) { return handleDiscordError(error); }
};

export const getForumPostHandler: ToolHandler = async (args, { client }) => {
  const { threadId } = GetForumPostSchema.parse(args);
  try {
    if (!client.isReady()) return { content: [{ type: "text", text: "Discord client not logged in." }], isError: true };
    const thread = await client.channels.fetch(threadId);
    if (!thread || !thread.isThread()) return { content: [{ type: "text", text: `Thread ${threadId} not found` }], isError: true };
    const messages = await thread.messages.fetch({ limit: 50 });
    const data = { id: thread.id, name: thread.name, messageCount: thread.messageCount, messages: messages.map(m => ({ author: m.author.tag, content: m.content, createdAt: m.createdAt })) };
    return { content: [{ type: "text", text: JSON.stringify(data, null, 2) }] };
  } catch (error) { return handleDiscordError(error); }
};

export const listForumThreadsHandler: ToolHandler = async (args, { client }) => {
  const { forumChannelId } = ListForumThreadsSchema.parse(args);
  try {
    if (!client.isReady()) return { content: [{ type: "text", text: "Discord client not logged in." }], isError: true };
    const channel = await client.channels.fetch(forumChannelId);
    if (!channel || channel.type !== ChannelType.GuildForum) return { content: [{ type: "text", text: `Channel ${forumChannelId} is not a forum channel` }], isError: true };
    const active = await (channel as any).threads.fetchActive();
    const threads = active.threads.map((t: any) => ({ id: t.id, name: t.name, messageCount: t.messageCount }));
    return { content: [{ type: "text", text: JSON.stringify(threads, null, 2) }] };
  } catch (error) { return handleDiscordError(error); }
};

export const replyToForumHandler: ToolHandler = async (args, { client }) => {
  const { threadId, message } = ReplyToForumSchema.parse(args);
  try {
    if (!client.isReady()) return { content: [{ type: "text", text: "Discord client not logged in." }], isError: true };
    const thread = await client.channels.fetch(threadId);
    if (!thread || !thread.isThread()) return { content: [{ type: "text", text: `Thread ${threadId} not found` }], isError: true };
    const msg = await thread.send({ content: message });
    return { content: [{ type: "text", text: `Replied to thread ${threadId} with message ID: ${msg.id}` }] };
  } catch (error) { return handleDiscordError(error); }
};

export const deleteForumPostHandler: ToolHandler = async (args, { client }) => {
  const { threadId } = DeleteForumPostSchema.parse(args);
  try {
    if (!client.isReady()) return { content: [{ type: "text", text: "Discord client not logged in." }], isError: true };
    const thread = await client.channels.fetch(threadId);
    if (!thread || !thread.isThread()) return { content: [{ type: "text", text: `Thread ${threadId} not found` }], isError: true };
    await thread.delete();
    return { content: [{ type: "text", text: `Deleted forum thread ${threadId}` }] };
  } catch (error) { return handleDiscordError(error); }
};

export const getForumTagsHandler: ToolHandler = async (args, { client }) => {
  const { forumChannelId } = GetForumTagsSchema.parse(args);
  try {
    if (!client.isReady()) return { content: [{ type: "text", text: "Discord client not logged in." }], isError: true };
    const channel = await client.channels.fetch(forumChannelId);
    if (!channel || channel.type !== ChannelType.GuildForum) return { content: [{ type: "text", text: `Channel ${forumChannelId} is not a forum channel` }], isError: true };
    return { content: [{ type: "text", text: JSON.stringify((channel as any).availableTags, null, 2) }] };
  } catch (error) { return handleDiscordError(error); }
};

export const setForumTagsHandler: ToolHandler = async (args, { client }) => {
  const { forumChannelId, tags } = SetForumTagsSchema.parse(args);
  try {
    if (!client.isReady()) return { content: [{ type: "text", text: "Discord client not logged in." }], isError: true };
    const channel = await client.channels.fetch(forumChannelId);
    if (!channel || channel.type !== ChannelType.GuildForum) return { content: [{ type: "text", text: `Channel ${forumChannelId} is not a forum channel` }], isError: true };
    await (channel as any).setAvailableTags(tags);
    return { content: [{ type: "text", text: `Set available tags for forum ${forumChannelId}` }] };
  } catch (error) { return handleDiscordError(error); }
};

export const updateForumPostHandler: ToolHandler = async (args, { client }) => {
  const { threadId, title, appliedTags, archived, locked } = UpdateForumPostSchema.parse(args);
  try {
    if (!client.isReady()) return { content: [{ type: "text", text: "Discord client not logged in." }], isError: true };
    const thread = await client.channels.fetch(threadId);
    if (!thread || !thread.isThread()) return { content: [{ type: "text", text: `Thread ${threadId} not found` }], isError: true };
    await thread.edit({ name: title, appliedTags, archived, locked });
    return { content: [{ type: "text", text: `Updated forum post ${threadId}` }] };
  } catch (error) { return handleDiscordError(error); }
};

