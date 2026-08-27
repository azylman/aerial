import { ToolHandler } from './types.js';
import { handleDiscordError } from "../errorHandler.js";
import { CreateWebhookSchema, SendWebhookMessageSchema, EditWebhookSchema, DeleteWebhookSchema } from '../schemas.js';
import { WebhookClient } from 'discord.js';

export const createWebhookHandler: ToolHandler = async (args, { client }) => {
  const { channelId, name, avatar, reason } = CreateWebhookSchema.parse(args);
  try {
    if (!client.isReady()) {
      return { content: [{ type: "text", text: "Discord client not logged in." }], isError: true };
    }
    const channel = await client.channels.fetch(channelId);
    if (!channel || !('createWebhook' in channel)) {
      return { content: [{ type: "text", text: `Channel ${channelId} cannot create webhooks` }], isError: true };
    }
    const webhook = await (channel as any).createWebhook({ name, avatar, reason });
    return { content: [{ type: "text", text: `Created webhook: ${webhook.name} (ID: ${webhook.id}, URL: ${webhook.url})` }] };
  } catch (error) {
    return handleDiscordError(error);
  }
};

export const sendWebhookMessageHandler: ToolHandler = async (args, { client }) => {
  const { webhookId, webhookToken, content, username, avatarURL, threadId } = SendWebhookMessageSchema.parse(args);
  try {
    const webhook = new WebhookClient({ id: webhookId, token: webhookToken });
    const msg = await webhook.send({ content, username, avatarURL, threadId });
    return { content: [{ type: "text", text: `Sent webhook message ID: ${msg.id}` }] };
  } catch (error) {
    return handleDiscordError(error);
  }
};

export const editWebhookHandler: ToolHandler = async (args, { client }) => {
  const { webhookId, webhookToken, name, avatar, channelId, reason } = EditWebhookSchema.parse(args);
  try {
    const webhook = new WebhookClient({ id: webhookId, token: webhookToken || '' });
    await webhook.edit({ name, avatar, channel: channelId, reason });
    return { content: [{ type: "text", text: `Successfully edited webhook ${webhookId}` }] };
  } catch (error) {
    return handleDiscordError(error);
  }
};

export const deleteWebhookHandler: ToolHandler = async (args, { client }) => {
  const { webhookId, webhookToken, reason } = DeleteWebhookSchema.parse(args);
  try {
    const webhook = new WebhookClient({ id: webhookId, token: webhookToken || '' });
    await webhook.delete(reason);
    return { content: [{ type: "text", text: `Successfully deleted webhook ${webhookId}` }] };
  } catch (error) {
    return handleDiscordError(error);
  }
};

