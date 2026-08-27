import { z } from "zod";
import { ChannelType, PermissionsBitField } from "discord.js";
import { ToolContext, ToolResponse } from "./types.js";
import {
  CreateTextChannelSchema,
  CreateForumChannelSchema,
  EditChannelSchema,
  DeleteChannelSchema,
  ReadMessagesSchema,
  CreateCategorySchema,
  EditCategorySchema,
  DeleteCategorySchema,
  SetChannelPermissionsSchema,
  RemoveChannelPermissionsSchema,
  CreateVoiceChannelSchema
} from "../schemas.js";
import { handleDiscordError } from "../errorHandler.js";
import { resolveSnowflakeOrDate } from "../utils/snowflake.js";

export async function createCategoryHandler(
  args: unknown,
  context: ToolContext
): Promise<ToolResponse> {
  const { guildId, name, position, reason } = CreateCategorySchema.parse(args);
  try {
    if (!context.client.isReady()) {
      return {
        content: [{ type: "text", text: "Discord client not logged in." }],
        isError: true
      };
    }
    const guild = await context.client.guilds.fetch(guildId);
    if (!guild) {
      return {
        content: [{ type: "text", text: `Cannot find guild with ID: ${guildId}` }],
        isError: true
      };
    }
    const channel = await guild.channels.create({
      name,
      type: ChannelType.GuildCategory,
      position,
      reason
    });
    return {
      content: [{ type: "text", text: `Successfully created category "${channel.name}" (ID: ${channel.id})` }]
    };
  } catch (error) {
    return handleDiscordError(error);
  }
}

export async function editCategoryHandler(
  args: unknown,
  context: ToolContext
): Promise<ToolResponse> {
  const { categoryId, name, position, reason } = EditCategorySchema.parse(args);
  try {
    if (!context.client.isReady()) {
      return {
        content: [{ type: "text", text: "Discord client not logged in." }],
        isError: true
      };
    }
    const channel = await context.client.channels.fetch(categoryId);
    if (!channel || channel.type !== ChannelType.GuildCategory) {
      return {
        content: [{ type: "text", text: `Cannot find category with ID: ${categoryId}` }],
        isError: true
      };
    }
    const editOptions: any = {};
    if (name) editOptions.name = name;
    if (position !== undefined) editOptions.position = position;
    if (reason) editOptions.reason = reason;
    await channel.edit(editOptions);
    return {
      content: [{ type: "text", text: `Successfully edited category (ID: ${categoryId})` }]
    };
  } catch (error) {
    return handleDiscordError(error);
  }
}

export async function deleteCategoryHandler(
  args: unknown,
  context: ToolContext
): Promise<ToolResponse> {
  const { categoryId, reason } = DeleteCategorySchema.parse(args);
  try {
    if (!context.client.isReady()) {
      return {
        content: [{ type: "text", text: "Discord client not logged in." }],
        isError: true
      };
    }
    const channel = await context.client.channels.fetch(categoryId);
    if (!channel || channel.type !== ChannelType.GuildCategory) {
      return {
        content: [{ type: "text", text: `Cannot find category with ID: ${categoryId}` }],
        isError: true
      };
    }
    await channel.delete(reason);
    return {
      content: [{ type: "text", text: `Successfully deleted category (ID: ${categoryId})` }]
    };
  } catch (error) {
    return handleDiscordError(error);
  }
}

export async function createTextChannelHandler(
  args: unknown,
  context: ToolContext
): Promise<ToolResponse> {
  const { guildId, channelName, topic, categoryId, reason } = CreateTextChannelSchema.parse(args);
  try {
    if (!context.client.isReady()) {
      return {
        content: [{ type: "text", text: "Discord client not logged in." }],
        isError: true
      };
    }
    const guild = await context.client.guilds.fetch(guildId);
    if (!guild) {
      return {
        content: [{ type: "text", text: `Cannot find guild with ID: ${guildId}` }],
        isError: true
      };
    }
    const channel = await guild.channels.create({
      name: channelName,
      type: ChannelType.GuildText,
      topic,
      parent: categoryId,
      reason
    });
    return {
      content: [{ type: "text", text: `Successfully created text channel "${channel.name}" (ID: ${channel.id})` }]
    };
  } catch (error) {
    return handleDiscordError(error);
  }
}

export async function createForumChannelHandler(
  args: unknown,
  context: ToolContext
): Promise<ToolResponse> {
  const { guildId, name, topic, categoryId, reason } = CreateForumChannelSchema.parse(args);
  try {
    if (!context.client.isReady()) {
      return {
        content: [{ type: "text", text: "Discord client not logged in." }],
        isError: true
      };
    }
    const guild = await context.client.guilds.fetch(guildId);
    if (!guild) {
      return {
        content: [{ type: "text", text: `Cannot find guild with ID: ${guildId}` }],
        isError: true
      };
    }
    const channel = await guild.channels.create({
      name,
      type: ChannelType.GuildForum,
      topic,
      parent: categoryId,
      reason
    });
    return {
      content: [{ type: "text", text: `Successfully created forum channel "${channel.name}" (ID: ${channel.id})` }]
    };
  } catch (error) {
    return handleDiscordError(error);
  }
}

export async function createVoiceChannelHandler(
  args: unknown,
  context: ToolContext
): Promise<ToolResponse> {
  const { guildId, name, categoryId, bitrate, userLimit, position, reason } = CreateVoiceChannelSchema.parse(args);
  try {
    if (!context.client.isReady()) {
      return {
        content: [{ type: "text", text: "Discord client not logged in." }],
        isError: true
      };
    }
    const guild = await context.client.guilds.fetch(guildId);
    if (!guild) {
      return {
        content: [{ type: "text", text: `Cannot find guild with ID: ${guildId}` }],
        isError: true
      };
    }
    const channel = await guild.channels.create({
      name,
      type: ChannelType.GuildVoice,
      parent: categoryId,
      bitrate,
      userLimit,
      position,
      reason
    });
    return {
      content: [{ type: "text", text: `Successfully created voice channel "${channel.name}" (ID: ${channel.id})` }]
    };
  } catch (error) {
    return handleDiscordError(error);
  }
}

export async function editChannelHandler(
  args: unknown,
  context: ToolContext
): Promise<ToolResponse> {
  const { channelId, name, topic, parentId, position, reason } = EditChannelSchema.parse(args);
  try {
    if (!context.client.isReady()) {
      return {
        content: [{ type: "text", text: "Discord client not logged in." }],
        isError: true
      };
    }
    const channel = await context.client.channels.fetch(channelId);
    if (!channel || !('edit' in channel)) {
      return {
        content: [{ type: "text", text: `Cannot find editable channel with ID: ${channelId}` }],
        isError: true
      };
    }
    const editOptions: any = {};
    if (name) editOptions.name = name;
    if (topic !== undefined) editOptions.topic = topic;
    if (parentId !== undefined) editOptions.parent = parentId;
    if (position !== undefined) editOptions.position = position;
    if (reason) editOptions.reason = reason;
    await (channel as any).edit(editOptions);
    return {
      content: [{ type: "text", text: `Successfully edited channel (ID: ${channelId})` }]
    };
  } catch (error) {
    return handleDiscordError(error);
  }
}

export async function deleteChannelHandler(
  args: unknown,
  context: ToolContext
): Promise<ToolResponse> {
  const { channelId, reason } = DeleteChannelSchema.parse(args);
  try {
    if (!context.client.isReady()) {
      return {
        content: [{ type: "text", text: "Discord client not logged in." }],
        isError: true
      };
    }
    const channel = await context.client.channels.fetch(channelId);
    if (!channel) {
      return {
        content: [{ type: "text", text: `Cannot find channel with ID: ${channelId}` }],
        isError: true
      };
    }
    await channel.delete(reason);
    return {
      content: [{ type: "text", text: `Successfully deleted channel (ID: ${channelId})` }]
    };
  } catch (error) {
    return handleDiscordError(error);
  }
}

export async function setChannelPermissionsHandler(
  args: unknown,
  context: ToolContext
): Promise<ToolResponse> {
  const { channelId, targetId, targetType, allow, deny, reason } = SetChannelPermissionsSchema.parse(args);
  try {
    if (!context.client.isReady()) {
      return {
        content: [{ type: "text", text: "Discord client not logged in." }],
        isError: true
      };
    }
    const channel = await context.client.channels.fetch(channelId);
    if (!channel || !('permissionOverwrites' in channel)) {
      return {
        content: [{ type: "text", text: `Cannot find channel with permissions support: ${channelId}` }],
        isError: true
      };
    }
    const allowBits = (allow || []).reduce((acc: bigint, p: string) => acc | ((PermissionsBitField.Flags as any)[p] || 0n), 0n);
    const denyBits = (deny || []).reduce((acc: bigint, p: string) => acc | ((PermissionsBitField.Flags as any)[p] || 0n), 0n);
    await (channel as any).permissionOverwrites.create(targetId, {
      ...(allowBits && { allow: allowBits }),
      ...(denyBits && { deny: denyBits })
    }, { reason, type: targetType === 'role' ? 0 : 1 });
    return {
      content: [{ type: "text", text: `Successfully set permissions for ${targetType} ${targetId} on channel ${channelId}` }]
    };
  } catch (error) {
    return handleDiscordError(error);
  }
}

export async function removeChannelPermissionsHandler(
  args: unknown,
  context: ToolContext
): Promise<ToolResponse> {
  const { channelId, targetId, reason } = RemoveChannelPermissionsSchema.parse(args);
  try {
    if (!context.client.isReady()) {
      return {
        content: [{ type: "text", text: "Discord client not logged in." }],
        isError: true
      };
    }
    const channel = await context.client.channels.fetch(channelId);
    if (!channel || !('permissionOverwrites' in channel)) {
      return {
        content: [{ type: "text", text: `Cannot find channel with permissions support: ${channelId}` }],
        isError: true
      };
    }
    await (channel as any).permissionOverwrites.delete(targetId, reason);
    return {
      content: [{ type: "text", text: `Successfully removed permissions for ${targetId} on channel ${channelId}` }]
    };
  } catch (error) {
    return handleDiscordError(error);
  }
}

export async function readMessagesHandler(
  args: unknown,
  context: ToolContext
): Promise<ToolResponse> {
  const { channelId, limit, before, after, around } = ReadMessagesSchema.parse(args);
  try {
    if (!context.client.isReady()) {
      return {
        content: [{ type: "text", text: "Discord client not logged in." }],
        isError: true
      };
    }
    const channel = await context.client.channels.fetch(channelId);
    if (!channel || !channel.isTextBased() || !('messages' in channel)) {
      return {
        content: [{ type: "text", text: `Cannot find text channel with ID: ${channelId}` }],
        isError: true
      };
    }
    const fetchOptions: any = { limit: limit || 50 };
    if (before) fetchOptions.before = resolveSnowflakeOrDate(before);
    if (after) fetchOptions.after = resolveSnowflakeOrDate(after);
    if (around) fetchOptions.around = resolveSnowflakeOrDate(around);
    const messages = await channel.messages.fetch(fetchOptions);
    const msgArray = Array.isArray(messages) ? messages : ('values' in messages ? Array.from((messages as any).values()) : [messages]);
    const result = msgArray.map((m: any) => ({
      id: m.id,
      content: m.content,
      author: { id: m.author.id, username: m.author.username, tag: m.author.tag, bot: m.author.bot },
      createdAt: m.createdAt.toISOString(),
      editedAt: m.editedAt ? m.editedAt.toISOString() : null,
      attachments: m.attachments.map((a: any) => ({ id: a.id, name: a.name, url: a.url, contentType: a.contentType })),
      reactions: m.reactions.cache.map((r: any) => ({ emoji: r.emoji.name, count: r.count }))
    }));
    return {
      content: [{ type: "text", text: JSON.stringify(result, null, 2) }]
    };
  } catch (error) {
    return handleDiscordError(error);
  }
}

