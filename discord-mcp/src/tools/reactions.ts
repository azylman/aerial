import { ToolHandler } from './types.js';
import { handleDiscordError } from "../errorHandler.js";
import { AddReactionSchema, AddMultipleReactionsSchema, RemoveReactionSchema, GetReactionUsersSchema, DeleteMessageSchema } from '../schemas.js';

export const addReactionHandler: ToolHandler = async (args, { client }) => {
  const { channelId, messageId, emoji } = AddReactionSchema.parse(args);

  try {
    if (!client.isReady()) {
      return {
        content: [{ type: "text", text: "Discord client not logged in." }],
        isError: true
      };
    }

    const channel = await client.channels.fetch(channelId);
    if (!channel || !channel.isTextBased() || !('messages' in channel)) {
      return {
        content: [{ type: "text", text: `Cannot find text channel with ID: ${channelId}` }],
        isError: true
      };
    }

    const message = await channel.messages.fetch(messageId);
    if (!message) {
      return {
        content: [{ type: "text", text: `Cannot find message with ID: ${messageId}` }],
        isError: true
      };
    }

    await message.react(emoji);

    return {
      content: [{ type: "text", text: `Successfully added reaction ${emoji} to message ${messageId}` }]
    };
  } catch (error) {
    return handleDiscordError(error);
  }
};

export const addMultipleReactionsHandler: ToolHandler = async (args, { client }) => {
  const { channelId, messageId, emojis } = AddMultipleReactionsSchema.parse(args);

  try {
    if (!client.isReady()) {
      return {
        content: [{ type: "text", text: "Discord client not logged in." }],
        isError: true
      };
    }

    const channel = await client.channels.fetch(channelId);
    if (!channel || !channel.isTextBased() || !('messages' in channel)) {
      return {
        content: [{ type: "text", text: `Cannot find text channel with ID: ${channelId}` }],
        isError: true
      };
    }

    const message = await channel.messages.fetch(messageId);
    if (!message) {
      return {
        content: [{ type: "text", text: `Cannot find message with ID: ${messageId}` }],
        isError: true
      };
    }

    const results = [];
    for (const emoji of emojis) {
      try {
        await message.react(emoji);
        results.push(`Added ${emoji}`);
      } catch (err: any) {
        results.push(`Failed ${emoji}: ${err.message}`);
      }
    }

    return {
      content: [{ type: "text", text: `Reactions processed for message ${messageId}:\n${results.join('\n')}` }]
    };
  } catch (error) {
    return handleDiscordError(error);
  }
};

export const removeReactionHandler: ToolHandler = async (args, { client }) => {
  const { channelId, messageId, emoji, userId } = RemoveReactionSchema.parse(args);

  try {
    if (!client.isReady()) {
      return {
        content: [{ type: "text", text: "Discord client not logged in." }],
        isError: true
      };
    }

    const channel = await client.channels.fetch(channelId);
    if (!channel || !channel.isTextBased() || !('messages' in channel)) {
      return {
        content: [{ type: "text", text: `Cannot find text channel with ID: ${channelId}` }],
        isError: true
      };
    }

    const message = await channel.messages.fetch(messageId);
    if (!message) {
      return {
        content: [{ type: "text", text: `Cannot find message with ID: ${messageId}` }],
        isError: true
      };
    }

    const reaction = message.reactions.cache.get(emoji);
    if (!reaction) {
      return {
        content: [{ type: "text", text: `Reaction ${emoji} not found on message ${messageId}` }],
        isError: true
      };
    }

    if (userId) {
      await reaction.users.remove(userId);
    } else {
      await reaction.users.remove(client.user?.id);
    }

    return {
      content: [{ type: "text", text: `Successfully removed reaction ${emoji} from message ${messageId}` }]
    };
  } catch (error) {
    return handleDiscordError(error);
  }
};

export const getReactionUsersHandler: ToolHandler = async (args, { client }) => {
  const { channelId, messageId, emoji, limit } = GetReactionUsersSchema.parse(args);

  try {
    if (!client.isReady()) {
      return {
        content: [{ type: "text", text: "Discord client not logged in." }],
        isError: true
      };
    }

    const channel = await client.channels.fetch(channelId);
    if (!channel || !channel.isTextBased() || !('messages' in channel)) {
      return {
        content: [{ type: "text", text: `Cannot find text channel with ID: ${channelId}` }],
        isError: true
      };
    }

    const message = await channel.messages.fetch(messageId);
    if (!message) {
      return {
        content: [{ type: "text", text: `Cannot find message with ID: ${messageId}` }],
        isError: true
      };
    }

    const reaction = message.reactions.cache.get(emoji);
    if (!reaction) {
      return {
        content: [{ type: "text", text: `Reaction ${emoji} not found on message ${messageId}` }]
      };
    }

    const users = await reaction.users.fetch({ limit });
    const userList = users.map(u => ({ id: u.id, username: u.username, tag: u.tag }));

    return {
      content: [{ type: "text", text: JSON.stringify(userList, null, 2) }]
    };
  } catch (error) {
    return handleDiscordError(error);
  }
};

export const deleteMessageHandler: ToolHandler = async (args, { client }) => {
  const { channelId, messageId, reason } = DeleteMessageSchema.parse(args);

  try {
    if (!client.isReady()) {
      return {
        content: [{ type: "text", text: "Discord client not logged in." }],
        isError: true
      };
    }

    const channel = await client.channels.fetch(channelId);
    if (!channel || !channel.isTextBased() || !('messages' in channel)) {
      return {
        content: [{ type: "text", text: `Cannot find text channel with ID: ${channelId}` }],
        isError: true
      };
    }

    const message = await channel.messages.fetch(messageId);
    if (!message) {
      return {
        content: [{ type: "text", text: `Cannot find message with ID: ${messageId}` }],
        isError: true
      };
    }

    await message.delete();

    return {
      content: [{ type: "text", text: `Successfully deleted message ${messageId} from channel ${channelId}${reason ? ` (Reason: ${reason})` : ''}` }]
    };
  } catch (error) {
    return handleDiscordError(error);
  }
};

