import { SendMessageSchema, EditMessageSchema, CreateThreadSchema } from '../schemas.js';
import { ToolHandler } from './types.js';
import { handleDiscordError } from "../errorHandler.js";

export const sendMessageHandler: ToolHandler = async (args, { client }) => {
  const { channelId, message, replyToMessageId } = SendMessageSchema.parse(args);
  
  try {
    if (!client.isReady()) {
      return {
        content: [{ type: "text", text: "Discord client not logged in." }],
        isError: true
      };
    }

    const channel = await client.channels.fetch(channelId);
    if (!channel || !channel.isTextBased()) {
      return {
        content: [{ type: "text", text: `Cannot find text channel ID: ${channelId}` }],
        isError: true
      };
    }

    if ('send' in channel) {
      const messageOptions: any = {};
      if (replyToMessageId) {
        if ('messages' in channel) {
          try {
            await channel.messages.fetch(replyToMessageId);
            messageOptions.reply = { messageReference: replyToMessageId };
          } catch (error) {
            return {
              content: [{ type: "text", text: `Cannot find message with ID: ${replyToMessageId} in channel ${channelId}` }],
              isError: true
            };
          }
        } else {
          return {
            content: [{ type: "text", text: `This channel type does not support message replies` }],
            isError: true
          };
        }
      }
      
      messageOptions.content = message;
      await channel.send(messageOptions);
      
      const responseText = replyToMessageId 
        ? `Message successfully sent to channel ID: ${channelId} as a reply to message ID: ${replyToMessageId}`
        : `Message successfully sent to channel ID: ${channelId}`;
      
      return {
        content: [{ type: "text", text: responseText }]
      };
    } else {
      return {
        content: [{ type: "text", text: `This channel type does not support sending messages` }],
        isError: true
      };
    }
  } catch (error) {
    return handleDiscordError(error);
  }
};

export const createThreadHandler: ToolHandler = async (args, { client }) => {
  const { channelId, messageId, name, message } = CreateThreadSchema.parse(args);

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

    const targetMessage = await channel.messages.fetch(messageId);
    if (!targetMessage) {
      return {
        content: [{ type: "text", text: `Cannot find message with ID: ${messageId}` }],
        isError: true
      };
    }

    const thread = await targetMessage.startThread({
      name: name || "Discussion",
      autoArchiveDuration: 60,
    });

    let sentMessageText = "";
    if (message) {
      const sent = await thread.send({ content: message });
      sentMessageText = ` and posted initial reply message ID: ${sent.id}`;
    }

    return {
      content: [{ type: "text", text: `Successfully created thread "${thread.name}" (ID: ${thread.id}) from message ${messageId}${sentMessageText}` }]
    };
  } catch (error) {
    return handleDiscordError(error);
  }
};

export const editMessageHandler: ToolHandler = async (args, { client }) => {
  const { channelId, messageId, content } = EditMessageSchema.parse(args);

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
        content: [{ type: "text", text: `Cannot find text channel ID: ${channelId}` }],
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

    if (message.author.id !== client.user?.id) {
      return {
        content: [{ type: "text", text: `Cannot edit message: only messages sent by the bot can be edited` }],
        isError: true
      };
    }

    await message.edit(content);

    return {
      content: [{ type: "text", text: `Successfully edited message ${messageId} in channel ${channelId}` }]
    };
  } catch (error) {
    return handleDiscordError(error);
  }
};
