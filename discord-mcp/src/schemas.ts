import { z } from "zod";

export const DiscordLoginSchema = z.object({
    token: z.string({ description: "The bot token to use for login." }).optional()
}, {
    description: "Login to Discord using a bot token. If no token is provided, the bot will attempt to use the token from the environment variable DISCORD_TOKEN."
});

export const SendMessageSchema = z.object({
    channelId: z.string({ description: "The ID of the channel to send the message to." }),
    message: z.string({ description: "The content of the message to send." }),
    replyToMessageId: z.string({ description: "The ID of the message to reply to, if any." }).optional()
}, {
    description: "Send a message to a specified channel, optionally as a reply to another message."
});

export const CreateThreadSchema = z.object({
    channelId: z.string({ description: "The ID of the text channel containing the message." }),
    messageId: z.string({ description: "The ID of the message to start the thread from." }),
    name: z.string({ description: "The title/name for the new thread." }),
    message: z.string({ description: "Optional initial response message to post directly inside the newly created thread." }).optional()
}, {
    description: "Create a new thread from an existing message in a Discord text channel, and optionally post an initial response inside it."
});

export const GetForumChannelsSchema = z.object({
    guildId: z.string({ description: "The ID of the server (guild) to get forum channels from." })
}, {
    description: "Get all forum channels in a specified server (guild)."
});

export const CreateForumPostSchema = z.object({
    forumChannelId: z.string({ description: "The ID of the forum channel where the thread will be created." }),
    title: z.string({ description: "The title of the forum post (thread)." }),
    content: z.string({ description: "The body content of the forum post." }),
    tags: z.array(z.string({ description: "A tag to attach to the forum post." })).optional()
}, {
    description: "Create a new forum post (thread) in a specified forum channel."
});

export const GetForumPostSchema = z.object({
    threadId: z.string({ description: "The ID of the forum thread to retrieve." })
}, {
    description: "Get details of a specific forum post (thread) by its ID."
});

export const ListForumThreadsSchema = z.object({
    forumChannelId: z.string(),
    includeArchived: z.boolean().optional().default(true),
    limit: z.number().min(1).max(100).optional().default(100)
});

export const ReplyToForumSchema = z.object({
    threadId: z.string({ description: "The ID of the forum thread to reply to." }),
    message: z.string({ description: "The content of the reply message." })
}, {
    description: "Reply to a specific forum post (thread) by its ID."
});

export const CreateTextChannelSchema = z.object({
    guildId: z.string(),
    channelName: z.string(),
    topic: z.string().optional(),
    categoryId: z.string().optional(),
    reason: z.string().optional()
});

export const CreateForumChannelSchema = z.object({
    guildId: z.string({ description: "The ID of the server (guild) to create the forum channel in." }),
    name: z.string({ description: "The name of the forum channel to create." }),
    topic: z.string({ description: "The forum channel guidelines/description." }).optional(),
    categoryId: z.string({ description: "The ID of the parent category to create the channel under." }).optional(),
    reason: z.string({ description: "Optional reason for audit logs." }).optional()
}, {
    description: "Create a new forum channel in a specified server (guild), optionally under a category."
});

export const EditChannelSchema = z.object({
    channelId: z.string({ description: "The ID of the channel to edit." }),
    name: z.string({ description: "New name for the channel." }).optional(),
    topic: z.string({ description: "New topic for the channel." }).optional(),
    parentId: z.string({ description: "The ID of a category to move the channel under." }).optional(),
    position: z.number({ description: "New position of the channel in the list." }).optional(),
    reason: z.string({ description: "Optional reason for audit logs." }).optional()
}, {
    description: "Edit a Discord channel's name, topic, parent category, or position."
});

export const CreateCategorySchema = z.object({
    guildId: z.string({ description: "The ID of the server (guild) where the category will be created." }),
    name: z.string({ description: "The name of the category to create." }),
    position: z.number({ description: "Optional sorting position index for the category." }).optional(),
    reason: z.string({ description: "Optional reason for audit logs when creating the category." }).optional()
}, {
    description: "Create a new category in a specified server (guild)."
});

export const EditCategorySchema = z.object({
    categoryId: z.string({ description: "The ID of the category to edit." }),
    name: z.string({ description: "New name for the category (optional)." }).optional(),
    position: z.number({ description: "New position index for the category (optional)." }).optional(),
    reason: z.string({ description: "Optional reason for audit logs when editing the category." }).optional()
}, {
    description: "Edit an existing category's properties."
});

export const DeleteCategorySchema = z.object({
    categoryId: z.string({ description: "The ID of the category to delete." }),
    reason: z.string({ description: "Optional reason for audit logs when deleting the category." }).optional()
}, {
    description: "Delete a category by its ID."
});

export const CreateVoiceChannelSchema = z.object({
    guildId: z.string({ description: "The ID of the server (guild) where the voice channel will be created." }),
    name: z.string({ description: "The name of the voice channel to create." }),
    categoryId: z.string({ description: "The ID of the parent category to create the channel under." }).optional(),
    bitrate: z.number({ description: "The bitrate (in bits) for the voice channel." }).optional(),
    userLimit: z.number({ description: "The maximum number of users allowed in the voice channel." }).optional(),
    position: z.number({ description: "Optional sorting position index for the channel." }).optional(),
    reason: z.string({ description: "Optional reason for audit logs when creating the channel." }).optional()
}, {
    description: "Create a new voice channel in a specified server (guild)."
});

export const SetChannelPermissionsSchema = z.object({
    channelId: z.string({ description: "The ID of the channel to set permissions on." }),
    targetId: z.string({ description: "The ID of the role or user to set permissions for." }),
    targetType: z.enum(['role', 'member'], { description: "The type of target: 'role' or 'member'." }),
    allow: z.array(z.string(), { description: "Permission flags to allow (e.g. ['ViewChannel', 'SendMessages'])." }).optional(),
    deny: z.array(z.string(), { description: "Permission flags to deny (e.g. ['SendMessages'])." }).optional(),
    reason: z.string({ description: "Optional reason for audit logs." }).optional()
}, {
    description: "Set permission overrides for a role or user on a Discord channel."
});

export const RemoveChannelPermissionsSchema = z.object({
    channelId: z.string({ description: "The ID of the channel to remove permissions from." }),
    targetId: z.string({ description: "The ID of the role or user whose permission overrides should be removed." }),
    reason: z.string({ description: "Optional reason for audit logs." }).optional()
}, {
    description: "Remove permission overrides for a role or user from a Discord channel."
});

export const DeleteChannelSchema = z.object({
    channelId: z.string({ description: "The ID of the channel to delete." }),
    reason: z.string({ description: "Optional reason for audit logs." }).optional()
}, {
    description: "Delete a channel by its ID."
});

export const SearchMessagesSchema = z.object({
    guildId: z.string({ description: "The ID of the server (guild) to search messages in." }),
    content: z.string({ description: "The text content to search for." }).optional(),
    authorId: z.string({ description: "The ID of the user who authored the messages." }).optional(),
    channelId: z.string({ description: "The ID of the specific channel to search in." }).optional(),
    mentions: z.string({ description: "The ID of a user mentioned in the messages." }).optional(),
    has: z.enum(['link', 'embed', 'file', 'image', 'sound', 'video'], { description: "Filter messages that have a specific type of attachment or embed." }).optional(),
    minId: z.string({ description: "Filter messages after this message ID (Snowflake)." }).optional(),
    maxId: z.string({ description: "Filter messages before this message ID (Snowflake)." }).optional(),
    limit: z.number({ description: "Maximum number of messages to return (default 25, max 100)." }).min(1).max(100).optional().default(25)
}, {
    description: "Search messages in a server with various filters."
});

export const ReadMessagesSchema = z.object({
    channelId: z.string({ description: "The ID of the channel to read messages from." }),
    limit: z.number({ description: "Maximum number of messages to fetch (1-100, default 50)." }).min(1).max(100).optional().default(50),
    before: z.string({ description: "Fetch messages before this message ID or ISO 8601 date." }).optional(),
    after: z.string({ description: "Fetch messages after this message ID or ISO 8601 date." }).optional(),
    around: z.string({ description: "Fetch messages around this message ID or ISO 8601 date." }).optional()
}, {
    description: "Read messages from a specified channel with pagination support."
});

export const EditMessageSchema = z.object({
    channelId: z.string({ description: "The ID of the channel containing the message to edit." }),
    messageId: z.string({ description: "The ID of the message to edit." }),
    content: z.string({ description: "The new text content for the message." })
}, {
    description: "Edit a message previously sent by the bot."
});

export const AddReactionSchema = z.object({
    channelId: z.string({ description: "The ID of the channel containing the message to react to." }),
    messageId: z.string({ description: "The ID of the message to add a reaction to." }),
    emoji: z.string({ description: "The emoji to use for the reaction (unicode or custom)." })
}, {
    description: "Add a reaction to a specific message in a channel."
});

export const AddMultipleReactionsSchema = z.object({
    channelId: z.string({ description: "The ID of the channel containing the message to react to." }),
    messageId: z.string({ description: "The ID of the message to add reactions to." }),
    emojis: z.array(z.string({ description: "An emoji to add (unicode or custom)." }))
}, {
    description: "Add multiple reactions to a specific message in a channel."
});

export const RemoveReactionSchema = z.object({
    channelId: z.string({ description: "The ID of the channel containing the message to modify reactions on." }),
    messageId: z.string({ description: "The ID of the message to remove the reaction from." }),
    emoji: z.string({ description: "The emoji reaction to remove." }),
    userId: z.string({ description: "Optional ID of the user whose reaction should be removed; if omitted, removes the current bot's reaction." }).optional()
}, {
    description: "Remove a reaction from a specific message in a channel."
});

export const GetReactionUsersSchema = z.object({
    channelId: z.string({ description: "The ID of the channel containing the message." }),
    messageId: z.string({ description: "The ID of the message to inspect reactions on." }),
    emoji: z.string({ description: "The emoji whose reactors should be listed." }),
    limit: z.number({ description: "Maximum number of users to fetch per reaction (1-100, default 100)." }).min(1).max(100).optional().default(100)
}, {
    description: "List the users who reacted with a specific emoji to a message."
});

export const DeleteMessageSchema = z.object({
    channelId: z.string({ description: "The ID of the channel containing the message to delete." }),
    messageId: z.string({ description: "The ID of the message to delete." }),
    reason: z.string({ description: "Optional reason for audit logs when deleting the message." }).optional()
}, {
    description: "Delete a message by its ID in a specified channel."
});

export const CreateWebhookSchema = z.object({
    channelId: z.string({ description: "The ID of the channel to create the webhook in." }),
    name: z.string({ description: "The name to assign to the webhook." }),
    avatar: z.string({ description: "Optional avatar URL or data for the webhook." }).optional(),
    reason: z.string({ description: "Optional reason for audit logs when creating the webhook." }).optional()
}, {
    description: "Create a webhook in a specified channel."
});

export const SendWebhookMessageSchema = z.object({
    webhookId: z.string({ description: "The ID of the webhook to send the message with." }),
    webhookToken: z.string({ description: "The token for the webhook (used for authentication)." }),
    content: z.string({ description: "The message content to send via the webhook." }),
    username: z.string({ description: "Optional username to display for the webhook message." }).optional(),
    avatarURL: z.string({ description: "Optional avatar URL to display for the webhook message." }).optional(),
    threadId: z.string({ description: "Optional ID of the thread to post the webhook message into." }).optional()
}, {
    description: "Send a message using a webhook."
});

export const EditWebhookSchema = z.object({
    webhookId: z.string({ description: "The ID of the webhook to edit." }),
    webhookToken: z.string({ description: "Optional token for the webhook if required to authorize edits." }).optional(),
    name: z.string({ description: "Optional new name for the webhook." }).optional(),
    avatar: z.string({ description: "Optional new avatar URL or data for the webhook." }).optional(),
    channelId: z.string({ description: "Optional channel ID to move the webhook to." }).optional(),
    reason: z.string({ description: "Optional reason for audit logs when editing the webhook." }).optional()
}, {
    description: "Edit a webhook's properties."
});

export const DeleteWebhookSchema = z.object({
    webhookId: z.string({ description: "The ID of the webhook to delete." }),
    webhookToken: z.string({ description: "Optional token for the webhook if required for deletion." }).optional(),
    reason: z.string({ description: "Optional reason for audit logs when deleting the webhook." }).optional()
}, {
    description: "Delete a webhook by its ID and token."
});

export const ListServersSchema = z.object({
    limit: z.number({ description: "Maximum number of servers to return (1-200, default 100)." }).min(1).max(200).optional().default(100)
}, {
    description: "List all Discord servers (guilds) the bot is a member of."
});

export const GetServerInfoSchema = z.object({
    guildId: z.string({ description: "The ID of the server (guild) to get information for." })
}, {
    description: "Get detailed information about a Discord server."
});

export const ListRolesSchema = z.object({
    guildId: z.string({ description: "The ID of the server (guild) to list roles from." })
}, {
    description: "List all roles in a server."
});

export const CreateRoleSchema = z.object({
    guildId: z.string({ description: "The ID of the server (guild) to create the role in." }),
    name: z.string({ description: "The name of the role." }),
    color: z.string({ description: "The color of the role in hex format." }).optional(),
    hoist: z.boolean({ description: "Whether the role should be displayed separately in the member list." }).optional(),
    mentionable: z.boolean({ description: "Whether the role should be mentionable by anyone." }).optional(),
    reason: z.string({ description: "Optional reason for audit logs." }).optional()
}, {
    description: "Create a new role in a server."
});

export const EditRoleSchema = z.object({
    guildId: z.string({ description: "The ID of the server (guild) containing the role." }),
    roleId: z.string({ description: "The ID of the role to edit." }),
    name: z.string({ description: "New name for the role." }).optional(),
    color: z.string({ description: "New color in hex format." }).optional(),
    hoist: z.boolean({ description: "Whether the role should be displayed separately." }).optional(),
    mentionable: z.boolean({ description: "Whether the role should be mentionable." }).optional(),
    reason: z.string({ description: "Optional reason for audit logs." }).optional()
}, {
    description: "Edit an existing role in a server."
});

export const DeleteRoleSchema = z.object({
    guildId: z.string({ description: "The ID of the server (guild) containing the role." }),
    roleId: z.string({ description: "The ID of the role to delete." }),
    reason: z.string({ description: "Optional reason for audit logs." }).optional()
}, {
    description: "Delete a role from a server."
});

export const AssignRoleSchema = z.object({
    guildId: z.string({ description: "The ID of the server (guild) containing the role and member." }),
    userId: z.string({ description: "The ID of the user (member) to assign the role to." }),
    roleId: z.string({ description: "The ID of the role to assign." }),
    reason: z.string({ description: "Optional reason for audit logs." }).optional()
}, {
    description: "Assign a role to a member in a server."
});

export const RemoveRoleSchema = z.object({
    guildId: z.string({ description: "The ID of the server (guild) containing the role and member." }),
    userId: z.string({ description: "The ID of the user (member) to remove the role from." }),
    roleId: z.string({ description: "The ID of the role to remove." }),
    reason: z.string({ description: "Optional reason for audit logs." }).optional()
}, {
    description: "Remove a role from a member in a server."
});

export const ListMembersSchema = z.object({
    guildId: z.string({ description: "The ID of the server (guild) to list members from." }),
    limit: z.number({ description: "Maximum number of members to return (1-1000, default 100)." }).min(1).max(1000).optional().default(100)
}, {
    description: "List members in a server with their roles."
});

export const GetMemberSchema = z.object({
    guildId: z.string({ description: "The ID of the server (guild) containing the member." }),
    userId: z.string({ description: "The ID of the member to get details for." })
}, {
    description: "Get detailed information about a specific member in a server."
});

export const GetForumTagsSchema = z.object({
    forumChannelId: z.string({ description: "The ID of the forum channel to get tags from." })
}, {
    description: "Get all available tags for a forum channel."
});

export const SetForumTagsSchema = z.object({
    forumChannelId: z.string({ description: "The ID of the forum channel to set tags for." }),
    tags: z.array(z.object({
        name: z.string(),
        moderated: z.boolean().optional(),
        emoji: z.string().optional()
    }))
}, {
    description: "Set tags for a forum channel."
});

export const UpdateForumPostSchema = z.object({
    threadId: z.string({ description: "The ID of the forum post (thread) to update." }),
    title: z.string().optional(),
    appliedTags: z.array(z.string()).optional(),
    archived: z.boolean().optional(),
    locked: z.boolean().optional()
}, {
    description: "Update a forum post (thread) properties."
});

export const DeleteForumPostSchema = z.object({
    threadId: z.string({ description: "The ID of the forum post (thread) to delete." })
}, {
    description: "Delete a forum post (thread)."
});
