import { ToolHandler } from './types.js';
import { handleDiscordError } from "../errorHandler.js";
import {
  ListRolesSchema,
  CreateRoleSchema,
  EditRoleSchema,
  DeleteRoleSchema,
  AssignRoleSchema,
  RemoveRoleSchema,
  ListMembersSchema,
  GetMemberSchema
} from '../schemas.js';

export const listRolesHandler: ToolHandler = async (args, { client }) => {
  const { guildId } = ListRolesSchema.parse(args);
  try {
    if (!client.isReady()) {
      return { content: [{ type: "text", text: "Discord client not logged in." }], isError: true };
    }
    const guild = await client.guilds.fetch(guildId);
    const roles = await guild.roles.fetch();
    const roleList = roles.map(r => ({ id: r.id, name: r.name, color: r.hexColor, position: r.position, mentionable: r.mentionable, memberCount: r.members.size }));
    return { content: [{ type: "text", text: JSON.stringify(roleList, null, 2) }] };
  } catch (error) {
    return handleDiscordError(error);
  }
};

export const createRoleHandler: ToolHandler = async (args, { client }) => {
  const { guildId, name, color, hoist, mentionable, reason } = CreateRoleSchema.parse(args);
  try {
    if (!client.isReady()) {
      return { content: [{ type: "text", text: "Discord client not logged in." }], isError: true };
    }
    const guild = await client.guilds.fetch(guildId);
    const role = await guild.roles.create({ name, color: color as any, hoist, mentionable, reason });
    return { content: [{ type: "text", text: `Successfully created role "${role.name}" (ID: ${role.id})` }] };
  } catch (error) {
    return handleDiscordError(error);
  }
};

export const editRoleHandler: ToolHandler = async (args, { client }) => {
  const { guildId, roleId, name, color, hoist, mentionable, reason } = EditRoleSchema.parse(args);
  try {
    if (!client.isReady()) {
      return { content: [{ type: "text", text: "Discord client not logged in." }], isError: true };
    }
    const guild = await client.guilds.fetch(guildId);
    const role = await guild.roles.fetch(roleId);
    if (!role) {
      return { content: [{ type: "text", text: `Role ${roleId} not found` }], isError: true };
    }
    await role.edit({ name, color: color as any, hoist, mentionable, reason });
    return { content: [{ type: "text", text: `Successfully edited role "${role.name}" (ID: ${role.id})` }] };
  } catch (error) {
    return handleDiscordError(error);
  }
};

export const deleteRoleHandler: ToolHandler = async (args, { client }) => {
  const { guildId, roleId, reason } = DeleteRoleSchema.parse(args);
  try {
    if (!client.isReady()) {
      return { content: [{ type: "text", text: "Discord client not logged in." }], isError: true };
    }
    const guild = await client.guilds.fetch(guildId);
    const role = await guild.roles.fetch(roleId);
    if (!role) {
      return { content: [{ type: "text", text: `Role ${roleId} not found` }], isError: true };
    }
    await role.delete(reason);
    return { content: [{ type: "text", text: `Successfully deleted role ${roleId}` }] };
  } catch (error) {
    return handleDiscordError(error);
  }
};

export const assignRoleHandler: ToolHandler = async (args, { client }) => {
  const { guildId, userId, roleId, reason } = AssignRoleSchema.parse(args);
  try {
    if (!client.isReady()) {
      return { content: [{ type: "text", text: "Discord client not logged in." }], isError: true };
    }
    const guild = await client.guilds.fetch(guildId);
    const member = await guild.members.fetch(userId);
    await member.roles.add(roleId, reason);
    return { content: [{ type: "text", text: `Successfully assigned role ${roleId} to user ${userId}` }] };
  } catch (error) {
    return handleDiscordError(error);
  }
};

export const removeRoleHandler: ToolHandler = async (args, { client }) => {
  const { guildId, userId, roleId, reason } = RemoveRoleSchema.parse(args);
  try {
    if (!client.isReady()) {
      return { content: [{ type: "text", text: "Discord client not logged in." }], isError: true };
    }
    const guild = await client.guilds.fetch(guildId);
    const member = await guild.members.fetch(userId);
    await member.roles.remove(roleId, reason);
    return { content: [{ type: "text", text: `Successfully removed role ${roleId} from user ${userId}` }] };
  } catch (error) {
    return handleDiscordError(error);
  }
};

export const listMembersHandler: ToolHandler = async (args, { client }) => {
  const { guildId, limit } = ListMembersSchema.parse(args);
  try {
    if (!client.isReady()) {
      return { content: [{ type: "text", text: "Discord client not logged in." }], isError: true };
    }
    const guild = await client.guilds.fetch(guildId);
    const members = await guild.members.fetch({ limit });
    const memberList = members.map(m => ({ id: m.id, username: m.user.username, tag: m.user.tag, nickname: m.nickname, roles: m.roles.cache.map(r => r.name) }));
    return { content: [{ type: "text", text: JSON.stringify(memberList, null, 2) }] };
  } catch (error) {
    return handleDiscordError(error);
  }
};

export const getMemberHandler: ToolHandler = async (args, { client }) => {
  const { guildId, userId } = GetMemberSchema.parse(args);
  try {
    if (!client.isReady()) {
      return { content: [{ type: "text", text: "Discord client not logged in." }], isError: true };
    }
    const guild = await client.guilds.fetch(guildId);
    const member = await guild.members.fetch(userId);
    const memberData = {
      id: member.id,
      username: member.user.username,
      tag: member.user.tag,
      nickname: member.nickname,
      joinedAt: member.joinedAt,
      roles: member.roles.cache.map(r => ({ id: r.id, name: r.name }))
    };
    return { content: [{ type: "text", text: JSON.stringify(memberData, null, 2) }] };
  } catch (error) {
    return handleDiscordError(error);
  }
};

