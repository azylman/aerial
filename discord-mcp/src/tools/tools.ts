import { Client } from "discord.js";
import { ToolResponse, ToolContext, ToolHandler } from "./types.js";
import { loginHandler } from './login.js';
import { sendMessageHandler, editMessageHandler, createThreadHandler } from './send-message.js';
import {
  getForumChannelsHandler,
  createForumPostHandler,
  getForumPostHandler,
  listForumThreadsHandler,
  replyToForumHandler,
  deleteForumPostHandler,
  getForumTagsHandler,
  setForumTagsHandler,
  updateForumPostHandler
} from './forum.js';
import {
  createTextChannelHandler,
  createForumChannelHandler,
  editChannelHandler,
  deleteChannelHandler,
  readMessagesHandler,
  createCategoryHandler,
  editCategoryHandler,
  deleteCategoryHandler,
  createVoiceChannelHandler,
  setChannelPermissionsHandler,
  removeChannelPermissionsHandler
} from './channel.js';
import {
  getServerInfoHandler,
  listServersHandler,
  searchMessagesHandler
} from "./server.js";
import {
  addReactionHandler,
  addMultipleReactionsHandler,
  removeReactionHandler,
  getReactionUsersHandler,
  deleteMessageHandler
} from './reactions.js';
import {
  createWebhookHandler,
  sendWebhookMessageHandler,
  editWebhookHandler,
  deleteWebhookHandler
} from './webhooks.js';
import {
  listRolesHandler,
  createRoleHandler,
  editRoleHandler,
  deleteRoleHandler,
  assignRoleHandler,
  removeRoleHandler,
  listMembersHandler,
  getMemberHandler
} from './roles.js';

export {
  loginHandler,
  sendMessageHandler,
  createThreadHandler,
  getForumChannelsHandler,
  createForumPostHandler,
  getForumPostHandler,
  listForumThreadsHandler,
  replyToForumHandler,
  deleteForumPostHandler,
  getForumTagsHandler,
  setForumTagsHandler,
  updateForumPostHandler,
  editMessageHandler,
  createTextChannelHandler,
  createForumChannelHandler,
  editChannelHandler,
  deleteChannelHandler,
  readMessagesHandler,
  getServerInfoHandler,
  addReactionHandler,
  addMultipleReactionsHandler,
  removeReactionHandler,
  getReactionUsersHandler,
  deleteMessageHandler,
  createWebhookHandler,
  sendWebhookMessageHandler,
  editWebhookHandler,
  deleteWebhookHandler,
  createCategoryHandler,
  editCategoryHandler,
  deleteCategoryHandler,
  createVoiceChannelHandler,
  setChannelPermissionsHandler,
  removeChannelPermissionsHandler,
  listRolesHandler,
  createRoleHandler,
  editRoleHandler,
  deleteRoleHandler,
  assignRoleHandler,
  removeRoleHandler,
  listMembersHandler,
  getMemberHandler,
  listServersHandler,
  searchMessagesHandler
};

export type { ToolResponse, ToolContext, ToolHandler };

export function createToolContext(client: Client): ToolContext {
  return { client };
}
