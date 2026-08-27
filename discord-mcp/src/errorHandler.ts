import { ToolResponse } from './tools/types.js';

export function handleDiscordError(error: any, clientId?: string): ToolResponse {
  const errorMessage = typeof error === 'string' ? error : error?.message || String(error);
  const errorCode = error?.code;

  const inviteLink = clientId 
    ? `https://discord.com/oauth2/authorize?client_id=${clientId}&scope=bot&permissions=8` 
    : "https://discord.com/oauth2/authorize?client_id=YOUR_CLIENT_ID&scope=bot&permissions=52076489808";

  if (errorMessage.includes('Privileged intent provided is not enabled or whitelisted')) {
    return {
      content: [{ 
        type: "text", 
        text: `Error: Privileged intents are not enabled.\n\nSolution: Please enable the required intents (Message Content, Server Members, Presence) in the Discord Developer Portal.` 
      }],
      isError: true
    };
  }

  if (
    errorCode === 50001 ||
    errorCode === 10004 ||
    errorMessage.includes('Missing Access') ||
    errorMessage.includes('Unknown Guild') ||
    errorMessage.includes('Missing Permissions')
  ) {
    return {
      content: [{ 
        type: "text", 
        text: `Error: The bot lacks required permissions or is not a member of the server.\n\nInvite link: ${inviteLink}` 
      }],
      isError: true
    };
  }

  if (errorCode === 429 || errorMessage.includes('rate limit')) {
    return {
      content: [{ 
        type: "text", 
        text: `Error: Discord API rate limit reached. Please wait before retrying.` 
      }],
      isError: true
    };
  }

  return {
    content: [{ 
      type: "text", 
      text: `Discord API Error: ${errorMessage}` 
    }],
    isError: true
  };
}

