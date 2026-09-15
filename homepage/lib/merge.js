'use strict';

/**
 * Pure Homepage configuration merging utilities.
 * Operating exclusively on Plain Old JavaScript Objects (POJOs) with zero external dependencies.
 */

function normalizeList(val) {
  if (Array.isArray(val)) {
    return val.filter(item => item !== null && item !== undefined);
  }
  return [];
}

function normalizeObject(val) {
  if (val && typeof val === 'object' && !Array.isArray(val)) {
    return { ...val };
  }
  return {};
}

function cloneJson(obj) {
  return JSON.parse(JSON.stringify(obj));
}

/**
 * Merges service groups and items.
 * Core groups appear first; user groups are appended.
 * If a service exists in both core and user under the same group name, user overrides core.
 */
function mergeServices(core, user) {
  const coreList = normalizeList(core);
  const userList = normalizeList(user);

  const result = cloneJson(coreList);

  for (const userGroupObj of userList) {
    if (!userGroupObj || typeof userGroupObj !== 'object') continue;
    const entries = Object.entries(userGroupObj);
    if (entries.length === 0) continue;
    const [groupName, userServicesRaw] = entries[0];
    const userServices = normalizeList(userServicesRaw);

    const existingGroup = result.find(g => g && typeof g === 'object' && Object.keys(g)[0] === groupName);

    if (existingGroup) {
      const currentServices = normalizeList(existingGroup[groupName]);
      for (const uSvc of userServices) {
        if (!uSvc || typeof uSvc !== 'object') continue;
        const svcKey = Object.keys(uSvc)[0];
        const matchIdx = currentServices.findIndex(s => s && typeof s === 'object' && Object.keys(s)[0] === svcKey);
        if (matchIdx !== -1) {
          currentServices[matchIdx] = uSvc;
        } else {
          currentServices.push(uSvc);
        }
      }
      existingGroup[groupName] = currentServices;
    } else {
      result.push({ [groupName]: userServices });
    }
  }

  return result;
}

/**
 * Merges bookmark categories and link items.
 * Preserves Homepage's mandatory nested array schema:
 * [ { CategoryName: [ { LinkName: [ { href, icon } ] } ] } ]
 */
function mergeBookmarks(core, user) {
  const coreList = normalizeList(core);
  const userList = normalizeList(user);

  const result = cloneJson(coreList);

  for (const userCatObj of userList) {
    if (!userCatObj || typeof userCatObj !== 'object') continue;
    const entries = Object.entries(userCatObj);
    if (entries.length === 0) continue;
    const [catName, userLinksRaw] = entries[0];
    const userLinks = normalizeList(userLinksRaw);

    const existingCat = result.find(c => c && typeof c === 'object' && Object.keys(c)[0] === catName);

    if (existingCat) {
      const currentLinks = normalizeList(existingCat[catName]);
      for (const uLink of userLinks) {
        if (!uLink || typeof uLink !== 'object') continue;
        const linkKey = Object.keys(uLink)[0];
        const matchIdx = currentLinks.findIndex(l => l && typeof l === 'object' && Object.keys(l)[0] === linkKey);
        if (matchIdx !== -1) {
          currentLinks[matchIdx] = uLink;
        } else {
          currentLinks.push(uLink);
        }
      }
      existingCat[catName] = currentLinks;
    } else {
      result.push({ [catName]: userLinks });
    }
  }

  return result;
}

const SINGLETON_WIDGETS = new Set([
  'search',
  'openmeteo',
  'datetime',
  'resources',
  'glances',
  'weather'
]);

/**
 * Merges header widgets list.
 * Appends non-singleton widgets. Deduplicates singletons (e.g. search, openmeteo) favoring user config.
 */
function mergeWidgets(core, user) {
  const coreList = normalizeList(core);
  const userList = normalizeList(user);

  const result = cloneJson(coreList);

  for (const uWidget of userList) {
    if (!uWidget || typeof uWidget !== 'object') continue;
    const widgetKey = Object.keys(uWidget)[0];

    if (widgetKey && SINGLETON_WIDGETS.has(widgetKey.toLowerCase())) {
      const matchIdx = result.findIndex(w => w && typeof w === 'object' && Object.keys(w)[0]?.toLowerCase() === widgetKey.toLowerCase());
      if (matchIdx !== -1) {
        result[matchIdx] = uWidget;
      } else {
        result.push(uWidget);
      }
    } else {
      result.push(uWidget);
    }
  }

  return result;
}

/**
 * Merges settings.yaml.
 * Shallow merges root styling keys; deep merges `layout` mapping to preserve core layout order.
 */
function mergeSettings(core, user) {
  const coreObj = normalizeObject(core);
  const userObj = normalizeObject(user);

  const coreLayout = normalizeObject(coreObj.layout);
  const userLayout = normalizeObject(userObj.layout);

  return {
    ...coreObj,
    ...userObj,
    layout: {
      ...coreLayout,
      ...userLayout
    }
  };
}

/**
 * Merges docker.yaml connection configurations.
 */
function mergeDocker(core, user) {
  const coreObj = normalizeObject(core);
  const userObj = normalizeObject(user);

  return {
    ...coreObj,
    ...userObj
  };
}

/**
 * Concatenates custom CSS strings.
 */
function mergeCustomCss(coreContent, userContent) {
  const coreStr = (typeof coreContent === 'string' ? coreContent.trim() : '');
  const userStr = (typeof userContent === 'string' ? userContent.trim() : '');

  if (coreStr && userStr) {
    return `${coreStr}\n\n${userStr}`;
  }
  return coreStr || userStr || '';
}

module.exports = {
  mergeServices,
  mergeBookmarks,
  mergeWidgets,
  mergeSettings,
  mergeDocker,
  mergeCustomCss,
  normalizeList,
  normalizeObject
};
