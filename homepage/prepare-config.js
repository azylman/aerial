'use strict';

const fs = require('fs');
const path = require('path');
const {
  mergeServices,
  mergeBookmarks,
  mergeWidgets,
  mergeSettings,
  mergeDocker,
  mergeCustomCss
} = require('./lib/merge');

const CORE_DIR = process.env.CORE_CONFIG_DIR || '/app/core-homepage/config';
const USER_DIR = process.env.USER_CONFIG_DIR || '/share/aerial-config/homepage';
const TARGET_DIR = process.env.TARGET_CONFIG_DIR || '/config';

function resolveYamlParser() {
  try {
    return require('js-yaml');
  } catch (_) {
    // Try common container paths
    const candidates = [
      '/app/node_modules/js-yaml',
      '/app/.next/standalone/node_modules/js-yaml',
      '/usr/local/lib/node_modules/js-yaml'
    ];
    for (const p of candidates) {
      try {
        return require(p);
      } catch (_) {}
    }
    return null;
  }
}

function safeRead(filePath) {
  try {
    if (fs.existsSync(filePath)) {
      return fs.readFileSync(filePath, 'utf8');
    }
  } catch (err) {
    console.error(`[prepare-config] Warning: Failed to read ${filePath}: ${err.message}`);
  }
  return null;
}

function parseYaml(yaml, content, filePath) {
  if (!content || !content.trim()) {
    return null;
  }
  try {
    return yaml.load(content);
  } catch (err) {
    console.error(`[prepare-config] Error parsing YAML in ${filePath}: ${err.message}`);
    return { __parseError: err.message };
  }
}

function writeAtomic(targetDir, filename, content) {
  const targetPath = path.join(targetDir, filename);
  const tempPath = path.join(targetDir, `.${filename}.tmp.${process.pid}.${Date.now()}`);

  try {
    fs.writeFileSync(tempPath, content, { mode: 0o644, encoding: 'utf8' });
    // Remove symlink if exists to prevent writing through to read-only mounts
    try {
      const lstat = fs.lstatSync(targetPath);
      if (lstat.isSymbolicLink()) {
        fs.unlinkSync(targetPath);
      }
    } catch (_) {}
    fs.renameSync(tempPath, targetPath);
  } catch (err) {
    console.error(`[prepare-config] Error writing ${targetPath}: ${err.message}`);
    try {
      if (fs.existsSync(tempPath)) fs.unlinkSync(tempPath);
    } catch (_) {}
    throw err;
  }
}

function copyFallback(srcDir, dstDir) {
  try {
    if (!fs.existsSync(srcDir)) return;
    const entries = fs.readdirSync(srcDir);
    for (const entry of entries) {
      const srcPath = path.join(srcDir, entry);
      const dstPath = path.join(dstDir, entry);
      try {
        const stat = fs.statSync(srcPath);
        if (stat.isFile()) {
          const content = fs.readFileSync(srcPath);
          fs.writeFileSync(dstPath, content, { mode: 0o644 });
        }
      } catch (_) {}
    }
  } catch (err) {
    console.error(`[prepare-config] Fallback copy error: ${err.message}`);
  }
}

function run() {
  console.log(`[prepare-config] Initializing Homepage configuration...`);
  console.log(`[prepare-config] Core: ${CORE_DIR} | User: ${USER_DIR} | Target: ${TARGET_DIR}`);

  if (!fs.existsSync(TARGET_DIR)) {
    fs.mkdirSync(TARGET_DIR, { recursive: true, mode: 0o755 });
  }

  const yaml = resolveYamlParser();
  if (!yaml) {
    console.error(`[prepare-config] js-yaml not found in container environment. Falling back to raw core copy.`);
    copyFallback(CORE_DIR, TARGET_DIR);
    return;
  }

  const configFiles = [
    { name: 'services.yaml', merger: mergeServices, isYaml: true },
    { name: 'widgets.yaml', merger: mergeWidgets, isYaml: true },
    { name: 'bookmarks.yaml', merger: mergeBookmarks, isYaml: true },
    { name: 'settings.yaml', merger: mergeSettings, isYaml: true },
    { name: 'docker.yaml', merger: mergeDocker, isYaml: true },
    { name: 'custom.css', merger: mergeCustomCss, isYaml: false }
  ];

  const logEntries = [];

  for (const cfg of configFiles) {
    try {
      const corePath = path.join(CORE_DIR, cfg.name);
      const userPath = path.join(USER_DIR, cfg.name);

      const coreRaw = safeRead(corePath);
      const userRaw = safeRead(userPath);

      if (cfg.isYaml) {
        const coreParsed = parseYaml(yaml, coreRaw, corePath);
        const userParsed = parseYaml(yaml, userRaw, userPath);

        let finalObj;
        if (userParsed && userParsed.__parseError) {
          console.error(`[prepare-config] Warning: Skipping malformed user ${cfg.name}; falling back to core.`);
          logEntries.push(`${cfg.name}: user parse error (${userParsed.__parseError}) - fell back to core`);
          finalObj = (coreParsed && !coreParsed.__parseError) ? coreParsed : null;
        } else {
          const safeCore = (coreParsed && !coreParsed.__parseError) ? coreParsed : null;
          const safeUser = (userParsed && !userParsed.__parseError) ? userParsed : null;
          finalObj = cfg.merger(safeCore, safeUser);
          logEntries.push(`${cfg.name}: merged successfully`);
        }

        const defaultFallback = (cfg.name === 'settings.yaml' || cfg.name === 'docker.yaml') ? {} : [];
        const dumped = yaml.dump(finalObj !== null && finalObj !== undefined ? finalObj : defaultFallback);
        writeAtomic(TARGET_DIR, cfg.name, dumped);
      } else {
        // Custom CSS
        const mergedCss = cfg.merger(coreRaw, userRaw);
        writeAtomic(TARGET_DIR, cfg.name, mergedCss);
        logEntries.push(`${cfg.name}: concatenated successfully`);
      }
    } catch (err) {
      console.error(`[prepare-config] Error processing ${cfg.name}: ${err.message}`);
      logEntries.push(`${cfg.name}: failed (${err.message})`);
      // Fallback: copy core file directly if present
      const corePath = path.join(CORE_DIR, cfg.name);
      if (fs.existsSync(corePath)) {
        try {
          const content = fs.readFileSync(corePath, 'utf8');
          writeAtomic(TARGET_DIR, cfg.name, content);
        } catch (_) {}
      }
    }
  }

  // Copy any extra core files not explicitly merged (e.g. custom.js, icons, etc.)
  try {
    if (fs.existsSync(CORE_DIR)) {
      const coreFiles = fs.readdirSync(CORE_DIR);
      for (const file of coreFiles) {
        if (!configFiles.some(c => c.name === file)) {
          const src = path.join(CORE_DIR, file);
          const dst = path.join(TARGET_DIR, file);
          if (fs.statSync(src).isFile() && !fs.existsSync(dst)) {
            const data = fs.readFileSync(src);
            writeAtomic(TARGET_DIR, file, data);
            logEntries.push(`${file}: copied from core`);
          }
        }
      }
    }
  } catch (err) {
    console.error(`[prepare-config] Warning: Copying extra files failed: ${err.message}`);
  }

  try {
    const summary = `Homepage Config Merge Summary - ${new Date().toISOString()}\n` +
      logEntries.map(e => ` • ${e}`).join('\n') + '\n';
    fs.writeFileSync(path.join(TARGET_DIR, 'merge-summary.log'), summary, { mode: 0o644 });
  } catch (_) {}

  console.log(`[prepare-config] Configuration preparation complete.`);
}

try {
  run();
} catch (fatalErr) {
  console.error(`[prepare-config] Fatal error during config preparation: ${fatalErr.message}`);
  console.error(`[prepare-config] Copying raw core configs to prevent container crash loop.`);
  copyFallback(CORE_DIR, TARGET_DIR);
}

// Ensure clean exit code 0 so container continues to server.js
process.exit(0);
