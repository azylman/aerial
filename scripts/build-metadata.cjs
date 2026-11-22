'use strict';
const fs = require('node:fs');
function requireValid(ok, message) { if (!ok) throw new Error(message); }
// CLI: build-metadata.cjs <manifest|running|result|final> [--image-tag commit|tree]
// --image-tag commit (default): pushes reference the commit tag, pull requests tree-<sha>.
// --image-tag tree: every pushed image references its tree-<sha> tag.
function parseArgs(argv) {
  const [mode, ...rest] = argv;
  let imageTag = 'commit';
  for (let i = 0; i < rest.length; i++) {
    if (rest[i] === '--image-tag') imageTag = rest[++i];
    else if (rest[i].startsWith('--image-tag=')) imageTag = rest[i].slice('--image-tag='.length);
    else throw new Error(`Unknown argument: ${rest[i]}`);
  }
  requireValid(imageTag === 'commit' || imageTag === 'tree', 'Invalid --image-tag');
  return {mode,imageTag};
}
// JSON.parse alone accepts duplicate keys. Reject ambiguous records before
// interpreting them, including escaped spellings of the same property.
function parseStrict(raw) {
  requireValid(typeof raw === 'string' && Buffer.byteLength(raw) <= 65536, 'Invalid metadata payload');
  const stack = [];
  const tokens = raw.match(/"(?:[^"\\]|\\.)*"|[{}\[\]:,]|[^\s{}\[\]:,]+/g) || [];
  for (let i = 0; i < tokens.length; i++) {
    const t = tokens[i];
    if (t === '{') stack.push(new Set());
    else if (t === '[') stack.push(null);
    else if (t === '}' || t === ']') stack.pop();
    else if (t.startsWith('"') && tokens[i+1] === ':') {
      const key = JSON.parse(t), keys = stack.at(-1);
      requireValid(keys && !keys.has(key), 'Duplicate JSON key');
      keys.add(key);
    }
  }
  return JSON.parse(raw);
}
const recordFields = new Set(['version','kind','repository','workflow_path','run_id','run_number','run_attempt','event','source_sha','execution_sha','pr_number','selected','results','service','image_name','state','pushed','source_attempt','image_refs','digest']);
function validateRecord(m) {
  requireValid(m !== null && typeof m === 'object' && m.version === 1 && Array.isArray(m.selected) && Number.isSafeInteger(m.run_attempt) && m.run_attempt > 0, 'Invalid metadata');
  requireValid(/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/.test(m.repository) && /^\.github\/workflows\/[A-Za-z0-9_.-]+\.ya?ml$/.test(m.workflow_path) && ['push','pull_request','workflow_dispatch'].includes(m.event) && ['manifest','result','final'].includes(m.kind), 'Invalid run identity');
  requireValid([m.run_id,m.run_number].every(n => Number.isSafeInteger(n) && n > 0) && [m.source_sha,m.execution_sha].every(sha => /^[a-f0-9]{40}$/.test(sha)) && (m.pr_number === null || (Number.isSafeInteger(m.pr_number) && m.pr_number > 0)), 'Invalid source identity');
  requireValid(Object.keys(m).every(k => recordFields.has(k)), 'Unknown metadata field');
  return m;
}
function selection(matrix, hasBuilds) {
  requireValid(hasBuilds === 'true' || hasBuilds === 'false', 'Missing build selection');
  if (hasBuilds === 'false') return [];
  const rows = JSON.parse(matrix);
  requireValid(Array.isArray(rows), 'Invalid matrix');
  const selected = rows.map(({service,image_name}) => ({service,image_name}));
  const seen = new Set();
  for (const row of selected) {
    requireValid(/^[a-z0-9][a-z0-9_-]*$/.test(row.service) && row.service !== 'none' && /^[a-z0-9][a-z0-9_-]*$/.test(row.image_name) && !seen.has(row.service), 'Invalid selection');
    seen.add(row.service);
  }
  return selected;
}
const immutable = ['version','repository','workflow_path','run_id','run_number','event','source_sha','execution_sha','pr_number'];
function sameRun(a,b) { return immutable.every(key => a[key] === b[key]) && JSON.stringify(a.selected) === JSON.stringify(b.selected); }
function aggregate(base, records, matrix) {
  requireValid(Array.isArray(base.selected), 'Missing selection');
  const latest = new Map();
  const seen = new Set();
  for (const r of records) {
    requireValid(sameRun(base,r) && r.kind === 'result' && Number.isSafeInteger(r.run_attempt) && r.run_attempt > 0 && r.run_attempt <= base.run_attempt && r.source_attempt === r.run_attempt, 'Invalid result identity');
    requireValid(base.selected.some(s => s.service === r.service && s.image_name === r.image_name), 'Unexpected service');
    requireValid(['running','success','failure','cancelled'].includes(r.state) && typeof r.pushed === 'boolean' && (!r.pushed || r.state === 'success'), 'Invalid result state');
    const key = `${r.run_attempt}/${r.service}`;
    requireValid(!seen.has(key), 'Duplicate result'); seen.add(key);
    if (!latest.has(r.service) || latest.get(r.service).run_attempt < r.run_attempt) latest.set(r.service,r);
  }
  const results = base.selected.map(s => {
    const r = latest.get(s.service);
    if (!r || (r.run_attempt < base.run_attempt && (matrix !== 'success' || r.state !== 'success'))) return {...s,state:'running',pushed:false,source_attempt:base.run_attempt};
    const {state,pushed,source_attempt,image_refs,digest} = r;
    return {...s,state,pushed,source_attempt,...(image_refs ? {image_refs} : {}),...(digest ? {digest} : {})};
  });
  return {...base,kind:'final',results};
}
function outcome(steps, allowed, jobState) {
  const pushes = [steps.cache_push?.outcome,steps.image_push?.outcome];
  const state = jobState === 'cancelled' ? 'cancelled' : jobState === 'failure' || pushes.includes('failure') ? 'failure' : 'success';
  return {state,pushed:state === 'success' && allowed && pushes.includes('success')};
}
function externalID(m) { return `build-metadata:v1:${m.repository}:${m.run_id}:${m.run_attempt}:${m.kind === 'result' ? m.service : m.kind}`; }
async function listChecks(api,repo,sha) {
  const all = [];
  for (let page=1; ;page++) {
    const data = await api(`/repos/${repo}/commits/${sha}/check-runs?filter=all&per_page=100&page=${page}`);
    requireValid(Array.isArray(data?.check_runs), 'Invalid checks response');
    all.push(...data.check_runs);
    if (data.check_runs.length < 100) return all;
  }
}
// Compatibility is limited to this v1 transport: only absent text may use
// the historical summary payload. Canonical text never falls back on errors.
function transportPayload(output) {
  const raw = output?.text === undefined || output.text === null ? output?.summary : output.text;
  const metadata = validateRecord(parseStrict(raw));
  if (output?.text !== undefined && output.text !== null && output.summary) {
    let summary;
    try { summary = JSON.parse(output.summary); } catch { /* Human summary is normal. */ }
    if (summary !== undefined) parseStrict(output.summary);
    requireValid(summary === undefined || output.summary === raw, 'Conflicting metadata payloads');
  }
  return {raw,metadata};
}
async function main(env=process.env, argv=process.argv.slice(2)) {
  const {mode,imageTag} = parseArgs(argv);
  const event = JSON.parse(fs.readFileSync(env.GITHUB_EVENT_PATH,'utf8'));
  // Fork PRs keep native CI validation with their read-only token. Missing
  // transport records mean unavailable metadata, never an empty selection.
  const headRepo = event.pull_request?.head?.repo?.full_name;
  if (env.GITHUB_EVENT_NAME === 'pull_request' && headRepo && headRepo !== env.GITHUB_REPOSITORY) {
    const message = 'Build metadata unavailable for fork pull requests: read-only token; native CI validation continues.\n';
    if (env.GITHUB_STEP_SUMMARY) fs.appendFileSync(env.GITHUB_STEP_SUMMARY,message);
    console.log(message.trim());
    return {status:'unavailable',reason:'fork_pull_request'};
  }
  const base = {version:1,kind:'manifest',repository:env.GITHUB_REPOSITORY,workflow_path:env.GITHUB_WORKFLOW_REF.split('@')[0].split('/').slice(2).join('/'),run_id:Number(env.GITHUB_RUN_ID),run_number:Number(env.GITHUB_RUN_NUMBER),run_attempt:Number(env.GITHUB_RUN_ATTEMPT),event:env.GITHUB_EVENT_NAME,source_sha:event.pull_request?.head.sha || env.GITHUB_SHA,execution_sha:env.GITHUB_SHA,pr_number:event.pull_request?.number || null,selected:selection(env.MATRIX_JSON,env.HAS_BUILDS)};
  const api = async (path,method='GET',body) => {
    const response = await fetch(`${env.GITHUB_API_URL || 'https://api.github.com'}${path}`,{method,headers:{Authorization:`Bearer ${env.GITHUB_TOKEN}`,Accept:'application/vnd.github+json','X-GitHub-Api-Version':'2022-11-28'},...(body ? {body:JSON.stringify(body)} : {})});
    requireValid(response.ok,`Checks API failed (${response.status})`);
    return response.json();
  };
  let metadata = base;
  const prefix = `build-metadata:v1:${base.repository}:${base.run_id}:`;
  if (mode === 'running' || mode === 'result') {
    const service = JSON.parse(env.SERVICE_JSON);
    requireValid(base.selected.some(s => s.service === service.service && s.image_name === service.image_name),'Service outside selection');
    const progress = mode === 'running' ? {state:'running',pushed:false} : outcome(JSON.parse(env.STEPS_JSON),env.PUSH_ALLOWED === 'true',env.JOB_STATUS);
    metadata = {...base,kind:'result',service:service.service,image_name:service.image_name,...progress,source_attempt:base.run_attempt};
    if (progress.pushed) {
      const treeTag = imageTag === 'tree' || base.event === 'pull_request';
      const tag = treeTag ? `tree-${env.TREE_SHA}` : base.execution_sha;
      requireValid(/^[a-f0-9]{40}$/.test(treeTag ? env.TREE_SHA : tag),'Invalid image tag');
      metadata.image_refs = [`ghcr.io/${base.repository.split('/')[0].toLowerCase()}/${service.image_name}:${tag}`];
      if (env.IMAGE_DIGEST) metadata.digest = env.IMAGE_DIGEST;
    }
  } else if (mode === 'final') {
    const run = await api(`/repos/${base.repository}/actions/runs/${base.run_id}`);
    requireValid(!base.selected.length || env.MATRIX_RESULT === 'success', 'Current matrix did not succeed');
    requireValid(run.id === base.run_id && run.run_attempt === base.run_attempt && run.run_number === base.run_number && run.path === base.workflow_path && run.event === base.event && run.repository?.full_name === base.repository && run.head_sha === (event.pull_request?.head.sha || base.execution_sha), 'Workflow identity changed');
    const suite = await api(`/repos/${base.repository}/check-suites/${run.check_suite_id}`);
    requireValid(Number.isSafeInteger(suite.app?.id), 'Missing Actions app identity');
    const checks = await listChecks(api,base.repository,base.source_sha);
    // The final record must belong to a run whose selection was published.
    const manifests = new Map();
    for (const check of checks) {
      if (check.name !== 'build-metadata' || check.app?.id !== suite.app.id || check.head_sha !== base.source_sha || !check.external_id?.startsWith(prefix) || !check.external_id.endsWith(':manifest')) continue;
      const {raw,metadata:manifest} = transportPayload(check.output);
      requireValid(manifest.kind === 'manifest' && check.external_id === externalID(manifest) && check.status === 'completed' && check.conclusion === 'neutral' && manifest.run_attempt <= base.run_attempt && sameRun(base,manifest), 'Conflicting manifest');
      requireValid(!manifests.has(manifest.run_attempt) || manifests.get(manifest.run_attempt) === raw, 'Conflicting duplicate manifest');
      manifests.set(manifest.run_attempt,raw);
    }
    requireValid(manifests.size > 0, 'Missing current manifest');
    const records = [];
    const replicas = new Map();
    for (const check of checks) {
      if (check.name !== 'build-metadata-result' || check.app?.id !== suite.app?.id || check.head_sha !== base.source_sha) continue;
      // Other runs on this commit are unrelated. Our external ID is immutable.
      if (!check.external_id?.startsWith(prefix)) continue;
      const {raw,metadata:r} = transportPayload(check.output);
      requireValid(check.external_id === externalID(r) && check.status === 'completed' && check.conclusion === 'neutral','Invalid transport');
      // Validate every replica before collapsing it. IDs and creation times may
      // change when GitHub clones retained jobs after the aggregate completed.
      aggregate(base,[r],env.MATRIX_RESULT);
      const identity = JSON.stringify([check.external_id,check.name,check.app.id,check.head_sha,check.status,check.conclusion]);
      if (replicas.has(identity)) {
        requireValid(replicas.get(identity) === raw, 'Conflicting result replicas');
        continue;
      }
      replicas.set(identity,raw);
      records.push(r);
    }
    metadata = aggregate(base,records,env.MATRIX_RESULT);
    requireValid(metadata.results.every(r => r.state === 'success'), 'Incomplete or failed matrix results');
    const needs = JSON.parse(env.NEEDS_JSON);
    requireValid(Object.values(needs).every(n => ['success','skipped'].includes(n.result)), 'Upstream failure');
  } else requireValid(mode === 'manifest','Invalid publisher mode');
  const summary = JSON.stringify(metadata);
  requireValid(Buffer.byteLength(summary) <= 65536,'Metadata too large');
  const body = {name:metadata.kind === 'result' ? 'build-metadata-result' : 'build-metadata',head_sha:base.source_sha,external_id:externalID(metadata),status:'completed',conclusion:'neutral',output:{title:'Build metadata v1',summary:`Build metadata v1: ${metadata.kind}`,text:summary}};
  const id = mode === 'result' ? Number(env.RESULT_CHECK_ID) : 0;
  requireValid(mode !== 'result' || id > 0,'Missing running check');
  if (mode === 'result') {
    // Only finalize the exact running record this job published.
    const running = await api(`/repos/${base.repository}/check-runs/${id}`);
    requireValid(Number.isSafeInteger(running.check_suite?.id), 'Missing running check suite');
    const suite = await api(`/repos/${base.repository}/check-suites/${running.check_suite.id}`);
    const {metadata:r} = transportPayload(running.output);
    requireValid(running.id === id && Number.isSafeInteger(suite.app?.id) && running.app?.id === suite.app.id && running.name === body.name && running.external_id === body.external_id && running.head_sha === base.source_sha && running.status === 'completed' && running.conclusion === 'neutral' && r.kind === 'result' && r.state === 'running' && r.pushed === false && r.run_attempt === base.run_attempt && r.source_attempt === base.run_attempt && r.service === metadata.service && r.image_name === metadata.image_name && sameRun(base,r), 'Invalid running result binding');
  }
  const check = await api(`/repos/${base.repository}/check-runs${id ? '/'+id : ''}`,id ? 'PATCH' : 'POST',body);
  if (mode === 'running') fs.appendFileSync(env.GITHUB_OUTPUT,`check_id=${check.id}\n`);
}
module.exports = {parseArgs,parseStrict,selection,aggregate,outcome,listChecks,externalID,main};
if (require.main === module) main().catch(error => { console.error(error.message); process.exitCode=1; });
