const { test } = require('node:test');
const assert = require('node:assert/strict');
const m = require('./build-metadata.cjs');
const base = {version:1,kind:'manifest',repository:'owner/repo',workflow_path:'.github/workflows/ci.yml',run_id:1,run_number:2,run_attempt:2,event:'push',source_sha:'a'.repeat(40),execution_sha:'b'.repeat(40),pr_number:null,selected:[{service:'api',image_name:'api'}]};
const result = (attempt,state='success',pushed=true) => ({...base,kind:'result',run_attempt:attempt,service:'api',image_name:'api',source_attempt:attempt,state,pushed});
test('aggregate fences partial reruns and newest results', () => {
  const cases = [
    {name:'retained success',records:[result(1)],matrix:'success',state:'success'},
    {name:'matrix failed before publication',records:[result(1)],matrix:'failure',state:'running'},
    {name:'new failure wins',records:[result(1),result(2,'failure',false)],matrix:'failure',state:'failure'},
    {name:'new progress wins',records:[result(1),result(2,'running',false)],matrix:'success',state:'running'},
    {name:'compile only',records:[result(2,'success',false)],matrix:'success',state:'success'},
    {name:'cancelled',records:[result(2,'cancelled',false)],matrix:'cancelled',state:'cancelled'}
  ];
  for (const c of cases) assert.equal(m.aggregate(base,c.records,c.matrix).results[0].state,c.state,c.name);
});
test('rejects conflicting or foreign records', () => {
  for (const records of [[result(2),result(2)], [{...result(2),execution_sha:'c'.repeat(40)}], [{...result(2),image_name:'other'}]]) assert.throws(() => m.aggregate(base,records,'success'));
  assert.deepEqual(m.aggregate({...base,selected:[]},[],'skipped').results,[]);
  assert.throws(()=>m.aggregate({...base,selected:undefined},[],'success'));
});
test('push outcomes use step IDs and explicit push permission', () => {
  for (const c of [
    {steps:{cache_push:{outcome:'success'}},allowed:true,pushed:true},
    {steps:{image_push:{outcome:'success'}},allowed:false,pushed:false},
    {steps:{image_push:{outcome:'failure'}},allowed:true,pushed:false}
  ]) assert.equal(m.outcome(c.steps,c.allowed,'success').pushed,c.pushed);
});
test('pagination preserves independent concurrent results', async () => {
  const pages = {1:Array.from({length:100},(_,i)=>({id:i})),2:[{id:100}]};
  const records = await m.listChecks(async path => ({check_runs:pages[Number(new URL('https://api.github.com'+path).searchParams.get('page'))]}),'owner/repo',base.source_sha);
  assert.equal(records.length,101);
});
test('selection normalizes sentinel and rejects malformed input', () => {
  assert.deepEqual(m.selection('[{"service":"none"}]','false'),[]);
  for (const matrix of ['[{"service":"none","image_name":"none"}]','[{"service":"api","image_name":"api"},{"service":"api","image_name":"api"}]','null']) assert.throws(()=>m.selection(matrix,'true'));
  assert.throws(()=>m.selection('[]',undefined));
  const fixture = require('./testdata/build-metadata-v1.json');
  assert.deepEqual(m.aggregate(fixture,[],'skipped').results,[]);
});
test('publisher final binds API provenance and fails closed', async () => {
  const fs = require('node:fs');
  const os = require('node:os');
  const path = require('node:path');
  const dir = fs.mkdtempSync(path.join(os.tmpdir(),'metadata-'));
  const eventPath = path.join(dir,'event.json');
  fs.writeFileSync(eventPath,'{}');
  const savedFetch = global.fetch;
  const savedArgv = process.argv;
  try {
    process.argv = ['node','publisher','final'];
    const env = {GITHUB_EVENT_PATH:eventPath,GITHUB_REPOSITORY:'owner/repo',GITHUB_WORKFLOW_REF:'owner/repo/.github/workflows/ci.yml@refs/heads/main',GITHUB_RUN_ID:'1',GITHUB_RUN_NUMBER:'2',GITHUB_RUN_ATTEMPT:'2',GITHUB_EVENT_NAME:'push',GITHUB_SHA:'a'.repeat(40),MATRIX_JSON:'[{"service":"api","image_name":"api"}]',HAS_BUILDS:'true',MATRIX_RESULT:'success',NEEDS_JSON:'{"build":{"result":"success"}}'};
    const r = {...result(1),source_sha:'a'.repeat(40),execution_sha:'a'.repeat(40)};
    for (const c of [
      {name:'retained prior success',app:123,attempt:2,duplicate:false,fail:false},
      {name:'wrong app',app:9,attempt:2,duplicate:false,fail:true},
      {name:'newer rerun',app:123,attempt:3,duplicate:false,fail:true},
      {name:'post-final cloned result',app:123,attempt:2,duplicate:true,fail:false},
      {name:'canonical text with human summary',app:123,attempt:2,text:true,fail:false},
      {name:'identical dual payload',app:123,attempt:2,text:true,dual:true,fail:false},
      {name:'mixed transport replica',app:123,attempt:2,text:true,duplicate:true,legacyClone:true,fail:false},
      {name:'conflicting clone',app:123,attempt:2,duplicate:true,conflict:true,fail:true},
      {name:'conflicting dual payload',app:123,attempt:2,text:true,conflict:true,dual:true,fail:true},
      {name:'different raw clone',app:123,attempt:2,duplicate:true,whitespace:true,fail:true},
      {name:'invalid canonical text',app:123,attempt:2,text:true,invalid:true,fail:true},
      {name:'wrong workflow payload',app:123,attempt:2,foreign:true,fail:true},
      {name:'wrong head',app:123,attempt:2,wrongHead:true,fail:true},
      {name:'wrong external identity',app:123,attempt:2,wrongID:true,fail:true},
      {name:'publication failure',app:123,attempt:2,publishFailure:true,fail:true},
      {name:'matrix failed before publication',app:123,attempt:2,matrix:'failure',fail:true},
      {name:'actual rerun failure supersedes retained success',app:123,attempt:2,reexecuted:true,fail:true},
      {name:'current success with failed matrix',app:123,attempt:2,current:true,matrix:'failure',fail:true},
      {name:'missing manifest',app:123,attempt:2,manifest:'missing',fail:true},
      {name:'conflicting manifest selection',app:123,attempt:2,manifest:'conflict',fail:true},
      {name:'conflicting manifest replicas',app:123,attempt:2,manifest:'replica',fail:true},
      {name:'retained manifest replicas',app:123,attempt:2,manifest:'clone',fail:false},
      {name:'duplicate JSON key',app:123,attempt:2,text:true,dupKey:true,fail:true},
      {name:'duplicate JSON key in summary',app:123,attempt:2,text:true,dupSummary:true,fail:true},
      {name:'unknown metadata field',app:123,attempt:2,text:true,unknownField:true,fail:true},
      {name:'malformed checks page',app:123,attempt:2,badPage:true,fail:true},
      {name:'missing actions app',app:123,attempt:2,noSuiteApp:true,fail:true},
      {name:'wrong run id',app:123,attempt:2,wrongRun:true,fail:true}
    ]) {
      let published;
      global.fetch = async (url,options) => {
        let payload;
        if (options.method === 'POST') { if (c.publishFailure) return {ok:false,status:403}; published=JSON.parse(options.body);payload={id:42};}
        else if (url.includes('/actions/runs/')) payload={id:c.wrongRun ? 5 : 1,run_attempt:c.attempt,run_number:2,path:base.workflow_path,event:'push',repository:{full_name:'owner/repo'},head_sha:env.GITHUB_SHA,check_suite_id:7};
        else if (url.includes('/check-suites/')) payload=c.noSuiteApp ? {} : {app:{id:123}};
        else if (c.badPage) payload={};
        else {
          const current=c.current ? {...r,run_attempt:2,source_attempt:2} : r;
          let raw=JSON.stringify(c.foreign ? {...current,workflow_path:'.github/workflows/other.yml'} : c.unknownField ? {...current,extra:true} : current);
          if (c.dupKey) raw=raw.replace('{"version":1','{"version":1,"version":1');
          const other=JSON.stringify({...r,state:'failure',pushed:false});
          const record={id:10,name:'build-metadata-result',app:{id:c.app},head_sha:c.wrongHead ? 'd'.repeat(40) : env.GITHUB_SHA,external_id:m.externalID(current)+(c.wrongID ? ':wrong' : ''),status:'completed',conclusion:'neutral',output:c.text ? {text:c.invalid ? 'invalid' : raw,summary:c.dual ? (c.conflict ? other : raw) : c.dupSummary ? raw.replace('{"version":1','{"version":1,"version":1') : 'Image result'} : {summary:raw}};
          const clone={...record,id:99,output:c.legacyClone ? {summary:raw} : {...record.output,summary:c.conflict ? other : c.whitespace ? raw+' ' : record.output.summary}};
          const rerun={...record,id:100,external_id:m.externalID({...r,run_attempt:2}),output:{text:JSON.stringify({...r,run_attempt:2,source_attempt:2,state:'failure',pushed:false}),summary:'Reexecuted'}};
          const manifest={...r,kind:'manifest',run_attempt:1};
          for (const key of ['service','image_name','state','pushed','source_attempt']) delete manifest[key];
          const manifestCheck=raw => ({id:1,name:'build-metadata',app:{id:123},head_sha:env.GITHUB_SHA,external_id:m.externalID(manifest),status:'completed',conclusion:'neutral',output:{text:raw,summary:'Build metadata v1: manifest'}});
          const manifestRaw=JSON.stringify(c.manifest === 'conflict' ? {...manifest,selected:[{service:'other',image_name:'other'}]} : manifest);
          const manifests={missing:[],replica:[manifestCheck(manifestRaw),manifestCheck(manifestRaw+' ')],clone:[manifestCheck(manifestRaw),manifestCheck(manifestRaw)]}[c.manifest] || [manifestCheck(manifestRaw)];
          payload={check_runs:[...manifests,...(c.reexecuted ? [record,rerun] : c.duplicate ? [record,clone] : [record])]};
        }
        return {ok:true,json:async()=>payload};
      };
      if (c.fail) await assert.rejects(m.main({...env,MATRIX_RESULT:c.matrix || 'success'}),undefined,c.name);
      else {
        await m.main(env);
        assert.equal(published.head_sha,env.GITHUB_SHA);
        assert.equal(published.conclusion,'neutral');
        assert.equal(JSON.parse(published.output.text).results[0].source_attempt,1);
      }
    }
  } finally {global.fetch=savedFetch;process.argv=savedArgv;fs.rmSync(dir,{recursive:true,force:true});}
});
// Serves the running record the publisher wrote back as its own check run,
// so result mode binds to exactly what running mode published.
const serviceAPI = (state,fault) => async (url,options) => {
  if (fault === 'publication') return {ok:false,status:403};
  let payload = {id:42};
  if (options.method === 'POST' || options.method === 'PATCH') {
    const body = JSON.parse(options.body);
    state.published = body; state.method = options.method; state.url = url;
    if (options.method === 'POST' && body.name === 'build-metadata-result') state.running = body;
  } else if (url.includes('/check-suites/')) payload = {app:{id:7}};
  else {
    const running = JSON.parse(state.running.output.text);
    if (fault === 'identity') running.execution_sha = 'd'.repeat(40);
    if (fault === 'state') running.state = 'success';
    payload = {...state.running,id:42,app:{id:fault === 'app' ? 8 : 7},check_suite:fault === 'suite' ? undefined : {id:9},output:{text:JSON.stringify(running)}};
  }
  return {ok:true,json:async()=>payload};
};
const serviceEnv = (dir,overrides) => ({GITHUB_EVENT_PATH:path.join(dir,'event'),GITHUB_OUTPUT:path.join(dir,'output'),GITHUB_REPOSITORY:'owner/repo',GITHUB_WORKFLOW_REF:'owner/repo/.github/workflows/ci.yml@refs/pull/3/merge',GITHUB_RUN_ID:'1',GITHUB_RUN_NUMBER:'2',GITHUB_RUN_ATTEMPT:'2',GITHUB_EVENT_NAME:'pull_request',GITHUB_SHA:'b'.repeat(40),MATRIX_JSON:'[{"service":"api","image_name":"api"}]',HAS_BUILDS:'true',SERVICE_JSON:'{"service":"api","image_name":"api"}',RESULT_CHECK_ID:'42',STEPS_JSON:'{"image_push":{"outcome":"success"}}',PUSH_ALLOWED:'true',JOB_STATUS:'success',TREE_SHA:'a'.repeat(40),IMAGE_DIGEST:'sha256:123',...overrides});
const fs = require('node:fs'), os = require('node:os'), path = require('node:path');
test('publisher writes neutral source checks and updates service records', async () => {
  const dir=fs.mkdtempSync(path.join(os.tmpdir(),'metadata-'));
  const savedFetch=global.fetch;
  fs.writeFileSync(path.join(dir,'event'),JSON.stringify({pull_request:{number:3,head:{sha:'c'.repeat(40),repo:{full_name:'owner/repo'}}}}));
  try {
    const env=serviceEnv(dir,{});
    const state={};
    for (const c of [{mode:'manifest',kind:'manifest'},{mode:'running',kind:'result'},{mode:'result',kind:'result'}]) {
      global.fetch=serviceAPI(state);
      await m.main(env,[c.mode]);
      assert.equal(typeof state.published.output.text,'string');
      assert.match(state.published.output.summary,/^Build metadata v1: /);
      const data=JSON.parse(state.published.output.text);
      assert.equal(data.kind,c.kind);
      assert.equal(state.published.head_sha,'c'.repeat(40));
      assert.equal(data.execution_sha,'b'.repeat(40));
      assert.equal(state.method,c.mode==='result'?'PATCH':'POST');
      if(c.mode==='result') assert.deepEqual(data.image_refs,['ghcr.io/owner/api:tree-'+ 'a'.repeat(40)]);
    }
    assert.equal(fs.readFileSync(env.GITHUB_OUTPUT,'utf8'),'check_id=42\n');
    global.fetch=serviceAPI(state,'publication');
    await assert.rejects(m.main(env,['manifest']),/403/);
  } finally {global.fetch=savedFetch;fs.rmSync(dir,{recursive:true,force:true});}
});
test('result binds to its own running check and selects the image tag', async () => {
  const savedFetch=global.fetch;
  const sha='b'.repeat(40), tree='a'.repeat(40);
  const tests=[
    {name:'push default commit tag',event:'push',args:[],fault:'',ref:`ghcr.io/owner/api:${sha}`},
    {name:'push tree tag flag',event:'push',args:['--image-tag','tree'],fault:'',ref:`ghcr.io/owner/api:tree-${tree}`},
    {name:'push tree tag equals form',event:'push',args:['--image-tag=tree'],fault:'',ref:`ghcr.io/owner/api:tree-${tree}`},
    {name:'pull request explicit commit flag keeps tree tag',event:'pull_request',args:['--image-tag=commit'],fault:'',ref:`ghcr.io/owner/api:tree-${tree}`},
    {name:'wrong running app',event:'push',args:[],fault:'app',error:/Invalid running result binding/},
    {name:'wrong running identity',event:'push',args:[],fault:'identity',error:/Invalid running result binding/},
    {name:'running check already finished',event:'push',args:[],fault:'state',error:/Invalid running result binding/},
    {name:'running check without suite',event:'push',args:[],fault:'suite',error:/Missing running check suite/}
  ];
  for (const tc of tests) await (async () => {
    const dir=fs.mkdtempSync(path.join(os.tmpdir(),'metadata-'));
    try {
      fs.writeFileSync(path.join(dir,'event'),tc.event==='pull_request' ? JSON.stringify({pull_request:{number:3,head:{sha,repo:{full_name:'owner/repo'}}}}) : '{}');
      const env=serviceEnv(dir,{GITHUB_EVENT_NAME:tc.event,GITHUB_WORKFLOW_REF:'owner/repo/.github/workflows/ci.yml@refs/heads/main'});
      const state={};
      global.fetch=serviceAPI(state);
      await m.main(env,['running',...tc.args]);
      global.fetch=serviceAPI(state,tc.fault);
      if (tc.error) return assert.rejects(m.main(env,['result',...tc.args]),tc.error,tc.name);
      await m.main(env,['result',...tc.args]);
      assert.equal(state.url,'https://api.github.com/repos/owner/repo/check-runs/42',tc.name);
      assert.deepEqual(JSON.parse(state.published.output.text).image_refs,[tc.ref],tc.name);
    } finally {fs.rmSync(dir,{recursive:true,force:true});}
  })();
  global.fetch=savedFetch;
});
test('fork pull requests skip publication with a step summary', async () => {
  const savedFetch=global.fetch;
  for (const mode of ['manifest','running','result','final']) {
    const dir=fs.mkdtempSync(path.join(os.tmpdir(),'metadata-fork-'));
    try {
      fs.writeFileSync(path.join(dir,'event'),JSON.stringify({pull_request:{number:3,head:{sha:'c'.repeat(40),repo:{full_name:'contributor/repo'}}}}));
      global.fetch=async()=>{throw new Error('read-only fork token cannot publish checks');};
      const outcome=await m.main({GITHUB_EVENT_PATH:path.join(dir,'event'),GITHUB_EVENT_NAME:'pull_request',GITHUB_REPOSITORY:'owner/repo',GITHUB_STEP_SUMMARY:path.join(dir,'summary')},[mode]);
      assert.deepEqual(outcome,{status:'unavailable',reason:'fork_pull_request'},mode);
      assert.match(fs.readFileSync(path.join(dir,'summary'),'utf8'),/unavailable for fork pull requests/,mode);
    } finally {fs.rmSync(dir,{recursive:true,force:true});}
  }
  global.fetch=savedFetch;
});
test('parseArgs reads the mode and image tag flag', () => {
  const tests=[
    {name:'mode only',argv:['final'],want:{mode:'final',imageTag:'commit'}},
    {name:'separate flag value',argv:['result','--image-tag','tree'],want:{mode:'result',imageTag:'tree'}},
    {name:'equals flag value',argv:['result','--image-tag=commit'],want:{mode:'result',imageTag:'commit'}},
    {name:'unknown tag',argv:['result','--image-tag=latest'],error:/Invalid --image-tag/},
    {name:'missing tag value',argv:['result','--image-tag'],error:/Invalid --image-tag/},
    {name:'unknown flag',argv:['result','--repo=x'],error:/Unknown argument: --repo=x/}
  ];
  for (const tc of tests) {
    if (tc.error) assert.throws(()=>m.parseArgs(tc.argv),tc.error,tc.name);
    else assert.deepEqual(m.parseArgs(tc.argv),tc.want,tc.name);
  }
});
test('parseStrict rejects ambiguous or oversized JSON', () => {
  const tests=[
    {name:'plain object',raw:'{"a":{"b":[1,{"c":2}]}}',want:{a:{b:[1,{c:2}]}}},
    {name:'same key in sibling objects',raw:'[{"a":1},{"a":2}]',want:[{a:1},{a:2}]},
    {name:'top-level duplicate',raw:'{"version":1,"version":1}',error:/Duplicate JSON key/},
    {name:'nested duplicate',raw:'{"n":{"a":1,"a":2}}',error:/Duplicate JSON key/},
    {name:'escaped duplicate',raw:'{"a":1,"\\u0061":2}',error:/Duplicate JSON key/},
    {name:'oversized',raw:'"'+'x'.repeat(65536)+'"',error:/Invalid metadata payload/},
    {name:'not a string',raw:undefined,error:/Invalid metadata payload/}
  ];
  for (const tc of tests) {
    if (tc.error) assert.throws(()=>m.parseStrict(tc.raw),tc.error,tc.name);
    else assert.deepEqual(m.parseStrict(tc.raw),tc.want,tc.name);
  }
});
