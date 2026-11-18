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
      {name:'duplicate transport',app:123,attempt:2,duplicate:true,fail:true}
    ]) {
      let published;
      global.fetch = async (url,options) => {
        let payload;
        if (options.method === 'POST') {published=JSON.parse(options.body);payload={id:42};}
        else if (url.includes('/actions/runs/')) payload={run_attempt:c.attempt,run_number:2,path:base.workflow_path,event:'push',repository:{full_name:'owner/repo'},head_sha:env.GITHUB_SHA,check_suite_id:7};
        else if (url.includes('/check-suites/')) payload={app:{id:123}};
        else {
          const record={name:'build-metadata-result',app:{id:c.app},head_sha:env.GITHUB_SHA,external_id:m.externalID(r),status:'completed',conclusion:'neutral',output:{summary:JSON.stringify(r)}};
          payload={check_runs:c.duplicate ? [record,record] : [record]};
        }
        return {ok:true,json:async()=>payload};
      };
      if (c.fail) await assert.rejects(m.main(env),undefined,c.name);
      else {
        await m.main(env);
        assert.equal(published.head_sha,env.GITHUB_SHA);
        assert.equal(published.conclusion,'neutral');
        assert.equal(JSON.parse(published.output.summary).results[0].source_attempt,1);
      }
    }
  } finally {global.fetch=savedFetch;process.argv=savedArgv;fs.rmSync(dir,{recursive:true,force:true});}
});
test('publisher writes neutral source checks and updates service records', async () => {
  const fs = require('node:fs'), os = require('node:os'), path = require('node:path');
  const dir=fs.mkdtempSync(path.join(os.tmpdir(),'metadata-'));
  const savedFetch=global.fetch, savedArgv=process.argv;
  fs.writeFileSync(path.join(dir,'event'),JSON.stringify({pull_request:{number:3,head:{sha:'c'.repeat(40)}}}));
  try {
    const env={GITHUB_EVENT_PATH:path.join(dir,'event'),GITHUB_OUTPUT:path.join(dir,'output'),GITHUB_REPOSITORY:'owner/repo',GITHUB_WORKFLOW_REF:'owner/repo/.github/workflows/ci.yml@refs/pull/3/merge',GITHUB_RUN_ID:'1',GITHUB_RUN_NUMBER:'2',GITHUB_RUN_ATTEMPT:'2',GITHUB_EVENT_NAME:'pull_request',GITHUB_SHA:'b'.repeat(40),MATRIX_JSON:'[{"service":"api","image_name":"api"}]',HAS_BUILDS:'true',SERVICE_JSON:'{"service":"api","image_name":"api"}',RESULT_CHECK_ID:'42',STEPS_JSON:'{"image_push":{"outcome":"success"}}',PUSH_ALLOWED:'true',JOB_STATUS:'success',TREE_SHA:'a'.repeat(40),IMAGE_DIGEST:'sha256:123'};
    for (const c of [{mode:'manifest',kind:'manifest'},{mode:'running',kind:'result'},{mode:'result',kind:'result'}]) {
      let published,method;
      global.fetch=async(url,options)=>{published=JSON.parse(options.body);method=options.method;return {ok:true,json:async()=>({id:42})};};
      process.argv=['node','publisher',c.mode];
      await m.main(env);
      const data=JSON.parse(published.output.summary);
      assert.equal(data.kind,c.kind);
      assert.equal(published.head_sha,'c'.repeat(40));
      assert.equal(data.execution_sha,'b'.repeat(40));
      assert.equal(method,c.mode==='result'?'PATCH':'POST');
      if(c.mode==='result') assert.deepEqual(data.image_refs,['ghcr.io/owner/api:tree-'+ 'a'.repeat(40)]);
    }
    assert.equal(fs.readFileSync(env.GITHUB_OUTPUT,'utf8'),'check_id=42\n');
    global.fetch=async()=>({ok:false,status:403});
    await assert.rejects(m.main(env),/403/);
  } finally {global.fetch=savedFetch;process.argv=savedArgv;fs.rmSync(dir,{recursive:true,force:true});}
});
