import {chromium,expect} from '@playwright/test';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
const base=process.env.SAMPLE_URL||'http://127.0.0.1:4102';
const token=process.env.DEMO_CONTROL_TOKEN;
if(!token)throw new Error('DEMO_CONTROL_TOKEN is required; configure locally or supply a CI secret.');
const headers={'Authorization':`Bearer ${token}`,'Content-Type':'application/json'};
async function control(phase,duration=180){const r=await fetch(base+'/api/demo/scenario',{method:'POST',headers,body:JSON.stringify({phase,duration_seconds:duration})});if(!r.ok)throw new Error(`Control rejected (${r.status})`);return r.json();}
if(process.env.WATCHDOG==='true'){const r=await fetch(base+'/api/demo/heartbeat',{headers});const h=await r.json();if(h.day===new Date().toISOString().slice(0,10)&&h.success&&h.browser){console.log('Watchdog: successful browser heartbeat already exists today.');process.exit(0)}console.log('Watchdog: missing browser heartbeat; running bounded regression/recovery journey.');}
let executablePath=process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH;
if(!executablePath&&!fs.existsSync(chromium.executablePath())){
 const cache=process.env.PLAYWRIGHT_BROWSERS_PATH||path.join(os.homedir(),process.platform==='darwin'?'Library/Caches/ms-playwright':'.cache/ms-playwright');
 if(fs.existsSync(cache))for(const folder of fs.readdirSync(cache).filter(f=>/^chromium-\d+$/.test(f)).sort().reverse())for(const rel of ['chrome-mac-arm64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing','chrome-linux64/chrome','chrome-linux/chrome']){const candidate=path.join(cache,folder,rel);if(fs.existsSync(candidate)){executablePath=candidate;break}}
}
const browser=await chromium.launch({headless:true,executablePath});
const page=await browser.newPage({viewport:{width:1280,height:960}});
const ids=new Set(),errors=[];page.on('pageerror',error=>errors.push(String(error)));
page.on('response',async response=>{if(response.url().startsWith(base+'/api/')){const id=response.headers()['x-trace-id'];if(id)ids.add(id)}});
const evidence={app:base,started:new Date().toISOString(),trace_ids:[],browser:true,success:false};
fs.mkdirSync('test-results',{recursive:true});
try{
 await control('healthy');await page.goto(base,{waitUntil:'networkidle'});await expect(page.getByRole('heading',{name:'Write a review'})).toBeVisible();
 async function submit(product,status){await page.getByTestId('product').selectOption(product);const old=await page.getByTestId('last-review').getAttribute('data-review-id').catch(()=>null);await page.getByTestId('submit').click();await expect(page.getByTestId('last-review')).not.toHaveAttribute('data-review-id',old||'');await expect(page.getByTestId('last-review')).toHaveAttribute('data-status',status,{timeout:130000});return page.getByTestId('last-review').getAttribute('data-review-id');}
 evidence.healthy_review=await submit('demo-product','approved');
 evidence.release=(await control('regression',180)).release;
 evidence.failed_review=await submit('demo-product','failed');
 evidence.control_review=await submit('control-product','approved');
 await page.screenshot({path:'test-results/regression.png',fullPage:true});
 await control('recover');evidence.recovered_review=await submit('demo-product','approved');
 evidence.rum=await page.evaluate(()=>window.CorridoraRum?.getInitializationStatus?.()||{state:'not loaded'});
 if(await page.locator('script[data-service="sample-reviews-web"]').count() && evidence.rum!=='ready' && evidence.rum?.state!=='ready')throw new Error('Configured RUM did not initialize: '+JSON.stringify(evidence.rum));
 await page.waitForTimeout(4000);await page.evaluate(()=>window.dispatchEvent(new Event('pagehide')));await page.waitForTimeout(1000);
 if(errors.length)throw new Error('Browser errors: '+errors.join('; '));
 evidence.trace_ids=[...ids];evidence.success=true;
 if(process.env.CAPTURE_FILE){
  const rows=fs.readFileSync(process.env.CAPTURE_FILE,'utf8').trim().split('\n').filter(Boolean).map(line=>JSON.parse(line));
  const spans=[];const logs=[];const metrics=[];
  const normalized=id=>/^[a-f\d]{16}$|^[a-f\d]{32}$/i.test(id||'')?id.toLowerCase():Buffer.from(id||'','base64').toString('hex');
  for(const row of rows){for(const resource of row.data.resourceSpans||[])for(const scope of resource.scopeSpans||[])for(const span of scope.spans||[])spans.push({...span,traceId:normalized(span.traceId),spanId:normalized(span.spanId),parentSpanId:normalized(span.parentSpanId),resource:resource.resource,rum:row.rum});for(const resource of row.data.resourceLogs||[])for(const scope of resource.scopeLogs||[])logs.push(...scope.logRecords||[]);for(const resource of row.data.resourceMetrics||[])for(const scope of resource.scopeMetrics||[])metrics.push(...scope.metrics||[])}
  const clients=spans.filter(s=>(s.kind===3||s.kind==='SPAN_KIND_CLIENT')&&s.rum);
  const servers=spans.filter(s=>s.kind===2||s.kind==='SPAN_KIND_SERVER');
  const pairs=servers.flatMap(server=>clients.filter(client=>client.traceId===server.traceId&&client.spanId===server.parentSpanId).map(client=>({trace_id:server.traceId,client_span_id:client.spanId,server_span_id:server.spanId,server_name:server.name})));
  if(!pairs.length)throw new Error('No captured real RUM CLIENT → Go SERVER parentage. A synthetic traceparent is insufficient.');
  if(!logs.length||!metrics.length)throw new Error('Missing captured OTLP logs or metrics.');
  evidence.captured_parentage=pairs;evidence.captured_logs=logs.length;evidence.captured_metrics=metrics.length;
  const sessionAttributes=spans.flatMap(s=>s.attributes||[]).filter(a=>['session.id','session_id','rum.session.id'].includes(a.key));evidence.session_ids=[...new Set(sessionAttributes.map(a=>a.value?.stringValue).filter(Boolean))];
 }
 const heartbeat=await fetch(base+'/api/demo/heartbeat',{method:'POST',headers,body:JSON.stringify({success:true,browser:true,trace_id:evidence.trace_ids[0]||''})});if(!heartbeat.ok)throw new Error('Heartbeat write failed');
 console.log(JSON.stringify({...evidence,trace_ids:evidence.trace_ids.slice(0,5),captured_parentage:evidence.captured_parentage?.slice(0,3),captured_pair_count:evidence.captured_parentage?.length},null,2));
 if(!process.env.CAPTURE_FILE)console.log('Browser workflow verified. Stored RUM/backend correlation and native provider evidence remain tenant UI checks.');
}catch(error){evidence.success=false;evidence.error=String(error);try{await page.screenshot({path:'test-results/failure.png',fullPage:true})}catch{};throw error;}
finally{await control('recover').catch(()=>{});evidence.finished=new Date().toISOString();fs.writeFileSync('test-results/journey.json',JSON.stringify(evidence,null,2));await browser.close();}
