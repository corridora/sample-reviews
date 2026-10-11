"""Configure/deploy/run a resource-owned AWS sample. No shell command interpolation."""
import argparse
import base64
import getpass
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import zipfile

ROOT=Path(__file__).resolve().parents[1]
LOCAL=ROOT/'.local'
SECRET_NAMES={'CORRIDORA_INGEST_KEY','CORRIDORA_RUM_KEY','DEMO_CONTROL_TOKEN','DOWNSTREAM_AUTH_TOKEN'}
DEFAULTS={
 'SAMPLE_MODE':'local','PORT':'4102','CORRIDORA_TARGET':'own',
 'CORRIDORA_OTLP_ENDPOINT':'https://ingest.corridora.com',
 'CORRIDORA_RUM_SCRIPT_URL':'https://rum.corridora.com/corridora-rum.js',
 'OTEL_SERVICE_NAME':'sample-reviews-api','OTEL_SERVICE_NAMESPACE':'corridora-samples.reviews',
 'DEPLOYMENT_ENVIRONMENT':'development','AWS_PROFILE':'default','AWS_REGION':'us-east-1',
 'AWS_ACCOUNT_ID':'','STACK_PREFIX':'sample-reviews','EXISTING_SECRET_ARN':'',
 'DEMO_DAILY_START_UTC':'13:20','DEMO_WINDOW_SECONDS':'600',
 'ENABLE_PROVIDER_FORWARDING':'false','ENABLE_SYNTHETICS':'false',
 'RECOMMENDER_URL':'','PUBLIC_ORIGIN':'','TRACKER_SIMULATOR_URL':'','SYNTHETIC_GITHUB_REPOSITORY':''}

def private_json(path, value):
 path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
 os.chmod(path.parent,0o700)
 fd,temporary=tempfile.mkstemp(dir=path.parent,prefix='.write-')
 try:
  with os.fdopen(fd,'w') as file:json.dump(value,file,indent=2);file.write('\n')
  os.chmod(temporary,0o600);os.replace(temporary,path)
 finally:
  if os.path.exists(temporary):os.unlink(temporary)

def read_json(path,private=False):
 if not path.exists():return {}
 if private and (path.stat().st_mode & 0o077):raise ValueError(f'{path} must be owner-only (chmod 600)')
 return json.loads(path.read_text())

def load(config=None):
 path=Path(config) if config else LOCAL/'config.json'
 values={**DEFAULTS,**read_json(path),**read_json(LOCAL/'secrets.json',True)}
 # A supplied config can include secrets only with owner-only permissions.
 if any(k in read_json(path) for k in SECRET_NAMES):read_json(path,True)
 for key in set(DEFAULTS)|SECRET_NAMES:
  if key in os.environ:values[key]=os.environ[key]
 return {k:str(v) for k,v in values.items()}

def validate(c):
 if c['SAMPLE_MODE'] not in {'local','cloud'}:raise ValueError('SAMPLE_MODE must be local or cloud')
 if c['CORRIDORA_TARGET'] not in {'own','demo'}:raise ValueError('CORRIDORA_TARGET must be own or demo')
 if not re.fullmatch(r'[a-z][a-z0-9-]{2,34}',c['STACK_PREFIX']):raise ValueError('STACK_PREFIX: lowercase letters, numbers, hyphens, 3..35 characters (fits the owned S3 bucket name)')
 if c['EXISTING_SECRET_ARN'] and not re.fullmatch(r'arn:(aws|aws-us-gov|aws-cn):secretsmanager:[a-z0-9-]+:\d{12}:secret:[A-Za-z0-9/_+=.@-]+',c['EXISTING_SECRET_ARN']):raise ValueError('EXISTING_SECRET_ARN must be a dedicated Secrets Manager ARN')
 if not c['PORT'].isdigit() or not 1024<=int(c['PORT'])<=65535:raise ValueError('PORT must be 1024..65535')
 if not c['DEMO_WINDOW_SECONDS'].isdigit() or not 1<=int(c['DEMO_WINDOW_SECONDS'])<=600:raise ValueError('window must be 1..600 seconds')
 if not re.fullmatch(r'(?:[01]\d|2[0-3]):[0-5]\d',c['DEMO_DAILY_START_UTC']):raise ValueError('daily start must be HH:MM in UTC')
 for name in ['ENABLE_PROVIDER_FORWARDING','ENABLE_SYNTHETICS']:
  if c[name] not in {'true','false'}:raise ValueError(f'{name} must be true or false')
 for name in ['CORRIDORA_OTLP_ENDPOINT','CORRIDORA_RUM_SCRIPT_URL','RECOMMENDER_URL','PUBLIC_ORIGIN','TRACKER_SIMULATOR_URL']:
  if not c[name]:continue
  u=urllib.parse.urlparse(c[name]);local=c['SAMPLE_MODE']=='local' and u.hostname in {'localhost','127.0.0.1'} and u.scheme=='http'
  if not u.hostname or u.username or u.password or u.query or u.fragment or (u.scheme!='https' and not local):raise ValueError(f'{name} requires HTTPS (loopback HTTP allowed in local mode), no credentials/query/fragment')
 if not c.get('DEMO_CONTROL_TOKEN'):raise ValueError('DEMO_CONTROL_TOKEN required')
 if c['CORRIDORA_TARGET']=='demo':c['DEPLOYMENT_ENVIRONMENT']='demo'
 if c['SAMPLE_MODE']=='cloud' or c['CORRIDORA_TARGET']=='demo':
  for key in ['CORRIDORA_INGEST_KEY','CORRIDORA_RUM_KEY']:
   if not c.get(key):raise ValueError(f'{key} required; Demo credentials must be supplied by an operator')
 if c['SAMPLE_MODE']=='cloud' and c['ENABLE_SYNTHETICS']=='true' and not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+',c['SYNTHETIC_GITHUB_REPOSITORY']):raise ValueError('Enabled browser synthetics require SYNTHETIC_GITHUB_REPOSITORY=owner/repo with the current daily workflow published')
 return c

def configure(args):
 c=load(args.config)
 prompts=[('SAMPLE_MODE','Mode (local/cloud)'),('CORRIDORA_TARGET','Tenant (own/demo; Demo requires operator keys)'),('CORRIDORA_OTLP_ENDPOINT','Corridora ingest base URL'),('CORRIDORA_RUM_SCRIPT_URL','RUM script URL'),('CORRIDORA_INGEST_KEY','Backend ingest key (blank disables local export)'),('CORRIDORA_RUM_KEY','Visible browser RUM key'),('OTEL_SERVICE_NAME','API service name (maintained cloud name is sample-reviews-api)'),('OTEL_SERVICE_NAMESPACE','Service namespace'),('DEPLOYMENT_ENVIRONMENT','Environment'),('PORT','Local port'),('DEMO_CONTROL_TOKEN','Scenario/control secret (blank generates a local token)'),('DEMO_DAILY_START_UTC','Daily UTC start'),('DEMO_WINDOW_SECONDS','Window seconds (max 600)'),('ENABLE_SYNTHETICS','Enable Scheduler probes and repository browser journey (true/false)'),('ENABLE_PROVIDER_FORWARDING','Enable CloudWatch platform logs/metric bridge (true/false)'),('RECOMMENDER_URL','Controlled recommender origin (blank uses fixture)'),('DOWNSTREAM_AUTH_TOKEN','Dedicated dependency bearer token (optional)')]
 if not args.non_interactive:
  for key,label in prompts:
   old=c.get(key,'');suffix='configured' if key in SECRET_NAMES and old else old
   answer=getpass.getpass(f'{label} [{suffix}]: ') if key in SECRET_NAMES else input(f'{label} [{suffix}]: ')
   if answer:c[key]=answer
  if c['SAMPLE_MODE']=='cloud':
   for key,label in [('AWS_PROFILE','Authenticated AWS CLI profile'),('AWS_REGION','AWS region'),('STACK_PREFIX','Dedicated stack prefix (reuse only this setup inventory)'),('EXISTING_SECRET_ARN','Existing DEDICATED config secret ARN; blank creates one')]:
    answer=input(f'{label} [{c[key]}]: ')
    if answer:c[key]=answer
   print('Queue configuration: visibility=30s, Lambda timeout=5s, maximum receives=3; fixed to bound retries.')
   print('Resources are created in an owned stack. Other existing buckets/queues/tables are never adopted.')
   if c['ENABLE_SYNTHETICS']=='true':
    answer=input(f'GitHub owner/repo for browser schedules (current daily.yml must be published) [{c["SYNTHETIC_GITHUB_REPOSITORY"]}]: ')
    if answer:c['SYNTHETIC_GITHUB_REPOSITORY']=answer
 c['DEMO_CONTROL_TOKEN']=c.get('DEMO_CONTROL_TOKEN') or secrets.token_urlsafe(32)
 validate(c)
 private_json(LOCAL/'config.json',{k:v for k,v in c.items() if k not in SECRET_NAMES})
 private_json(LOCAL/'secrets.json',{k:c.get(k,'') for k in SECRET_NAMES})
 inventory=read_json(LOCAL/'inventory.json');inventory.setdefault('owner',str(uuid.uuid4()));private_json(LOCAL/'inventory.json',inventory)
 print('Saved owner-only .local/config.json and .local/secrets.json. Secrets are Git-ignored.')
 if not c.get('CORRIDORA_INGEST_KEY') or not c.get('CORRIDORA_RUM_KEY'):print('Local app works; complete instrumentation verification requires your own scoped keys or a local capture sink.')
 return c

def version():
 digest=hashlib.sha256()
 for folder in ['cmd','internal','web/src']:
  for file in sorted((ROOT/folder).rglob('*')):
   if file.is_file():digest.update(str(file.relative_to(ROOT)).encode());digest.update(file.read_bytes())
 for name in ['go.mod','go.sum','package-lock.json','web/index.html','web/vite.config.js']:
  digest.update((ROOT/name).read_bytes())
 return 'sha256-'+digest.hexdigest()[:16]

def environment(c):
 result={**os.environ,**c,'SERVICE_VERSION':version(),'GOCACHE':str(LOCAL/'go-cache')}
 result['WEB_ROOT']=str(ROOT/'web/dist')
 return result

def run_command(argv,c=None,env=None,capture=False,input_text=None):
 if not shutil.which(argv[0]):raise RuntimeError(f'Missing prerequisite: {argv[0]}')
 result=subprocess.run(argv,cwd=ROOT,env=env or environment(c) if c else env,input=input_text,text=True,stdout=subprocess.PIPE if capture else None,stderr=subprocess.PIPE if capture else None)
 if result.returncode:
  text=(result.stderr or '')[-3000:]
  for key in SECRET_NAMES:
   if c and c.get(key):text=text.replace(c[key],'[redacted]')
  raise RuntimeError(f'{argv[0]} failed ({result.returncode}): {text}')
 return result.stdout.strip() if capture else ''

def aws(c,*arguments,json_result=False):
 out=run_command(['aws','--profile',c['AWS_PROFILE'],'--region',c['AWS_REGION'],'--no-cli-pager',*arguments,'--output','json'],c,capture=True)
 return json.loads(out) if json_result and out else out

def github_variables(c,repo):
 return {v['name']:v['value'] for v in json.loads(run_command(['gh','variable','list','--repo',repo,'--json','name,value'],c,capture=True))}

def github_preflight(c,inventory):
 repo=c['SYNTHETIC_GITHUB_REPOSITORY']
 run_command(['gh','auth','status'],c,capture=True)
 # Do not publish or overwrite source. Installing the runner requires its reviewed code.
 try:remote=json.loads(run_command(['gh','api',f'repos/{repo}/contents/.github/workflows/daily.yml?ref=main'],c,capture=True))
 except RuntimeError as error:raise RuntimeError('Publish the current .github/workflows/daily.yml to the selected repository main branch before enabling synthetics') from error
 if base64.b64decode(remote['content'])!=(ROOT/'.github/workflows/daily.yml').read_bytes():raise RuntimeError('Selected repo daily.yml differs from this checkout; publish the current reviewed workflow before enabling synthetics')
 variables=github_variables(c,repo)
 owner=variables.get('SAMPLE_REVIEWS_OWNER')
 if owner and owner!=inventory['owner']:raise RuntimeError('GitHub synthetic configuration belongs to another inventory; refusing to replace it')
 if not owner and any(k in variables for k in ['SAMPLE_URL','ENABLE_SYNTHETICS']):raise RuntimeError('Existing GitHub synthetic variables have no matching ownership marker; refusing to adopt them')
 return repo

def github_install(c,inventory,url):
 repo=github_preflight(c,inventory)
 # Save intent first so interrupted setup can safely finish or tear down CI configuration.
 inventory['github_repository']=repo;private_json(LOCAL/'inventory.json',inventory)
 for name,value in [('SAMPLE_REVIEWS_OWNER',inventory['owner']),('SAMPLE_URL',url),('ENABLE_SYNTHETICS','true')]:run_command(['gh','variable','set',name,'--repo',repo,'--body',value],c,capture=True)
 run_command(['gh','secret','set','DEMO_CONTROL_TOKEN','--repo',repo],c,capture=True,input_text=c['DEMO_CONTROL_TOKEN'])
 run_command(['gh','workflow','enable','daily.yml','--repo',repo],c,capture=True)
 inventory['github_enabled']=True;private_json(LOCAL/'inventory.json',inventory)
 print(f'Installed browser schedule/watchdog variables and secret in {repo}; enabled daily.yml.')

def github_disable(c,inventory,destroy=False):
 repo=inventory.get('github_repository')
 if not repo:return
 if github_variables(c,repo).get('SAMPLE_REVIEWS_OWNER')!=inventory['owner']:raise RuntimeError('GitHub runner ownership mismatch; refusing to alter it')
 run_command(['gh','workflow','disable','daily.yml','--repo',repo],c,capture=True)
 run_command(['gh','variable','set','ENABLE_SYNTHETICS','--repo',repo,'--body','false'],c,capture=True)
 if destroy:
  run_command(['gh','secret','delete','DEMO_CONTROL_TOKEN','--repo',repo],c,capture=True)
  for name in ['SAMPLE_URL','ENABLE_SYNTHETICS','SAMPLE_REVIEWS_OWNER']:run_command(['gh','variable','delete',name,'--repo',repo],c,capture=True)
 inventory['github_enabled']=False;private_json(LOCAL/'inventory.json',inventory)

def build(c,cloud=False):
 run_command(['npm','ci','--ignore-scripts'],c);run_command(['npm','run','build'],c)
 directory=ROOT/'build';directory.mkdir(exist_ok=True)
 e=environment(c)
 if cloud:e.update(GOOS='linux',GOARCH='arm64',CGO_ENABLED='0')
 binary=directory/('bootstrap' if cloud else 'reviews')
 run_command(['go','build','-trimpath','-ldflags',f'-s -w -X main.buildVersion={version()}','-o',str(binary),'./cmd/reviews'],c,env=e)
 if not cloud:private_json(LOCAL/'local-build.json',{'version':version(),'binary':str(binary)})
 if cloud:
  with zipfile.ZipFile(directory/'lambda.zip','w',zipfile.ZIP_DEFLATED) as archive:
   info=zipfile.ZipInfo('bootstrap');info.external_attr=(stat.S_IFREG|0o755)<<16;info.compress_type=zipfile.ZIP_DEFLATED;archive.writestr(info,binary.read_bytes())
   for file in (ROOT/'web/dist').rglob('*'):
    if file.is_file():archive.write(file,'web/'+str(file.relative_to(ROOT/'web/dist')))
 return binary

def plan(c,inventory=None):
 return {'mode':c['SAMPLE_MODE'],'account':c.get('AWS_ACCOUNT_ID') or 'resolved from current CLI profile at apply','profile':c['AWS_PROFILE'],'region':c['AWS_REGION'],'stack':c['STACK_PREFIX'],'owner':(inventory or {}).get('owner','created by configure'),'resources':['HTTP API','3-day CloudWatch logs','Go API Lambda (128 MB, concurrency 5)','Go moderation Lambda (128 MB, 5s timeout, concurrency 2)','DynamoDB on-demand + 7-day TTL','private S3 thumbnails + 7-day lifecycle','SQS (30s visibility, max 3 receives) + 1-day DLQ','dedicated Secrets Manager secret or explicitly supplied dedicated ARN','owned S3 build artifacts'], 'optional':{'Scheduler API probes':c['ENABLE_SYNTHETICS'],'browser schedule/watchdog repository':c['SYNTHETIC_GITHUB_REPOSITORY'] if c['ENABLE_SYNTHETICS']=='true' else 'disabled','CloudWatch bridge + 4 metric queries/15min':c['ENABLE_PROVIDER_FORWARDING']},'cost':'Estimate $1–3/month at <=100k requests, <0.5GB egress, 50MB logs; not a billing cap. No NAT/VPC/load balancer.'}

def confirm(args,message):
 if args.yes:return
 if not sys.stdin.isatty() or input(message+' Type yes: ')!='yes':raise RuntimeError('not confirmed; rerun with --yes after reviewing the plan')

def stack(c,allow_missing=False):
 try:return aws(c,'cloudformation','describe-stacks','--stack-name',c['STACK_PREFIX'],json_result=True)['Stacks'][0]
 except RuntimeError as error:
  if allow_missing and 'does not exist' in str(error):return None
  raise

def assert_owned(description,inventory):
 tags={t['Key']:t['Value'] for t in description.get('Tags',[])}
 if not inventory.get('owner') or tags.get('SampleOwner')!=inventory['owner']:raise RuntimeError('Stack ownership does not match local inventory; refusing to modify resources')

def deploy(args,c):
 inventory=read_json(LOCAL/'inventory.json');print(json.dumps(plan(c,inventory),indent=2))
 if args.plan:return
 if c['SAMPLE_MODE']=='local':build(c);print('Local build ready. Run ./setup run, then ./setup verify.');return
 if not inventory.get('owner'):raise ValueError('Run ./setup configure first to create ownership inventory')
 identity=aws(c,'sts','get-caller-identity',json_result=True);account=identity['Account'];c['AWS_ACCOUNT_ID']=account
 if c['EXISTING_SECRET_ARN']:
  parts=c['EXISTING_SECRET_ARN'].split(':')
  if parts[3]!=c['AWS_REGION'] or parts[4]!=account:raise RuntimeError('Existing dedicated secret must be in the selected account and region')
 print(f'Authenticated account {account}; identity {identity["Arn"]}')
 if inventory.get('account') and (inventory['account']!=account or inventory['region']!=c['AWS_REGION'] or inventory['stack']!=c['STACK_PREFIX']):raise RuntimeError('Inventory belongs to a different account/region/stack')
 existing=stack(c,True)
 if existing:assert_owned(existing,inventory)
 if c['ENABLE_SYNTHETICS']=='true':github_preflight(c,inventory)
 confirm(args,'Apply this owned AWS stack and metered resources?')
 inventory.update(account=account,region=c['AWS_REGION'],stack=c['STACK_PREFIX']);private_json(LOCAL/'inventory.json',inventory)
 build(c,True)
 bucket=inventory.get('artifact_bucket') or f'{c["STACK_PREFIX"]}-build-{account}-{inventory["owner"][:8]}'
 if not inventory.get('artifact_bucket'):
  inventory['artifact_bucket']=bucket;inventory['artifact_created']=False;private_json(LOCAL/'inventory.json',inventory)
 if not inventory.get('artifact_created'):
  # HeadBucket failure is not proof of ownership. Never adopt a globally existing bucket.
  try:
   tags=aws(c,'s3api','get-bucket-tagging','--bucket',bucket,json_result=True)
  except RuntimeError as error:
   if 'NoSuchTagSet' in str(error) and inventory.get('artifact_create_requested'):
    # Resume the exact, previously absent bucket whose create intent was persisted.
    aws(c,'s3api','get-bucket-location','--bucket',bucket,'--expected-bucket-owner',account)
   elif 'NoSuchBucket' in str(error) or 'Not Found' in str(error):
    inventory['artifact_create_requested']=True;private_json(LOCAL/'inventory.json',inventory)
    arguments=['s3api','create-bucket','--bucket',bucket]
    if c['AWS_REGION']!='us-east-1':arguments+=['--create-bucket-configuration',f'LocationConstraint={c["AWS_REGION"]}']
    aws(c,*arguments)
   else:raise
   aws(c,'s3api','put-bucket-tagging','--bucket',bucket,'--tagging',json.dumps({'TagSet':[{'Key':'SampleOwner','Value':inventory['owner']}]}))
  else:
   if {x['Key']:x['Value'] for x in tags['TagSet']}.get('SampleOwner')!=inventory['owner']:raise RuntimeError('Artifact bucket ownership mismatch')
  aws(c,'s3api','put-public-access-block','--bucket',bucket,'--public-access-block-configuration','BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true')
  aws(c,'s3api','put-bucket-encryption','--bucket',bucket,'--server-side-encryption-configuration',json.dumps({'Rules':[{'ApplyServerSideEncryptionByDefault':{'SSEAlgorithm':'AES256'}}]}))
  aws(c,'s3api','put-bucket-lifecycle-configuration','--bucket',bucket,'--lifecycle-configuration',json.dumps({'Rules':[{'ID':'expire-builds','Status':'Enabled','Filter':{'Prefix':''},'Expiration':{'Days':7}}]}))
  inventory['artifact_created']=True;private_json(LOCAL/'inventory.json',inventory)
 key='lambda/'+version()+'.zip';aws(c,'s3api','put-object','--bucket',bucket,'--key',key,'--body',str(ROOT/'build/lambda.zip'))
 hour,minute=c['DEMO_DAILY_START_UTC'].split(':')
 params={'ArtifactBucket':bucket,'ArtifactKey':key,'Owner':inventory['owner'],'Version':version(),'ConfigRevision':inventory.get('config_revision','initial'),'ApiServiceName':c['OTEL_SERVICE_NAME'],'Namespace':c['OTEL_SERVICE_NAMESPACE'],'Environment':c['DEPLOYMENT_ENVIRONMENT'],'IngestEndpoint':c['CORRIDORA_OTLP_ENDPOINT'],'RUMScript':c['CORRIDORA_RUM_SCRIPT_URL'],'RecommenderURL':c['RECOMMENDER_URL'],'ExistingSecretArn':c['EXISTING_SECRET_ARN'],'DailyStart':c['DEMO_DAILY_START_UTC'],'DailyExpression':f'cron({int(minute)} {int(hour)} * * ? *)','WindowSeconds':c['DEMO_WINDOW_SECONDS'],'EnableSynthetics':c['ENABLE_SYNTHETICS'],'EnableForwarding':c['ENABLE_PROVIDER_FORWARDING']}
 aws(c,'cloudformation','deploy','--stack-name',c['STACK_PREFIX'],'--template-file',str(ROOT/'infra/template.yaml'),'--capabilities','CAPABILITY_IAM','--parameter-overrides',*[f'{k}={v}' for k,v in params.items()],'--tags',f'SampleOwner={inventory["owner"]}','--no-fail-on-empty-changeset')
 description=stack(c);assert_owned(description,inventory);outputs={o['OutputKey']:o['OutputValue'] for o in description['Outputs']};inventory['outputs']=outputs;private_json(LOCAL/'inventory.json',inventory)
 # Secrets never appear in argv. AWS CLI reads this owner-only file.
 private_json(LOCAL/'cloud-secret.json',{k:c.get(k,'') for k in SECRET_NAMES})
 try:aws(c,'secretsmanager','put-secret-value','--secret-id',outputs['SecretArn'],'--secret-string','file://'+str(LOCAL/'cloud-secret.json'))
 finally:(LOCAL/'cloud-secret.json').unlink(missing_ok=True)
 # Recycle cold-start configuration after writing/rotating the secret, without fabricating service.version.
 params['ConfigRevision']=str(uuid.uuid4())
 aws(c,'cloudformation','deploy','--stack-name',c['STACK_PREFIX'],'--template-file',str(ROOT/'infra/template.yaml'),'--capabilities','CAPABILITY_IAM','--parameter-overrides',*[f'{k}={v}' for k,v in params.items()],'--tags',f'SampleOwner={inventory["owner"]}','--no-fail-on-empty-changeset')
 inventory['config_revision']=params['ConfigRevision'];private_json(LOCAL/'inventory.json',inventory)
 print('Deployed '+outputs['URL']);print('Run ./setup verify. Configuration revision updated so new Lambda environments load the current secret.')
 if c['ENABLE_SYNTHETICS']=='true':github_install(c,inventory,outputs['URL'])
 elif inventory.get('github_enabled'):github_disable(c,inventory)

def base_url(c):
 if c['SAMPLE_MODE']=='local':return f'http://127.0.0.1:{c["PORT"]}'
 inventory=read_json(LOCAL/'inventory.json');url=inventory.get('outputs',{}).get('URL')
 if not url:raise ValueError('No deployed URL in inventory; deploy first')
 return url

def request(c,path,data=None):
 req=urllib.request.Request(base_url(c)+path,data=json.dumps(data).encode() if data is not None else None,headers={'Authorization':'Bearer '+c['DEMO_CONTROL_TOKEN'],'Content-Type':'application/json'})
 with urllib.request.urlopen(req,timeout=15) as response:return json.load(response)

def run(c):
 if c['SAMPLE_MODE']=='cloud':print(json.dumps(request(c,'/healthz'),indent=2));print(base_url(c));return
 binary=ROOT/'build/reviews'
 if not binary.exists() or read_json(LOCAL/'local-build.json').get('version')!=version():binary=build(c)
 run_command([str(binary)],c)

def verify(args,c):
 print('App: '+base_url(c));health=request(c,'/healthz');print(json.dumps(health,indent=2))
 if c.get('CORRIDORA_INGEST_KEY') and not health.get('telemetry',{}).get('enabled'):raise RuntimeError('Backend export is disabled despite a configured key; check the deployed secret')
 if health.get('telemetry',{}).get('last_export_error'):raise RuntimeError('Backend exporter reports a failure; inspect configuration/connectivity before claiming verification')
 if not (ROOT/'node_modules').exists():run_command(['npm','ci','--ignore-scripts'],c)
 cache=Path(os.environ.get('PLAYWRIGHT_BROWSERS_PATH') or Path.home()/('Library/Caches/ms-playwright' if sys.platform=='darwin' else '.cache/ms-playwright'))
 installed=bool(os.environ.get('PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH')) or (cache.exists() and any(cache.glob('chromium-*')))
 if not installed:
  if args.non_interactive or not sys.stdin.isatty():raise RuntimeError('Chromium missing: run npx playwright install chromium (CI/Linux may need --with-deps)')
  if input('Chromium is required for verification. Install with pinned Playwright now? [yes/no]: ')!='yes':raise RuntimeError('Browser verification requires Chromium')
  run_command(['npx','--no-install','playwright','install','chromium'],c)
 e=environment(c);e['SAMPLE_URL']=base_url(c)
 run_command(['node','scripts/journey.mjs'],c,env=e)
 if c['SAMPLE_MODE']=='cloud':
  inventory=read_json(LOCAL/'inventory.json');assert_owned(stack(c),inventory)
  evidence=read_json(ROOT/'test-results/journey.json');failed=evidence['failed_review'];deadline=time.monotonic()+90;found=False
  while time.monotonic()<deadline:
   messages=aws(c,'sqs','receive-message','--queue-url',inventory['outputs']['DLQURL'],'--max-number-of-messages','10','--visibility-timeout','0','--wait-time-seconds','1',json_result=True)
   if any(json.loads(m['Body']).get('review_id')==failed for m in messages.get('Messages',[])):found=True;break
   time.sleep(3)
  if not found:raise RuntimeError('Browser saw three failed attempts, but the matching SQS DLQ message was not confirmed within 90s; inspect polling/visibility/redrive')
  evidence['aws_dlq_confirmed']=True;private_json(ROOT/'test-results/journey.json',evidence);print('Confirmed matching failed review in the actual owned SQS DLQ; no message deleted.')
 print('Browser workflow passed. See printed trace IDs. Corridora stored CLIENT→SERVER parentage, logs, metrics and DLQ still require the scoped tenant UI/query checks in docs/corridora-setup.md.')

def destroy(args,c):
 inventory=read_json(LOCAL/'inventory.json')
 if c['SAMPLE_MODE']=='local':
  if args.plan:print('Local: delete saved config/secrets/inventory, preserving source. Stop foreground server with Ctrl-C.');return
  confirm(args,'Remove local configuration/secrets?');
  for name in ['config.json','secrets.json','inventory.json']:(LOCAL/name).unlink(missing_ok=True)
  print('Local config removed. Stop any foreground server with Ctrl-C.');return
 print('Owned resource teardown: '+json.dumps({'stack':inventory.get('stack'),'artifact_bucket':inventory.get('artifact_bucket'),'external_secret_preserved':c['EXISTING_SECRET_ARN'] or None}))
 if args.plan:return
 if not inventory.get('owner'):raise RuntimeError('Missing ownership inventory; cannot destroy')
 identity=aws(c,'sts','get-caller-identity',json_result=True)
 if (inventory.get('account'),inventory.get('region'),inventory.get('stack'))!=(identity['Account'],c['AWS_REGION'],c['STACK_PREFIX']):raise RuntimeError('Account/region/stack does not match inventory')
 description=stack(c,True)
 if description:assert_owned(description,inventory)
 if inventory.get('github_repository') and github_variables(c,inventory['github_repository']).get('SAMPLE_REVIEWS_OWNER')!=inventory['owner']:raise RuntimeError('GitHub runner ownership does not match inventory')
 confirm(args,'Delete this owned stack, its data, and its artifact bucket?')
 if description:
  outputs={o['OutputKey']:o['OutputValue'] for o in description.get('Outputs',[])}
  if outputs.get('ThumbnailBucket'):
   bucket=outputs['ThumbnailBucket'];tags=aws(c,'s3api','get-bucket-tagging','--bucket',bucket,json_result=True)
   if {x['Key']:x['Value'] for x in tags['TagSet']}.get('SampleOwner')!=inventory['owner']:raise RuntimeError('Thumbnail bucket ownership mismatch')
   aws(c,'s3','rm','s3://'+bucket,'--recursive')
  aws(c,'cloudformation','delete-stack','--stack-name',c['STACK_PREFIX']);aws(c,'cloudformation','wait','stack-delete-complete','--stack-name',c['STACK_PREFIX'])
 if inventory.get('artifact_bucket'):
  bucket=inventory['artifact_bucket'];tags=aws(c,'s3api','get-bucket-tagging','--bucket',bucket,json_result=True)
  if {x['Key']:x['Value'] for x in tags['TagSet']}.get('SampleOwner')!=inventory['owner']:raise RuntimeError('Artifact bucket ownership mismatch')
  aws(c,'s3','rm','s3://'+bucket,'--recursive');aws(c,'s3api','delete-bucket','--bucket',bucket)
 if inventory.get('github_repository'):github_disable(c,inventory,destroy=True)
 private_json(LOCAL/'inventory.json',{'owner':inventory['owner']});print('Owned resources deleted; external resources preserved.')

def replay_dlq(c):
 if c['SAMPLE_MODE']!='cloud':raise ValueError('DLQ replay is cloud-only; local queue simulates retries without SQS')
 inventory=read_json(LOCAL/'inventory.json');description=stack(c);assert_owned(description,inventory)
 if request(c,'/api/demo/status')['phase']=='regression':raise ValueError('Recover the scenario before replaying DLQ')
 outputs=inventory['outputs'];batch=aws(c,'sqs','receive-message','--queue-url',outputs['DLQURL'],'--max-number-of-messages','1','--message-attribute-names','All',json_result=True)
 for message in batch.get('Messages',[]):
  private_json(LOCAL/'replay-message.json',{'QueueUrl':outputs['QueueURL'],'MessageBody':message['Body'],'MessageAttributes':message.get('MessageAttributes',{})})
  try:aws(c,'sqs','send-message','--cli-input-json','file://'+str(LOCAL/'replay-message.json'))
  finally:(LOCAL/'replay-message.json').unlink(missing_ok=True)
  aws(c,'sqs','delete-message','--queue-url',outputs['DLQURL'],'--receipt-handle',message['ReceiptHandle'])
 print('Replayed at most one DLQ message; original context preserved; duplicate processing is idempotent.')

def main():
 parser=argparse.ArgumentParser(description='Configure, deploy and run the Corridora AWS reviews sample. Python 3; no cloud calls for --help or --plan.')
 parser.add_argument('command',nargs='?',choices=['configure','deploy','run','verify','scenario','destroy'])
 parser.add_argument('--config',help='JSON configuration file; secret-containing files must be chmod 600')
 parser.add_argument('--non-interactive',action='store_true',help='Use environment/protected config/defaults, no prompts')
 parser.add_argument('--plan',action='store_true',help='Offline deployment/teardown plan, no credentials or network')
 parser.add_argument('--yes',action='store_true',help='Apply reviewed deploy/destroy plan without confirmation prompt')
 parser.add_argument('--phase',choices=['healthy','regression','recover'],default='regression')
 parser.add_argument('--duration',type=int,default=180)
 parser.add_argument('--replay-dlq',action='store_true')
 args=parser.parse_args()
 if args.command=='configure':configure(args);return
 if args.command is None:
  c=configure(args)
  deploy(args,c)
  if c['SAMPLE_MODE']=='local':
   process=subprocess.Popen([str(ROOT/'build/reviews')],cwd=ROOT,env=environment(c))
   try:
    for _ in range(60):
     try:request(c,'/healthz');break
     except (OSError,urllib.error.URLError):time.sleep(.25)
    verify(args,c);print('Sample is running. Ctrl-C to stop.');process.wait()
   finally:process.terminate();process.wait(timeout=10)
  else:verify(args,c)
  return
 c=load(args.config)
 # A useful plan never requires a secret or a login.
 if args.plan and args.command in {'deploy','destroy'}:
  if args.command=='deploy':print(json.dumps(plan(c,read_json(LOCAL/'inventory.json')),indent=2))
  else:destroy(args,c)
  return
 validate(c)
 if args.command=='deploy':deploy(args,c)
 elif args.command=='run':run(c)
 elif args.command=='verify':verify(args,c)
 elif args.command=='scenario':
  if args.replay_dlq:replay_dlq(c)
  else:print(json.dumps(request(c,'/api/demo/scenario',{'phase':args.phase,'duration_seconds':args.duration}),indent=2))
 elif args.command=='destroy':destroy(args,c)
