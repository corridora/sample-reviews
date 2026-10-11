#!/usr/bin/env python3
"""Runs real RUM + real Go SDK against a local OTLP sink; never contacts Corridora."""
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
import urllib.request
ROOT=Path(__file__).resolve().parents[1]
def port():
 with socket.socket() as sock:sock.bind(('127.0.0.1',0));return str(sock.getsockname()[1])
def wait(url):
 for _ in range(100):
  try:
   with urllib.request.urlopen(url,timeout=.5):return
  except OSError:time.sleep(.1)
 raise RuntimeError('Server did not start: '+url)
bundle=os.environ.get('RUM_BUNDLE_PATH')
if not bundle or not Path(bundle).is_file():sys.exit('Set RUM_BUNDLE_PATH to your real Corridora RUM distribution. This test never substitutes a fake browser SDK.')
local=ROOT/'.local';local.mkdir(exist_ok=True);local.chmod(0o700);capture=local/'capture.ndjson';capture.write_text('');capture.chmod(0o600)
api_port,capture_port=port(),port()
env={**os.environ,'GOCACHE':str(local/'go-cache'),'SAMPLE_MODE':'local','PORT':api_port,'DEMO_CONTROL_TOKEN':'local-validation-control-only','CORRIDORA_TARGET':'own','CORRIDORA_INGEST_KEY':'local-test-backend','CORRIDORA_RUM_KEY':'local-test-rum','CORRIDORA_OTLP_ENDPOINT':'http://127.0.0.1:'+capture_port,'CORRIDORA_RUM_SCRIPT_URL':'http://127.0.0.1:'+capture_port+'/corridora-rum.js','CAPTURE_PORT':capture_port,'CAPTURE_FILE':str(capture),'PUBLIC_ORIGIN':'http://127.0.0.1:'+api_port,'SAMPLE_URL':'http://127.0.0.1:'+api_port,'RECOMMENDER_URL':'','DOWNSTREAM_AUTH_TOKEN':'','SERVICE_VERSION':'local-validation'}
subprocess.run(['npm','run','build'],cwd=ROOT,env=env,check=True)
processes=[]
try:
 for name in ['capture','reviews']:
  subprocess.run(['go','build','-o',str(local/name),'./cmd/'+name],cwd=ROOT,env=env,check=True)
  log=(local/(name+'.log')).open('w');processes.append((subprocess.Popen([str(local/name)],cwd=ROOT,env=env,stdout=log,stderr=subprocess.STDOUT),log))
  wait('http://127.0.0.1:'+ (capture_port if name=='capture' else api_port)+'/healthz')
 subprocess.run(['node','scripts/journey.mjs'],cwd=ROOT,env=env,check=True)
 print('Verified real RUM CLIENT/Go SERVER parentage, workflow failure/control/recovery, and all three local OTLP signals.')
finally:
 for process,log in reversed(processes):process.terminate();process.wait(timeout=10);log.close()
