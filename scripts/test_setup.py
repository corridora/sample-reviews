import json
import base64
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch
import setup_app as setup

class SetupTests(unittest.TestCase):
 def config(self):return {**setup.DEFAULTS,'DEMO_CONTROL_TOKEN':'test-control'}
 def test_help_offline(self):
  result=subprocess.run([str(setup.ROOT/'setup'),'--help'],capture_output=True,text=True)
  self.assertEqual(result.returncode,0);self.assertIn('deploy',result.stdout)
 def test_plan_needs_no_credentials(self):
  with patch.dict(os.environ,{},clear=True):
   result=subprocess.run([str(setup.ROOT/'setup'),'deploy','--plan'],capture_output=True,text=True)
  self.assertEqual(result.returncode,0);self.assertIn('$1–3',json.loads(result.stdout)['cost']);self.assertNotIn('test-control',result.stdout)
 def test_secret_permissions(self):
  with tempfile.TemporaryDirectory() as directory:
   path=Path(directory)/'secrets.json';setup.private_json(path,{'secret':'mock'})
   self.assertEqual(path.stat().st_mode&0o777,0o600);self.assertEqual(setup.read_json(path,True)['secret'],'mock')
   path.chmod(0o644)
   with self.assertRaises(ValueError):setup.read_json(path,True)
 def test_default_tenant_is_own(self):self.assertEqual(setup.DEFAULTS['CORRIDORA_TARGET'],'own')
 def test_cloud_requires_keys(self):
  c=self.config();c['SAMPLE_MODE']='cloud'
  with self.assertRaises(ValueError):setup.validate(c)
 def test_demo_requires_operator_keys(self):
  c=self.config();c['CORRIDORA_TARGET']='demo'
  with self.assertRaises(ValueError):setup.validate(c)
  c.update(CORRIDORA_INGEST_KEY='mock',CORRIDORA_RUM_KEY='mock');setup.validate(c);self.assertEqual(c['DEPLOYMENT_ENVIRONMENT'],'demo')
 def test_url_safety(self):
  for value in ['http://example.com','https://user:password@example.com','https://example.com?key=mock','file:///tmp/app']:
   c=self.config();c['RECOMMENDER_URL']=value
   with self.assertRaises(ValueError):setup.validate(c)
  c=self.config();c['RECOMMENDER_URL']='http://127.0.0.1:9999';setup.validate(c)
 def test_expiry_bounds(self):
  for value in ['0','601','1.5','NaN']:
   c=self.config();c['DEMO_WINDOW_SECONDS']=value
   with self.assertRaises(ValueError):setup.validate(c)
 def test_ownership_rejects_foreign_stack(self):
  with self.assertRaises(RuntimeError):setup.assert_owned({}, {})
  with self.assertRaises(RuntimeError):setup.assert_owned({'Tags':[{'Key':'SampleOwner','Value':'other'}]},{'owner':'ours'})
  setup.assert_owned({'Tags':[{'Key':'SampleOwner','Value':'ours'}]},{'owner':'ours'})
 def test_version_is_source_digest(self):self.assertRegex(setup.version(),r'^sha256-[a-f0-9]{16}$')
 def test_resource_name_and_arn_bounds(self):
  c=self.config();c['STACK_PREFIX']='x'*36
  with self.assertRaises(ValueError):setup.validate(c)
  c=self.config();c['EXISTING_SECRET_ARN']='not-an-arn'
  with self.assertRaises(ValueError):setup.validate(c)
 def test_plan_does_not_disclose_secrets(self):
  c=self.config();c['CORRIDORA_INGEST_KEY']='private-backend'
  self.assertNotIn('private-backend',json.dumps(setup.plan(c)))
 def test_enabled_synthetics_require_selected_repository(self):
  c=self.config();c.update(SAMPLE_MODE='cloud',CORRIDORA_INGEST_KEY='mock',CORRIDORA_RUM_KEY='mock',ENABLE_SYNTHETICS='true')
  with self.assertRaises(ValueError):setup.validate(c)
  c['SYNTHETIC_GITHUB_REPOSITORY']='contributor/sample-reviews';setup.validate(c)
 def test_github_preflight_checks_workflow_and_owner(self):
  c=self.config();c['SYNTHETIC_GITHUB_REPOSITORY']='contributor/sample-reviews'
  workflow=base64.b64encode((setup.ROOT/'.github/workflows/daily.yml').read_bytes()).decode()
  def command(argv,*args,**kwargs):
   if argv[1]=='api':return json.dumps({'content':workflow})
   if argv[1:3]==['variable','list']:return '[]'
   return ''
  with patch.object(setup,'run_command',side_effect=command):self.assertEqual(setup.github_preflight(c,{'owner':'ours'}),'contributor/sample-reviews')
  with patch.object(setup,'run_command',side_effect=command),patch.object(setup,'github_variables',return_value={'SAMPLE_REVIEWS_OWNER':'other'}):
   with self.assertRaises(RuntimeError):setup.github_preflight(c,{'owner':'ours'})
 def test_github_secret_uses_stdin(self):
  c=self.config();inventory={'owner':'ours'}
  with tempfile.TemporaryDirectory() as directory,patch.object(setup,'LOCAL',Path(directory)),patch.object(setup,'github_preflight',return_value='contributor/sample-reviews'),patch('builtins.print'),patch.object(setup,'run_command',return_value='') as command:
   setup.github_install(c,inventory,'https://api.example.com')
   secret=[call for call in command.call_args_list if call.args[0][1:3]==['secret','set']][0]
   self.assertEqual(secret.kwargs['input_text'],'test-control');self.assertNotIn('test-control',secret.args[0]);self.assertTrue(inventory['github_enabled'])
if __name__=='__main__':unittest.main()
