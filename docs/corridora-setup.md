# Set up Corridora for Trace Reviews

Published 2026-10-11 · Updated 2026-10-11

## Choose your organization and keys

Contributors use their own Corridora organization (`CORRIDORA_TARGET=own`). Create a
project for the namespace `corridora-samples.reviews` and configure these services:

| Service | Source | Signals |
| --- | --- | --- |
| `sample-reviews-web` | Corridora browser RUM + React action spans | Browser traces/logs, session/performance observations |
| `sample-reviews-api` | Go API Lambda/local server | Traces, application logs and metrics |
| `sample-reviews-worker` | Go SQS moderation Lambda | Consumer/dependency traces, logs and operation metrics |
| `sample-reviews-cloudwatch` | Optional Go bridge Lambda | Converted Lambda platform logs and native AWS metric observations |

Issue a scoped **backend ingest key** for API/worker/bridge and a separate **RUM key**
for the browser. The browser key is intentionally visible; the backend key is secret.
Management/query credentials are separate and are never part of frontend configuration.
This sample does not implement management access or create keys from a demo persona.

Maintained Demo deployments use `CORRIDORA_TARGET=demo` and environment `demo`.
An authorized operator supplies the two Demo-scoped keys. Demo's canonical UUID is
`00000000-0000-4000-8000-000000000201`; routing comes from the keys, not a tenant ID
URL parameter. No shared credentials are included in the repository.

## Configure and deploy

```sh
./setup configure
./setup deploy --plan
./setup deploy
./setup run
```

Bare `./setup` walks through the same flow and verification. Choose local/cloud;
Corridora target (own by default); ingest **base URL** (default
`https://ingest.corridora.com`); RUM script (`https://rum.corridora.com/corridora-rum.js`);
the two keys; API service/namespace/environment; local port (4102); a control token
(blank generates one); daily UTC start (13:20), bounded window (600 seconds); optional
synthetics and provider forwarding; and optional controlled recommender origin/token.
Cloud mode additionally asks for the existing AWS CLI profile, `us-east-1` region,
dedicated stack prefix, and optionally a dedicated existing secret ARN. Queue visibility
is fixed at 30 seconds, worker timeout 5 seconds, and maximum receives 3 to keep the
failure bounded. Setup resolves the authenticated account and shows its resource/cost
plan before applying anything. It never asks for AWS account passwords.

Saved answers live in `.local/config.json`, secrets in `.local/secrets.json`, and
resource ownership/outputs in `.local/inventory.json`, all Git-ignored and owner-only.
CloudFormation provisions a dedicated Secrets Manager secret unless a dedicated ARN
was explicitly selected. AWS CLI reads the temporary secret JSON from a protected file,
which is removed afterward; the secret is never a command-line argument.

Cloud synthetics additionally require authenticated GitHub CLI and an explicitly
selected `SYNTHETIC_GITHUB_REPOSITORY=owner/repo`. Publish this checkout's current
`.github/workflows/daily.yml` to that repository's `main` branch first. Setup verifies
its exact content and existing ownership before AWS changes, then writes owned
repository variables, passes the control secret via stdin, and enables the real browser
schedule/watchdog. It refuses unrelated existing runner configuration. No source
is pushed or repository created by setup. The chosen repository/account and feature
are included in the reviewable plan.

For CI, provide the same names as environment variables and use
`./setup configure --non-interactive`, then `./setup deploy --yes` after reviewing the
offline plan. `--config /path/config.json` also works; secret-containing files need
chmod 600. A missing cloud/Demo key fails clearly. Local empty keys disable export;
that permits learning the UI but does not validate instrumentation.

## Instrumentation files and protocol

`internal/app/telemetry.go` constructs actual Go OpenTelemetry SDK providers and the
pinned official OTLP HTTP exporters. Backend requests use **binary protobuf** and
`X-CORRIDORA-KEY` at `/v1/traces`, `/v1/logs`, and `/v1/metrics`. Resource attributes
include service name/namespace, real source-build version, environment, cloud provider,
region/account and Lambda identity. A parent-based sampler respects incoming W3C
sampling; root demo spans sample at 100% under the small usage budget.

`internal/app/app.go` extracts incoming `traceparent` **before** its SERVER span.
`internal/app/lambda.go` reconstructs HTTP API v2 events and uses the same handler,
so AWS/X-Ray request IDs are never substituted for browser W3C IDs. Explicit client
spans cover DynamoDB, S3, SQS, metric reads and the optional recommender. Exporter HTTP
is uninstrumented, avoiding recursion. Go logs are emitted through the OTel log SDK
with context so they carry span/trace IDs. Operation counters/histograms and native
window gauges use the metric SDK.

The Lambda dispatcher flushes all three providers within a **2.5-second total budget**
before returning. Each exporter times out after one second, without unbounded retries.
Failures are visible in stderr and `/healthz.telemetry.last_export_error`; public
health responses redact low-level exporter errors. Failure to export does not recurse
through another telemetry HTTP client. There is no persistent local OTLP spool, so
serverless freeze/crash or network failure can lose application telemetry.

`web/index.html` has an opening-head RUM slot. The Go HTML handler inserts the configured
blocking RUM script with URL-encoded `ckid=<RUM key>` before React executes. Attributes
set ingest base URL, `sample-reviews-web`, namespace, environment, actual version,
100% trace sampling, long tasks and the exact API origin. The backend key never enters
Vite's frontend variables, generated HTML or API responses. `web/src/main.jsx` imports
only `@opentelemetry/api` for manual React spans; the distribution owns the browser
provider. Each asynchronous fetch is invoked inside an explicitly bound action scope,
without creating a second competing browser SDK.

## Origins, cross-cloud calls and queues

One Go origin serves React assets and `/api/*`, locally and on AWS's HTTPS hostname.
The CSP allows only the configured RUM script host and ingest connection host in
addition to same-origin assets. `PUBLIC_ORIGIN` is derived from the HTTP API. CORS
permits an exact configured origin and `content-type,traceparent,authorization`, with
no wildcard credentialed policy. API/control/status responses are not cached.

`RECOMMENDER_URL` is a controlled HTTPS origin; local loopback HTTP is accepted only
in local mode. The client appends `/api/recommendations?product_id=demo-product`, uses
a 1.5-second timeout and forbids redirects. Only this explicit dependency receives
trace context and the separate optional `DOWNSTREAM_AUTH_TOKEN`. A timeout or non-200
response records an error and returns a marked local fixture fallback. No Corridora
or scenario credentials go to dependencies. The leaf review-summary and fictional
price endpoints never call the recommender, preventing a cycle.

The SQS producer injects its own W3C context into both message attributes and the
message's protected JSON envelope. The worker extracts the attributes before a
CONSUMER span and records receive count/scenario/release. Messages are processed one
at a time with partial batch failure reporting; successful controls aren't retried
with the failing fixture. The local queue uses the same consumer and context logic,
with one-second retries instead of AWS visibility waits. There is no fan-in in this
app, so no artificial span links are invented.

## Add AWS platform data

Enable `ENABLE_PROVIDER_FORWARDING=true`, rerun setup deploy, and inspect
`infra/template.yaml` and `internal/app/lambda.go`:

- CloudWatch subscriptions on the API/worker's 3-day log groups invoke the dedicated
  bridge Lambda with gzip/base64 records. Only native text `START`, `END` and `REPORT`
  records export; application logs already use OTLP directly. The bridge's own log
  group has no subscription, avoiding loops.
- Converted logs preserve source timestamp, account, group, stream and event ID.
  DynamoDB stores checkpoints **after successful SDK flush**; retries can duplicate
  records after a crash, so use source event IDs when investigating. There is no
  exactly-once guarantee. The bridge is limited to one concurrent invocation.
- EventBridge Scheduler polls four `GetMetricData` queries every 15 minutes: API
  Lambda Invocations, Errors, Duration and DLQ visible depth. The IAM action is
  read-only and requires `Resource: '*'` for this AWS API; all writable store/secret
  permissions are scoped to owned resources.
- `aws.native.window.value` is a gauge of the **latest observed 900-second source
  aggregation**, not a raw historical sample. Attributes identify original statistic
  (Sum/Maximum), unit (Count/Milliseconds), metric ID/label and period. A correlated
  OTLP log preserves the original window timestamp and value. There is no claim of
  full CloudWatch parity or automatic AWS telemetry availability.

The API/worker roles read the dedicated secret and only their needed store/queue/S3
operations. The bridge reads native metrics, its secret and checkpoints. Runtime
SDK operations use IAM, not static AWS keys. Review actual CloudWatch API usage,
export egress and retention against the [README's budget](../README.md#aws-setup-and-cost).

## Verify in Corridora

```sh
./setup verify
```

This requires a running local server or deployed URL and installed Chromium. It runs
healthy → seeded rejection → unaffected control → recovery with the real page and
prints trace IDs plus RUM initialization status. Look up the browser session in the
chosen tenant. Confirm a stored browser **CLIENT** span and backend **SERVER** span
share a trace ID and that the server parent ID equals the client span ID. Continue
to `review.publish` → `SQS.SendMessage` → `moderation.process` and the database spans.
Find `review.submitted`, `moderation.failure`, deployment events and operation metrics.
Finally, confirm a native Lambda platform log and native metric-window observation
under `sample-reviews-cloudwatch` when forwarding is enabled.

Cloud `./setup verify` also polls the owned SQS DLQ for up to 90 seconds after the
browser journey and requires a message whose `review_id` matches the failed review.
It uses zero visibility timeout and does not delete the message. This requires your
deployment identity's SQS receive permission; the GitHub browser runner itself needs
no AWS credentials and reports browser evidence only. A third moderation failure
alone never claims DLQ delivery.

HTTP 200 from ingest, a trace-check response, a loaded script, or a synthetically
injected trace header is not proof of stored browser/backend correlation. This sample
does not infer query API authorization from the ingest key and ships no guessed
Corridora query API adapter. Tenant UI checks remain explicit unless an operator adds
a documented scoped query integration.

For independent local proof, `RUM_BUNDLE_PATH=/path/corridora-rum.js python3
scripts/local-validation.py` loads an actual distribution and captures all records
on loopback; it asserts real browser CLIENT/server parentage, logs and metrics.
Read `test-results/journey.json` for sample trace/session IDs and verified parent pairs.
The sink never forwards its mock-key records to Corridora. Hosted AWS delivery,
daily timing and stored tenant evidence are separate operator checks.

## Replay, rotate and troubleshoot

```sh
./setup scenario --phase regression --duration 180
./setup scenario --phase recover --duration 180
./setup scenario --replay-dlq
```

The API requires `Authorization: Bearer DEMO_CONTROL_TOKEN`; empty, raw or wrong-scheme
tokens fail. Numeric duration must be an integer from 1 to 600. Only the seeded bad
product fails. Auto-expiry is checked in each request and queue delivery. Deployment
logs carry `demo.simulated=true`, `demo.scenario=moderation-schema`, a unique release
and the actual service version. The UI's failed status precedes possible SQS redrive
completion; confirm the DLQ itself before claiming dead-letter arrival. Recover
before replaying at most one DLQ message; processing an approved review is idempotent.

Rotate keys by rerunning `./setup configure` and `./setup deploy`. The deploy script
updates the secret from a protected file and changes only a separate configuration
revision to recycle cached Lambda credentials; `service.version` still denotes the
built code. RUM key rotation changes the tag the next time HTML loads; use a fresh
browser session. Revoke superseded keys after verification.

If RUM is missing, inspect the early tag, ckid, CSP and ingest CORS preflight; check
`window.CorridoraRum.getInitializationStatus()` in the browser. If traces are disconnected,
inspect request `traceparent`, exported parent IDs and provider ownership. If the
worker is delayed, check SQS visibility, Lambda throttling and the actual DLQ; do not
raise retries indefinitely. If OTLP is missing, check health's exporter indicator,
secret IAM access, regional internet connectivity and the separate backend/RUM keys.
If a stack partially fails, retain the ownership inventory, inspect CloudFormation
events and rerun; never adopt an unrelated stack by changing its owner identifier.

```sh
./setup destroy --plan
./setup destroy
```

Teardown validates stack/account/region/resource tags, empties only owned buckets,
deletes the owned stack/build bucket, and preserves a selected external secret.
Schedules and log subscriptions disappear with the stack. No provider data, live
cloud billing or tenant access was exercised while generating this repository.
The installed GitHub workflow is disabled and this setup's written runner variables
and control secret are removed after ownership verification; the repository remains.
