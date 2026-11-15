<p align="center">
  <img src="https://query.farm/media-kit/products/vgi/vgi-logo.png" alt="Vector Gateway Interface (VGI)" width="320">
</p>

# Query Google Cloud Pub/Sub Topics, Subscriptions and Backlog With SQL in DuckDB

> **vgi-pubsub** · a [Query.Farm](https://query.farm) VGI worker

[![CI](https://github.com/Query-farm/vgi-pubsub/actions/workflows/ci.yml/badge.svg)](https://github.com/Query-farm/vgi-pubsub/actions/workflows/ci.yml)
[![vgi worker: query.farm](https://query.farm/media-kit/shields/vgi-worker.svg)](https://query.farm)

The Pub/Sub console shows one resource at a time. This worker turns a Google Cloud project's
Pub/Sub setup into SQL tables in DuckDB, so you can ask questions across all of it: which
topics have no subscriptions, which subscriptions have no dead-letter policy, who is granted
`allUsers`, which consumers are falling behind. Join the answers with anything else DuckDB reads.

- **Resources**: topics (labels, retention, schema settings, Cloud KMS key, storage policy,
  import-topic ingestion settings), subscriptions (pull / push / BigQuery / Cloud Storage, ack
  deadline, retention, filter, dead-letter and retry policy, exactly-once, ordering; the push
  endpoint is reduced to its **host**, never its path, query or auth audience), snapshots,
  schemas and every schema revision with its definition.
- **IAM**: resource-level policies of topics, subscriptions, snapshots and schemas, flattened to
  one row per role and member, with conditions. Policies that cannot be read show up as rows
  with `error` set, not as silently missing resources.
- **Backlog and throughput**: Cloud Monitoring time series (`num_undelivered_messages`,
  `oldest_unacked_message_age`, `send_request_count`, ...) as long rows in the same shape as
  [vgi-prometheus](https://github.com/Query-farm/vgi-prometheus) (`labels MAP`, `ts`, `value`),
  and the current consumer lag per subscription.
- **Messages, opt-in only**: replay a topic's *retained* messages through a temporary
  subscription that the worker creates, drains and deletes. **Existing subscriptions are never
  pulled from.** See [Reading messages](#reading-messages-and-why-it-is-opt-in).

Everything else is read-only: `GET` calls plus `getIamPolicy` / `testIamPermissions`.

## Quick start

```sql
INSTALL vgi FROM community;     -- Haybarn 2.0; on DuckDB 2.0 use the query_farm repository
LOAD vgi;
ATTACH 'pubsub' (TYPE vgi, LOCATION 'github-auto://Query-farm/vgi-pubsub@v0.1.0');

CREATE SECRET (TYPE gcp_pubsub, PROJECT_ID 'my-project',
               SERVICE_ACCOUNT_JSON '{"type": "service_account", ...}');

-- Subscriptions per topic: a topic with none drops every message.
SELECT t.topic, count(s.name) AS subscriptions
FROM pubsub.main.topics() t
LEFT JOIN pubsub.main.subscriptions() s ON s.topic = t.name
GROUP BY t.topic ORDER BY subscriptions, t.topic;

-- Who is behind right now?
SELECT subscription, undelivered_messages, oldest_unacked_age_seconds / 60 AS lag_minutes
FROM pubsub.main.subscription_backlog()
ORDER BY oldest_unacked_age_seconds DESC NULLS LAST;
```

The secret type is registered by the worker, so run `CREATE SECRET` after `ATTACH`.

<details><summary>Other ways to run it</summary>

- **DuckDB 2.0**:
  `SET allow_extension_repositories = 'allowed'; CREATE EXTENSION REPOSITORY IF NOT EXISTS query_farm WITH PREFIX 'https://duckdb-extensions.query.farm'; INSTALL vgi FROM query_farm; LOAD vgi FROM query_farm;`
  then attach as above.
- **Docker** (HTTP): `docker run -p 8000:8000 ghcr.io/query-farm/vgi-pubsub:0.1.0`, then
  `ATTACH 'pubsub' (TYPE vgi, LOCATION 'http://localhost:8000')`.
- **Go**: `go install github.com/Query-farm/vgi-pubsub/cmd/vgi-pubsub-worker@v0.1.0`,
  then attach the binary by absolute path.
- **From source**: `make build` (Go 1.26+, pure Go, no cgo), then
  `ATTACH 'pubsub' (TYPE vgi, LOCATION '/abs/path/to/bin/vgi-pubsub-worker')`.

v0.1.0 is not published yet. Until it is, build from source.
</details>

## Secrets and configuration

Every API function reads a DuckDB secret of type `gcp_pubsub`:

| Field | Meaning |
|---|---|
| `PROJECT_ID` | Default project. Every function also takes `project := '...'`. With a service-account key it defaults to the key's `project_id`. |
| `SERVICE_ACCOUNT_JSON` | The key file contents. The worker signs a JWT and exchanges it at the key's `token_uri` for an access token (scopes: `pubsub`, `monitoring.read`), cached in memory for its lifetime. Redacted. |
| `ACCESS_TOKEN` | An OAuth 2.0 access token, e.g. `gcloud auth print-access-token` (expires after about an hour). Redacted. |
| `ENDPOINT` | Pub/Sub API root, default `https://pubsub.googleapis.com`. For the **emulator** give its `host:port` (the `PUBSUB_EMULATOR_HOST` value) or `http://host:port`; then no credentials are needed. |
| `MONITORING_ENDPOINT` | Cloud Monitoring API root, default `https://monitoring.googleapis.com`. |
| `QUOTA_PROJECT_ID` | Sent as `x-goog-user-project` (some user credentials need a quota project). |

```sql
-- A service account (roles/pubsub.viewer + roles/monitoring.viewer is enough for everything but read_messages)
CREATE SECRET prod (TYPE gcp_pubsub, SERVICE_ACCOUNT_JSON '...');
-- A short-lived user token
CREATE SECRET me (TYPE gcp_pubsub, PROJECT_ID 'my-project', ACCESS_TOKEN 'ya29....');
-- The emulator
CREATE SECRET emu (TYPE gcp_pubsub, PROJECT_ID 'my-project', ENDPOINT 'localhost:8085');

SELECT * FROM pubsub.main.access_check(secret_name := 'prod');
```

**Application Default Credentials are never used implicitly.** The worker does not read
`GOOGLE_APPLICATION_CREDENTIALS`, gcloud's credentials or the metadata server unless you opt in
on the ATTACH, and then only when the secret has no `SERVICE_ACCOUNT_JSON` / `ACCESS_TOKEN`:

```sql
ATTACH 'pubsub' (TYPE vgi, LOCATION '...', use_application_default_credentials true);
CREATE SECRET (TYPE gcp_pubsub, PROJECT_ID 'my-project');
```

With several secrets, pick one per call with `secret_name := '...'`. Credentials never enter
table-function state or logs. 429 and 5xx responses are retried with exponential backoff,
honouring `Retry-After`; other errors carry the HTTP status and Google's canonical status
(`PERMISSION_DENIED`, `NOT_FOUND`, ...). Run `access_check()` to see which capabilities the
credential has.

## What's inside

All objects are in `pubsub.main`. Every table function takes `project :=` and `secret_name :=`.
Resource arguments accept an id (`'orders'`) or a full name (`'projects/p/topics/orders'`).

| Function | What it returns |
|---|---|
| `topics(topic := …, page_size := 100)` | One row per topic: `labels`, `message_retention_seconds`, schema settings, `kms_key_name`, `allowed_persistence_regions`, `ingestion_source` / `ingestion_settings` (import topics), `message_transforms`, `state`. |
| `subscriptions(topic := …, subscription := …)` | `type` (`pull`/`push`/`bigquery`/`cloud_storage`), `ack_deadline_seconds`, retention, `filter`, `labels`, `expiration_ttl_seconds`, `dead_letter_topic`, `max_delivery_attempts`, retry backoff, exactly-once, ordering, `detached`, `push_endpoint_host`, `push_oidc_service_account`, `bigquery_table`, `cloud_storage_bucket`, `export_state`. |
| `snapshots(topic := …)` | Snapshot, topic, `expire_time`, labels. |
| `schemas()` | Latest revision of every schema with its `definition` (Avro JSON or `.proto`). |
| `schema_revisions(schema)` | Every revision of one schema. |
| `iam_policies(resource_type := 'all', resource := …)` | One row per (resource, role, member), with `member_type`, IAM condition, `policy_version`, `etag`, `error`. |
| `time_series(metric, start_time, end_time, subscription := …, topic := …, filter := …, alignment_seconds := 60, aligner := 'auto', reducer := …, group_by := …)` | Cloud Monitoring points as long rows: `metric`, `resource_type`, `topic`, `subscription`, `labels`, `ts`, `start_ts`, `value`, `metric_kind`, `value_type`, `unit`. |
| `subscription_backlog(lookback_minutes := 10, subscription := …, end_time := …)` | Latest `undelivered_messages` and `oldest_unacked_age_seconds` per subscription. |
| `read_messages(topic, allow_temporary_subscription := true, start_time := …, end_time := …, snapshot := …, filter := …, max_messages := 100)` | Retained messages (see below). |
| `access_check()` | One row per capability: may the credential list topics / subscriptions / snapshots / schemas, read IAM, attach subscriptions, read metrics. |
| `resource_id(name)`, `resource_project(name)`, `duration_seconds(text)` | Offline scalars: the id or project of a resource name, and seconds in a `'604800s'` duration. |

Durations are `DOUBLE` seconds, times `TIMESTAMPTZ`, labels and attributes `MAP(VARCHAR, VARCHAR)`
(read one with `labels['team']`; an absent key is `NULL`). Name columns hold full resource names,
so join `subscriptions.topic = topics.name`; Monitoring rows carry ids, so join
`time_series.subscription = subscriptions.subscription`.

### What pushes down

| SQL | Becomes |
|---|---|
| `topic :=`, `subscription :=` on `topics` / `subscriptions` | one `GET` of that resource instead of a listing |
| `topic :=` on `subscriptions` / `snapshots` | the topic's own `…/subscriptions` / `…/snapshots` listing |
| `resource_type :=`, `resource :=` on `iam_policies` | which listings are walked / a single `getIamPolicy` |
| `metric`, `subscription :=`, `topic :=`, `filter :=` on `time_series` | the Monitoring `filter` (`metric.type`, `resource.labels.subscription_id`, ...) |
| `start_time` / `end_time` | the Monitoring `interval` |
| `alignment_seconds`, `aligner`, `reducer`, `group_by` | server-side `aggregation.*` |
| `filter :=` on `read_messages` | the temporary subscription's filter (attributes only) |
| `start_time :=` / `snapshot :=` on `read_messages` | the seek target |

SQL `WHERE` clauses are not translated: the list APIs have no server-side filters, so use the
named arguments above for selective calls and `WHERE` for everything else. Listings are paged
lazily with the API's `pageToken`, kept in the scan state so long scans resume across the HTTP
transport.

## Reading messages, and why it is opt-in

Pub/Sub has **no non-destructive peek**. Pulling from an existing subscription leases its
messages: the real subscribers cannot get them until the ack deadline passes, every pull counts
as a delivery attempt toward the dead-letter policy, and acknowledging them would lose them for
good. `read_messages` therefore **never pulls from, seeks or changes an existing subscription**.

With `allow_temporary_subscription := true` (required; without it the call fails before doing
anything) it:

1. creates a new subscription on the topic named `vgi-temp-<UTC time>-<random>`, labelled
   `vgi-temp=true` and `created-by=vgi-pubsub`, with a 24-hour `expiration_policy` (the minimum
   Pub/Sub allows) and 10-minute message retention, so a leak cleans itself up;
2. seeks it to `start_time` (default: now minus the topic's retention, i.e. the oldest retained
   message) or to `snapshot :=`;
3. pulls up to `max_messages` (at most `wait_seconds`), acknowledging them **on the temporary
   subscription only** (each subscription has its own copy of every message);
4. deletes the temporary subscription, also when any step fails. If the delete itself fails the
   query fails and names the subscription to remove.

All four steps happen inside one call, so the temporary subscription never outlives the query,
even over the HTTP transport. A create that collides (`ALREADY_EXISTS`) stops before any pull.

What remains: a subscription is created and deleted (audit log entries; needs
`pubsub.subscriptions.create` / `delete` and `pubsub.topics.attachSubscription`), the replayed
messages are billed as delivery, and **only retained messages can be read**: the topic must have
message retention (`message_retention_seconds` in `topics()`), or you read a snapshot. A new
subscription cannot see anything published before it otherwise.

```sql
SELECT timestamp, key, headers['tenant'] AS tenant, payload_text::JSON->>'order_id' AS order_id
FROM pubsub.main.read_messages('orders', allow_temporary_subscription := true,
                               start_time := now() - INTERVAL 1 HOUR,
                               filter := 'attributes.tenant = "a"')
ORDER BY timestamp;
```

### Messaging vocabulary

`read_messages` uses the column names shared by the VGI messaging workers
([vgi-rabbitmq](https://github.com/Query-farm/vgi-rabbitmq), [vgi-nats](https://github.com/Query-farm/vgi-nats)),
so their results `UNION ALL`:

| Shared column | Pub/Sub meaning |
|---|---|
| `sequence BIGINT` | always `NULL`: Pub/Sub has no offsets (use `timestamp`, `message_id`) |
| `timestamp TIMESTAMPTZ` | publish time |
| `key VARCHAR` | ordering key (`NULL` when none) |
| `payload BLOB`, `payload_text VARCHAR`, `payload_bytes BIGINT` | data; text when valid UTF-8; full size before `truncate` |
| `headers MAP(VARCHAR, VARCHAR)` | message attributes |

Pub/Sub-specific columns follow: `message_id`, `delivery_attempt`, plus `project`, `topic` and
the temporary `subscription`. Consumer lag is `subscription_backlog().undelivered_messages`
(messages) and `oldest_unacked_age_seconds` (time). (The two sibling workers were being built at
the same time; this table was aligned with vgi-rabbitmq's message columns, whose README section
was not written yet.)

## Examples

```sql
-- Subscriptions with no dead-letter policy on topics that carry payments
SELECT subscription, topic FROM pubsub.main.subscriptions()
WHERE dead_letter_topic IS NULL AND topic LIKE '%payments%';

-- Public IAM grants anywhere
SELECT resource_type, resource, role, member FROM pubsub.main.iam_policies()
WHERE member IN ('allUsers', 'allAuthenticatedUsers');

-- Peak backlog per subscription over a day
SELECT subscription, max(value) AS peak
FROM pubsub.main.time_series('subscription/num_undelivered_messages',
                             start_time := TIMESTAMPTZ '2026-01-01 00:00:00+00',
                             end_time   := TIMESTAMPTZ '2026-01-02 00:00:00+00')
GROUP BY subscription ORDER BY peak DESC;

-- Topics whose schema allows a revision range
SELECT t.topic, s.revision_id, s.revision_create_time
FROM pubsub.main.topics() t JOIN pubsub.main.schema_revisions('order-avro') s ON s.name = t.schema;
```

## How it is tested

`make check` runs gofmt, go vet, golangci-lint, `go test`, the SQL suite (`test/sql/pubsub.test`,
77 statements) over the stdio, launch and HTTP transports, and vgi-lint at `fail_on = "info"`
(score 100).

**The SQL suite needs no Docker and no credentials.** It talks to `test/fixture_server/server.py`,
which replays:

- **Recorded responses from the real Pub/Sub emulator** (`recorded.json`, project `vgi-test`):
  topics with and without retention, a schema with two revisions and a topic using it,
  subscriptions with filter / ordering / dead-letter / retry / exactly-once settings, a snapshot,
  five messages with attributes, ordering keys and a non-UTF-8 payload, and complete
  `read_messages` flows (create, seek, pull, acknowledge, delete of the temporary subscription).
  Temporary-subscription names are normalized and each request is keyed by the chain of requests
  before it.
- **Hand-written, documented REST shapes** (`handwritten.json`, generated by `make_handwritten.py`,
  project `prod-shop`) for what the emulator does not implement: IAM policies (with a condition,
  a public grant and a `PERMISSION_DENIED`), Cloud Monitoring time series (paged, reduced, delta
  and distribution metrics), Protocol Buffer schemas, push / BigQuery / Cloud Storage
  subscriptions, an import topic, a CMEK topic, 401/403 errors, and an OAuth token endpoint that
  the service-account key path exchanges its signed JWT at (a throwaway RSA key is generated per
  run, never committed).

The runner starts two copies: one answers every request with `429 + Retry-After: 0` once (so
retries are always exercised), the other demands a bearer token. Point the worker at either
through the secret's `ENDPOINT` / `MONITORING_ENDPOINT`; there are no test-only code paths.

**Against the live emulator** (also a CI job): `test/fixture/run-emulator.sh start` runs
`gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators` (about 380 MB compressed, ~150-300 MB
RAM, capped at 1 GB) on `127.0.0.1:18085` and seeds it; `make test-emulator` then runs the suite
with every `vgi-test` request sent to it. `make record-fixtures` re-records `recorded.json` from a
fresh emulator. The emulator speaks REST on the same port as gRPC, which is why this worker uses
the REST APIs rather than the gRPC client libraries: the same client code runs against Google,
the emulator and the replay fixtures. It has no IAM, Monitoring, Protocol Buffer schemas or
snapshot labels, which is what the hand-written fixtures cover. GitHub service containers cannot
set the emulator's start command, so the CI job runs it with `docker run`.

## Known limitations

- `WHERE` filters are not pushed into the APIs (they have none for listings); use the named
  arguments. Monitoring filters on arbitrary labels go in `filter :=`.
- `read_messages` only sees retained messages (topic retention or a snapshot), is capped at
  10,000 messages per call, and orders rows by publish time. Pub/Sub may deliver duplicates; they
  are dropped by `message_id`.
- `iam_policies` reads resource-level policies only; inherited project / folder / organization
  grants are not included.
- No Pub/Sub Lite (deprecated), no regional endpoints beyond what `ENDPOINT` allows, no
  workload-identity federation config files (use an access token or ADC opt-in).

## Development

See [AGENTS.md](AGENTS.md) for the layout, the recipe for adding a function and the sharp edges.
`make help` lists the targets; `make template-diff` shows upstream starter changes (this worker
started from vgi-starter-go v1.3.0).

## Support

Open an issue on this repository. For VGI itself, see [query.farm](https://query.farm).

## Authorship & License

Copyright 2026 Query Farm LLC - https://query.farm

Released under the [BSD Zero Clause License](LICENSE) (0BSD). Google Cloud and Pub/Sub are
trademarks of Google LLC; this worker is not affiliated with or endorsed by Google.
