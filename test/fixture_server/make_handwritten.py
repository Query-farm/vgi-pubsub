"""Generate handwritten.json: documented REST shapes the Pub/Sub emulator cannot produce.

    python3 test/fixture_server/make_handwritten.py

Shapes follow the public REST references:
  https://cloud.google.com/pubsub/docs/reference/rest (Topic, Subscription, Schema, Policy)
  https://cloud.google.com/monitoring/api/ref_v3/rest/v3/projects.timeSeries/list

Project ``prod-shop`` is a hand-written "production" project: CMEK topic with a
Protocol Buffer schema, an import (ingestion) topic, push / BigQuery / Cloud
Storage subscriptions, IAM policies (one with a condition, one public, one
denied) and Cloud Monitoring series. Project ``denied-project`` answers 403.
"""

import json
import os
import urllib.parse

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = {}
P = "prod-shop"
PP = f"projects/{P}"


def key(method, path, params=None, body=None):
    items = sorted((params or {}).items())
    flat = []
    for k, v in items:
        for x in v if isinstance(v, list) else [v]:
            flat.append((k, x))
    q = urllib.parse.urlencode(sorted(flat))
    b = json.dumps(body, sort_keys=True, separators=(",", ":")) if body is not None else ""
    return f"{method} {path}{'?' + q if q else ''} {b}".rstrip()


def put(k, body, status=200):
    OUT[k] = {"status": status, "body": json.dumps(body, sort_keys=True)}


def err(code, status, msg):
    return {"error": {"code": code, "message": msg, "status": status}}


PROTO = 'syntax = "proto3";\nmessage Payment {\n  string id = 1;\n  int64 amount_cents = 2;\n}\n'
PROTO_V1 = 'syntax = "proto3";\nmessage Payment {\n  string id = 1;\n}\n'

topics = [
    {"name": f"{PP}/topics/payments", "labels": {"team": "billing", "tier": "critical"},
     "kmsKeyName": f"{PP}/locations/us/keyRings/pubsub/cryptoKeys/payments",
     "messageStoragePolicy": {"allowedPersistenceRegions": ["us-central1", "us-east1"], "enforceInTransit": True},
     "schemaSettings": {"schema": f"{PP}/schemas/payment-proto", "encoding": "BINARY",
                        "firstRevisionId": "a1b2c3d4", "lastRevisionId": "e5f6a7b8"},
     "messageRetentionDuration": "604800s", "state": "ACTIVE", "satisfiesPzs": False},
    {"name": f"{PP}/topics/clickstream-import", "labels": {},
     "ingestionDataSourceSettings": {"awsKinesis": {"streamArn": "arn:aws:kinesis:us-east-1:111122223333:stream/clicks",
                                                    "consumerArn": "arn:aws:kinesis:us-east-1:111122223333:stream/clicks/consumer/gcp:1",
                                                    "awsRoleArn": "arn:aws:iam::111122223333:role/pubsub-import",
                                                    "gcpServiceAccount": "importer@prod-shop.iam.gserviceaccount.com",
                                                    "state": "ACTIVE"}},
     "state": "ACTIVE"},
    {"name": f"{PP}/topics/audit"},
]
put(key("GET", f"/v1/{PP}/topics", {"pageSize": "100"}), {"topics": topics})
for t in topics:
    put(key("GET", "/v1/" + t["name"]), t)

subscriptions = [
    {"name": f"{PP}/subscriptions/payments-push", "topic": f"{PP}/topics/payments",
     "pushConfig": {"pushEndpoint": "https://hooks.example.com/pubsub/receive?token=SECRET-IN-URL",
                    "oidcToken": {"serviceAccountEmail": "pusher@prod-shop.iam.gserviceaccount.com",
                                  "audience": "https://hooks.example.com/secret-audience"},
                    "attributes": {"x-goog-version": "v1"}, "noWrapper": {"writeMetadata": True}},
     "ackDeadlineSeconds": 20, "messageRetentionDuration": "604800s", "expirationPolicy": {},
     "retryPolicy": {"minimumBackoff": "10s", "maximumBackoff": "600s"}, "state": "ACTIVE"},
    {"name": f"{PP}/subscriptions/payments-bq", "topic": f"{PP}/topics/payments",
     "bigqueryConfig": {"table": "prod-shop.analytics.payments", "useTopicSchema": True, "writeMetadata": True,
                        "state": "ACTIVE", "serviceAccountEmail": "bq-writer@prod-shop.iam.gserviceaccount.com"},
     "ackDeadlineSeconds": 60, "messageRetentionDuration": "604800s", "state": "ACTIVE"},
    {"name": f"{PP}/subscriptions/clicks-archive", "topic": f"{PP}/topics/clickstream-import",
     "cloudStorageConfig": {"bucket": "prod-shop-clicks", "filenamePrefix": "clicks/", "maxDuration": "300s",
                            "avroConfig": {"writeMetadata": True}, "state": "PERMISSION_DENIED"},
     "ackDeadlineSeconds": 300, "messageRetentionDuration": "604800s", "state": "RESOURCE_ERROR"},
    {"name": f"{PP}/subscriptions/payments-worker", "topic": f"{PP}/topics/payments",
     "ackDeadlineSeconds": 10, "messageRetentionDuration": "86400s", "enableExactlyOnceDelivery": True,
     "deadLetterPolicy": {"deadLetterTopic": f"{PP}/topics/audit"},
     "labels": {"team": "billing"}, "state": "ACTIVE"},
]
put(key("GET", f"/v1/{PP}/subscriptions", {"pageSize": "100"}), {"subscriptions": subscriptions})
put(key("GET", f"/v1/{PP}/snapshots", {"pageSize": "100"}), {})
put(key("GET", f"/v1/{PP}/schemas", {"pageSize": "100", "view": "FULL"}),
    {"schemas": [{"name": f"{PP}/schemas/payment-proto", "type": "PROTOCOL_BUFFER", "definition": PROTO,
                  "revisionId": "e5f6a7b8", "revisionCreateTime": "2026-03-02T10:00:00Z"}]})
put(key("GET", f"/v1/{PP}/schemas/payment-proto:listRevisions", {"pageSize": "100", "view": "FULL"}),
    {"schemas": [{"name": f"{PP}/schemas/payment-proto", "type": "PROTOCOL_BUFFER", "definition": PROTO,
                  "revisionId": "e5f6a7b8", "revisionCreateTime": "2026-03-02T10:00:00Z"},
                 {"name": f"{PP}/schemas/payment-proto", "type": "PROTOCOL_BUFFER", "definition": PROTO_V1,
                  "revisionId": "a1b2c3d4", "revisionCreateTime": "2026-01-15T09:30:00Z"}]})

put(key("GET", f"/v1/{PP}/schemas", {"pageSize": "100"}),
    {"schemas": [{"name": f"{PP}/schemas/payment-proto", "type": "PROTOCOL_BUFFER", "revisionId": "e5f6a7b8"}]})

# IAM (getIamPolicy with options.requestedPolicyVersion=3).
IAM = {"options.requestedPolicyVersion": "3"}
policies = {
    f"{PP}/topics/payments": {"version": 3, "etag": "BwYA1", "bindings": [
        {"role": "roles/pubsub.publisher", "members": ["serviceAccount:checkout@prod-shop.iam.gserviceaccount.com",
                                                       "group:payments-eng@example.com"]},
        {"role": "roles/pubsub.viewer", "members": ["user:ada@example.com"],
         "condition": {"title": "business-hours", "expression": "request.time.getHours('UTC') < 18"}}]},
    f"{PP}/topics/clickstream-import": {"version": 1, "etag": "BwYA2", "bindings": [
        {"role": "roles/pubsub.subscriber", "members": ["allUsers"]}]},
    f"{PP}/topics/audit": err(403, "PERMISSION_DENIED", "User not authorized to perform this action."),
    f"{PP}/subscriptions/payments-push": {"version": 1, "etag": "BwYA3"},
    f"{PP}/subscriptions/payments-bq": {"version": 1, "etag": "BwYA4", "bindings": [
        {"role": "roles/pubsub.subscriber", "members": ["serviceAccount:service-123@gcp-sa-pubsub.iam.gserviceaccount.com"]}]},
    f"{PP}/subscriptions/clicks-archive": {"version": 1, "etag": "BwYA5"},
    f"{PP}/subscriptions/payments-worker": {"version": 1, "etag": "BwYA6", "bindings": [
        {"role": "roles/pubsub.subscriber", "members": ["serviceAccount:worker@prod-shop.iam.gserviceaccount.com",
                                                        "deleted:serviceAccount:old@prod-shop.iam.gserviceaccount.com?uid=1"]}]},
    f"{PP}/schemas/payment-proto": {"version": 1, "etag": "BwYA7", "bindings": [
        {"role": "roles/pubsub.viewer", "members": ["domain:example.com"]}]},
}
for name, pol in policies.items():
    status = pol["error"]["code"] if "error" in pol else 200
    put(key("GET", f"/v1/{name}:getIamPolicy", IAM), pol, status)

# testIamPermissions (access_check): granted.
put(key("POST", f"/v1/{PP}/topics/payments:testIamPermissions", None,
        {"permissions": ["pubsub.topics.attachSubscription"]}), {"permissions": ["pubsub.topics.attachSubscription"]})
put(key("GET", f"/v1/{PP}/topics", {"pageSize": "1"}), {"topics": topics[:1], "nextPageToken": "t2"})
put(key("GET", f"/v1/{PP}/subscriptions", {"pageSize": "1"}), {"subscriptions": subscriptions[:1], "nextPageToken": "s2"})
put(key("GET", f"/v1/{PP}/snapshots", {"pageSize": "1"}), {})
put(key("GET", f"/v1/{PP}/schemas", {"pageSize": "1"}), err(403, "PERMISSION_DENIED", "User not authorized to perform this action."), 403)

# A project the credential may not read at all.
put(key("GET", "/v1/projects/denied-project/topics", {"pageSize": "100"}),
    err(403, "PERMISSION_DENIED", "User not authorized to perform this action."), 403)

# ---------------------------------------------------------------- Cloud Monitoring
# interval.* parameters are omitted: the server matches without them.
TS = f"/v3/{PP}/timeSeries"


def mfilter(metric, sub=None, topic=None):
    f = f'metric.type = "pubsub.googleapis.com/{metric}"'
    if sub:
        f += f' AND resource.labels.subscription_id = "{sub}"'
    if topic:
        f += f' AND resource.labels.topic_id = "{topic}"'
    return f


def points(start_minute, values, kind="int64Value"):
    # newest first, as the API returns them; one point per minute from 2026-01-01 00:MM
    out = []
    for i, v in enumerate(values):
        end = f"2026-01-01T00:{start_minute + i:02d}:00Z"
        val = {kind: str(v) if kind == "int64Value" else v}
        out.append({"interval": {"startTime": end, "endTime": end}, "value": val})
    return list(reversed(out))


def series(metric, rtype, rlabels, pts, kind="GAUGE", vtype="INT64", mlabels=None, unit="1"):
    return {"metric": {"type": f"pubsub.googleapis.com/{metric}", "labels": mlabels or {}},
            "resource": {"type": rtype, "labels": dict(project_id=P, **rlabels)},
            "metricKind": kind, "valueType": vtype, "unit": unit, "points": pts}


undelivered = [
    series("subscription/num_undelivered_messages", "pubsub_subscription", {"subscription_id": "payments-worker"},
           points(0, [120, 340, 560])),
    series("subscription/num_undelivered_messages", "pubsub_subscription", {"subscription_id": "payments-bq"},
           points(0, [0, 0, 2])),
]
base = {"aggregation.alignmentPeriod": "60s", "aggregation.perSeriesAligner": "ALIGN_MEAN", "pageSize": "1000"}
put(key("GET", TS, dict(base, filter=mfilter("subscription/num_undelivered_messages"))),
    {"timeSeries": undelivered[:1], "nextPageToken": "page-2", "unit": "1"})
put(key("GET", TS, dict(base, filter=mfilter("subscription/num_undelivered_messages"), pageToken="page-2")),
    {"timeSeries": undelivered[1:]})
put(key("GET", TS, dict(base, filter=mfilter("subscription/num_undelivered_messages", sub="payments-worker"))),
    {"timeSeries": undelivered[:1]})
# Server-side reduction: total backlog across subscriptions.
put(key("GET", TS, dict(base, filter=mfilter("subscription/num_undelivered_messages"),
                        **{"aggregation.crossSeriesReducer": "REDUCE_SUM"})),
    {"timeSeries": [series("subscription/num_undelivered_messages", "pubsub_subscription", {},
                           points(0, [120, 340, 562]))]})

sends = [
    series("topic/send_request_count", "pubsub_topic", {"topic_id": "payments"}, points(0, [30, 45]),
           kind="DELTA", mlabels={"response_class": "success", "response_code": "success"}),
    series("topic/send_request_count", "pubsub_topic", {"topic_id": "payments"}, points(0, [1, 0]),
           kind="DELTA", mlabels={"response_class": "invalid", "response_code": "invalid_argument"}),
]
put(key("GET", TS, {"aggregation.alignmentPeriod": "300s", "aggregation.perSeriesAligner": "ALIGN_SUM",
                    "filter": mfilter("topic/send_request_count", topic="payments"), "pageSize": "1000"}),
    {"timeSeries": sends})
# Distribution metric (message sizes): value is the mean.
put(key("GET", TS, {"filter": mfilter("topic/message_sizes", topic="payments"), "pageSize": "1000"}),
    {"timeSeries": [{"metric": {"type": "pubsub.googleapis.com/topic/message_sizes", "labels": {}},
                     "resource": {"type": "pubsub_topic", "labels": {"project_id": P, "topic_id": "payments"}},
                     "metricKind": "DELTA", "valueType": "DISTRIBUTION", "unit": "By",
                     "points": [{"interval": {"startTime": "2026-01-01T00:00:00Z", "endTime": "2026-01-01T00:01:00Z"},
                                 "value": {"distributionValue": {"count": "4", "mean": 812.5}}}]}]})

# subscription_backlog: latest point per subscription of two metrics.
bl = {"aggregation.alignmentPeriod": "60s", "aggregation.perSeriesAligner": "ALIGN_MAX", "pageSize": "10000"}
put(key("GET", TS, dict(bl, filter=mfilter("subscription/num_undelivered_messages"))), {"timeSeries": undelivered})
age = [
    series("subscription/oldest_unacked_message_age", "pubsub_subscription", {"subscription_id": "payments-worker"},
           points(0, [60, 120, 180]), unit="s"),
    series("subscription/oldest_unacked_message_age", "pubsub_subscription", {"subscription_id": "payments-push"},
           points(0, [5]), unit="s"),
]
put(key("GET", TS, dict(bl, filter=mfilter("subscription/oldest_unacked_message_age"))), {"timeSeries": age})
put(key("GET", TS, dict(bl, filter=mfilter("subscription/num_undelivered_messages", sub="payments-worker"))),
    {"timeSeries": undelivered[:1]})
put(key("GET", TS, dict(bl, filter=mfilter("subscription/oldest_unacked_message_age", sub="payments-worker"))),
    {"timeSeries": age[:1]})
# access_check probe.
put(key("GET", TS, {"filter": mfilter("subscription/num_undelivered_messages"), "pageSize": "1"}), {})
# A credential without roles/monitoring.viewer (the emulator project).
put(key("GET", "/v3/projects/vgi-test/timeSeries", {"filter": mfilter("subscription/num_undelivered_messages"), "pageSize": "1"}),
    err(403, "PERMISSION_DENIED", "Permission monitoring.timeSeries.list denied (or the resource may not exist)."), 403)
put(key("GET", "/v3/projects/vgi-test/timeSeries", dict(base, filter=mfilter("subscription/num_undelivered_messages"))),
    err(403, "PERMISSION_DENIED", "Permission monitoring.timeSeries.list denied (or the resource may not exist)."), 403)

with open(os.path.join(HERE, "handwritten.json"), "w") as f:
    json.dump(OUT, f, indent=1, sort_keys=True)
    f.write("\n")
print(f"wrote {len(OUT)} responses")
