#!/usr/bin/env bash
# Start / seed / stop the Google Cloud Pub/Sub emulator used to record the
# replay fixtures (test/fixture_server/recorded.json).
#
#   test/fixture/run-emulator.sh start    # container vgi-pubsub-emulator on 127.0.0.1:18085, seeded
#   test/fixture/run-emulator.sh seed     # (re)create the deterministic topics/subscriptions/messages
#   test/fixture/run-emulator.sh stop     # docker rm -f
#
# Image: gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators (~380 MB
# compressed; the emulator JVM uses ~150-300 MB, capped at 1 GB here).
# The emulator speaks gRPC and REST on one port; the worker uses REST.
# It does not implement IAM (getIamPolicy -> 501), Protocol Buffer schemas or Cloud Monitoring, which
# are covered by test/fixture_server/handwritten.json instead.
set -euo pipefail
NAME=vgi-pubsub-emulator
PORT="${PUBSUB_EMULATOR_PORT:-18085}"
IMAGE=gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators
P=vgi-test
E="http://127.0.0.1:$PORT/v1/projects/$P"

j() { # j METHOD PATH [BODY]
  local out
  out="$(curl -sS -m 20 -X "$1" -H 'Content-Type: application/json' "$E$2" ${3:+-d "$3"})"
  if printf '%s' "$out" | grep -q '"error"'; then echo "seed: $1 $2 failed: $out" >&2; exit 1; fi
}
b64() { printf '%s' "$1" | base64 | tr -d '\n'; }

seed() {
  local avro1='{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}'
  local avro2='{"type":"record","name":"Order","fields":[{"name":"id","type":"string"},{"name":"qty","type":"int","default":0}]}'
  j POST '/schemas?schemaId=order-avro' "{\"type\":\"AVRO\",\"definition\":$(printf '%s' "$avro1" | python3 -c 'import json,sys;print(json.dumps(sys.stdin.read()))')}"
  j POST '/schemas/order-avro:commit' "{\"schema\":{\"type\":\"AVRO\",\"definition\":$(printf '%s' "$avro2" | python3 -c 'import json,sys;print(json.dumps(sys.stdin.read()))')}}"

  j PUT /topics/orders '{"labels":{"team":"shop","env":"test"},"messageRetentionDuration":"86400s"}'
  j PUT /topics/orders-dlq '{"labels":{"team":"shop"}}'
  j PUT /topics/events '{}'
  j PUT /topics/typed-orders '{"schemaSettings":{"schema":"projects/vgi-test/schemas/order-avro","encoding":"JSON"},"messageRetentionDuration":"3600s"}'

  j PUT /subscriptions/orders-worker '{"topic":"projects/vgi-test/topics/orders","ackDeadlineSeconds":30,"enableMessageOrdering":true,"labels":{"env":"test"}}'
  j PUT /subscriptions/orders-tenant-a '{"topic":"projects/vgi-test/topics/orders","filter":"attributes.tenant = \"a\"","retainAckedMessages":true,"messageRetentionDuration":"86400s"}'
  j PUT /subscriptions/orders-retry '{"topic":"projects/vgi-test/topics/orders","deadLetterPolicy":{"deadLetterTopic":"projects/vgi-test/topics/orders-dlq","maxDeliveryAttempts":7},"retryPolicy":{"minimumBackoff":"10s","maximumBackoff":"600s"},"enableExactlyOnceDelivery":true,"expirationPolicy":{"ttl":"172800s"}}'
  j PUT /subscriptions/events-audit '{"topic":"projects/vgi-test/topics/events","ackDeadlineSeconds":120}'

  # Messages: attributes, ordering keys, a JSON payload, a non-UTF-8 payload, an empty one.
  j POST /topics/orders:publish "{\"messages\":[
    {\"data\":\"$(b64 '{"order_id":1,"total":9.5}')\",\"attributes\":{\"tenant\":\"a\",\"event\":\"created\"},\"orderingKey\":\"cust-1\"},
    {\"data\":\"$(b64 '{"order_id":2,"total":20}')\",\"attributes\":{\"tenant\":\"b\",\"event\":\"created\"},\"orderingKey\":\"cust-2\"},
    {\"data\":\"$(b64 '{"order_id":1,"total":9.5}')\",\"attributes\":{\"tenant\":\"a\",\"event\":\"paid\"},\"orderingKey\":\"cust-1\"},
    {\"data\":\"/wD+AQ==\",\"attributes\":{\"tenant\":\"b\",\"event\":\"binary\"}},
    {\"data\":\"$(b64 'plain text')\",\"attributes\":{\"tenant\":\"c\"}}]}"
  j POST /topics/typed-orders:publish "{\"messages\":[{\"data\":\"$(b64 '{"id":"o-1","qty":3}')\"}]}"
  # Snapshot of orders-retry's backlog (all five messages unacked).
  j PUT /snapshots/orders-before-deploy '{"subscription":"projects/vgi-test/subscriptions/orders-retry","labels":{"reason":"deploy"}}'
  echo "seeded $E"
}

case "${1:-}" in
  start)
    docker rm -f "$NAME" >/dev/null 2>&1 || true
    docker run -d --name "$NAME" -m 1g -p "127.0.0.1:$PORT:8085" "$IMAGE" \
      gcloud beta emulators pubsub start --host-port=0.0.0.0:8085 --project="$P" >/dev/null
    for _ in $(seq 1 90); do
      curl -s -o /dev/null "http://127.0.0.1:$PORT/" && break
      sleep 1
    done
    seed
    echo "http://127.0.0.1:$PORT"
    ;;
  seed) seed ;;
  stop) docker rm -f "$NAME" >/dev/null 2>&1 || true ;;
  *) echo "usage: $0 start|seed|stop" >&2; exit 2 ;;
esac
