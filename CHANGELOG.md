# Changelog

## 0.1.1 (2026-11-16)

- Bump vgi-go to 0.42.2.
- CI tooling (haybarn-unittest, lint helpers) runs on Python 3.14.

## 0.1.0 (unreleased)

Initial Google Cloud Pub/Sub worker (from vgi-starter-go v1.3.0).

- topics, subscriptions, snapshots, schemas, schema_revisions over the Pub/Sub REST API, paged with
  externalized page tokens; push endpoints reduced to their host.
- iam_policies: resource IAM flattened to role/member rows, unreadable policies reported as rows.
- time_series and subscription_backlog from Cloud Monitoring.
- read_messages: opt-in replay of retained messages through a temporary subscription that is
  created, seeked, drained, acknowledged and deleted within one call; never pulls from an existing
  subscription.
- access_check, and offline resource_id / resource_project / duration_seconds scalars.
- gcp_pubsub secret (service-account key JSON, access token, emulator endpoint); ADC only with the
  use_application_default_credentials ATTACH opt-in.

Targets: vgi-go 0.42 · VGI protocol 3.3 · Haybarn 2.0.0rc11 · vgi-lint-check 2.x (unpinned)
