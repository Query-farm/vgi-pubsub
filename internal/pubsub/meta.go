// Discovery metadata: the one file to edit when you rename or extend the worker.
//
// DuckDB, coding agents and the vgi-lint metadata checker all read these tags.
// Keep every object documented and `make lint-explain` stays at 100.
//
//   - Catalog-level tags (title, license, author, links, long description)
//     live in CatalogTags below.
//   - Every function carries objectTags(...): a title, a category from
//     SchemaCategories, search keywords, and LLM- and human-facing docs.

package pubsub

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
)

// CatalogName is the name DuckDB attaches the worker under.
const CatalogName = "pubsub"

// SourceURL is where this worker's code lives.
const SourceURL = "https://github.com/Query-farm/vgi-pubsub"

// category is one entry of the schema's vgi.categories list.
type category struct {
	Name        string `json:"name"`
	Title       string `json:"title"`
	Description string `json:"description"`
}

// SchemaCategories group functions in docs and agent tools. Every function's
// vgi.category must name one of these.
var SchemaCategories = []category{
	{Name: "resources", Title: "Topics And Subscriptions", Description: "Topics, subscriptions and snapshots with their delivery, retention, encryption and export settings."},
	{Name: "schemas", Title: "Schemas", Description: "Avro and Protocol Buffer schemas and their revision history."},
	{Name: "security", Title: "IAM", Description: "Resource-level IAM policies flattened to role/member rows."},
	{Name: "metrics", Title: "Backlog And Metrics", Description: "Consumer lag and throughput from Cloud Monitoring."},
	{Name: "messages", Title: "Messages", Description: "Opt-in replay of retained messages through a temporary subscription."},
	{Name: "status", Title: "Access", Description: "Feature detection of what the credential may read."},
	{Name: "tools", Title: "Name And Duration Tools", Description: "Offline scalars for resource names and API duration text (no secret)."},
}

// mustJSON marshals v, panicking on failure (only static data is passed).
func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// keywordsArray turns "a, b, c" into the JSON array string vgi.keywords requires.
func keywordsArray(keywords string) string {
	out := []string{}
	for _, k := range strings.Split(keywords, ",") {
		if k = strings.TrimSpace(k); k != "" {
			out = append(out, k)
		}
	}
	return mustJSON(out)
}

// docs is the set of standard per-object discovery fields.
type docs struct {
	Title    string
	Category string
	Keywords string
	DocLLM   string
	DocMD    string
}

// objectTags builds the standard per-object discovery tags.
func objectTags(d docs) map[string]string {
	known := false
	for _, c := range SchemaCategories {
		known = known || c.Name == d.Category
	}
	if !known {
		panic(fmt.Sprintf("unknown category %q; add it to SchemaCategories", d.Category))
	}
	return map[string]string{
		"vgi.title":    d.Title,
		"vgi.category": d.Category,
		"vgi.keywords": keywordsArray(d.Keywords),
		"vgi.doc_llm":  d.DocLLM,
		"vgi.doc_md":   d.DocMD,
	}
}

// resultColumn describes one output column for vgi.result_columns_schema.
type resultColumn struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Description string `json:"description"`
}

// column builds an Arrow field whose comment shows up in DuckDB's DESCRIBE and agent tools.
func column(name string, typ arrow.DataType, comment string, nullable bool) arrow.Field {
	return arrow.Field{Name: name, Type: typ, Nullable: nullable,
		Metadata: arrow.NewMetadata([]string{"comment"}, []string{comment})}
}

// exampleQuery is one entry of vgi.example_queries.
type exampleQuery struct {
	Description string `json:"description"`
	SQL         string `json:"sql"`
}

// exampleQueries converts catalog examples to vgi.example_queries entries.
func exampleQueries(lists ...[]vgi.CatalogExample) []exampleQuery {
	out := []exampleQuery{}
	for _, examples := range lists {
		for _, ex := range examples {
			out = append(out, exampleQuery{Description: ex.Description, SQL: ex.SQL})
		}
	}
	return out
}

// withExamples publishes examples (with descriptions) as vgi.example_queries.
// Overloads share one SQL name, so pass every overload's examples.
func withExamples(tags map[string]string, lists ...[]vgi.CatalogExample) map[string]string {
	tags["vgi.example_queries"] = mustJSON(exampleQueries(lists...))
	return tags
}

const catalogDocLLM = "Inventory, audit and troubleshoot Google Cloud Pub/Sub from SQL: what topics and subscriptions exist and " +
	"how they deliver (pull, push, BigQuery, Cloud Storage), how long messages are retained, which schemas and encryption keys " +
	"apply, who holds which IAM role, and how far behind each consumer is. Every API function needs a gcp_pubsub DuckDB secret: " +
	"CREATE SECRET (TYPE gcp_pubsub, PROJECT_ID 'p', SERVICE_ACCOUNT_JSON '<key file>' or ACCESS_TOKEN '<token>'); for the " +
	"emulator ENDPOINT 'localhost:8085' and no credentials. Local Application Default Credentials are used only when the ATTACH " +
	"sets use_application_default_credentials true. Durations are `DOUBLE` seconds, times `TIMESTAMPTZ`, labels and message " +
	"attributes `MAP(VARCHAR, VARCHAR)`; metrics come from Cloud Monitoring as long rows (labels, ts, value). Message contents " +
	"are only read on explicit opt-in, through a temporary subscription the worker creates and deletes: an existing subscription " +
	"is never pulled from. Nothing else changes the project."

// CatalogDocMD is the catalog's comment: one short paragraph for directories.
const CatalogDocMD = "Google Cloud Pub/Sub as SQL: topics, subscriptions, snapshots, schemas and IAM policies, backlog and " +
	"throughput metrics from Cloud Monitoring, and opt-in replay of retained messages. Works with the Pub/Sub emulator."

const catalogDocMDLong = "# Google Cloud Pub/Sub in DuckDB\n\n" +
	"Inventory and troubleshoot Pub/Sub from SQL with typed columns: durations as `DOUBLE` seconds, times as `TIMESTAMPTZ`, " +
	"labels and attributes as `MAP`.\n\n" +
	"- **Resources**: topics (retention, schema, CMEK, ingestion), subscriptions (type, ack deadline, dead-letter, retry, " +
	"exactly-once, filter; push endpoint host only), snapshots, schemas and revisions.\n" +
	"- **IAM**: resource policies flattened to role/member rows, with unreadable policies reported, not hidden.\n" +
	"- **Backlog**: Cloud Monitoring series (`labels`, `ts`, `value`) and the current consumer lag per subscription.\n" +
	"- **Messages**: opt-in replay of retained messages through a temporary subscription that the worker deletes. " +
	"Existing subscriptions are never pulled from.\n\n" +
	"Credentials come from a `gcp_pubsub` DuckDB secret (service account key or access token).\n\n" +
	"Upstream references: the [Pub/Sub REST API](https://cloud.google.com/pubsub/docs/reference/rest), " +
	"[Pub/Sub monitoring metrics](https://cloud.google.com/pubsub/docs/monitoring) via the " +
	"[Cloud Monitoring API](https://cloud.google.com/monitoring/api/v3), " +
	"[Pub/Sub access control](https://cloud.google.com/pubsub/docs/access-control), and the " +
	"[Pub/Sub emulator](https://cloud.google.com/pubsub/docs/emulator)."

const schemaDocLLM = "The main schema holds every function; all are table functions taking project := (default the secret's " +
	"PROJECT_ID) and secret_name := (default the gcp_pubsub secret whose scope matches). Resource names are accepted as ids " +
	"('orders') or full names ('projects/p/topics/orders'); name columns hold full names, so join subscriptions.topic = " +
	"topics.name. Durations are `DOUBLE` seconds, times `TIMESTAMPTZ`, labels and message attributes `MAP(VARCHAR, VARCHAR)`. " +
	"read_messages shares the messaging columns of the other VGI messaging workers: timestamp (publish time), key (ordering " +
	"key), payload `BLOB`, payload_text, payload_bytes, headers (attributes). Monitoring functions need roles/monitoring.viewer."

// SchemaComment is the main schema's one-line comment.
const SchemaComment = "Read-only SQL over one Google Cloud project's Pub/Sub resources, IAM, backlog metrics and opt-in message replay."

// SchemaDocMD is the main schema's human-facing description.
const SchemaDocMD = "## main\n\n" +
	"A read-only view of one Google Cloud project's Pub/Sub deployment, for platform engineers auditing configuration " +
	"and on-call engineers chasing a lagging consumer. Resource listings are read-only GETs against the Pub/Sub REST " +
	"API; time series come from Cloud Monitoring and need `roles/monitoring.viewer`.\n\n" +
	"Every function takes `project` (default: the secret's `PROJECT_ID`) and `secret_name` (default: the matching " +
	"`gcp_pubsub` secret). Names are accepted as short ids or full resource names, and name columns always hold full " +
	"names, so joins between resources work without string surgery.\n\n" +
	"Message contents are never read from an existing subscription. Replaying retained messages is an explicit opt-in " +
	"that creates a temporary subscription and deletes it afterwards; nothing else changes the project."

// agentTask is one public analyst task for `vgi-lint simulate`.
type agentTask struct {
	Name   string `json:"name"`
	Prompt string `json:"prompt"`
}

// agentTestTasks are graded privately by vgi-agent-tests.yaml. They assume a
// gcp_pubsub secret pointing at the hand-written prod-shop fixture
// (test/fixture_server, project prod-shop).
var agentTestTasks = []agentTask{
	{Name: "push_hosts", Prompt: "Which Pub/Sub subscriptions deliver by push, and to which host?"},
	{Name: "public_grants", Prompt: "Which Pub/Sub resources grant a role to allUsers or allAuthenticatedUsers?"},
	{Name: "dead_letter", Prompt: "Which subscriptions have a dead-letter topic, and after how many delivery attempts are messages dead-lettered?"},
	{Name: "export_problems", Prompt: "Which BigQuery or Cloud Storage export subscriptions are not in the ACTIVE state?"},
	{Name: "schema_history", Prompt: "How many revisions does the payment-proto schema have, and when was the newest one created?"},
	{Name: "worst_backlog", Prompt: "At 2026-01-01 00:05 UTC, which subscription had the most undelivered messages, and how many?"},
	{Name: "cmek_topics", Prompt: "Which topics are encrypted with a customer-managed (Cloud KMS) key?"},
	{Name: "missing_access", Prompt: "Which Pub/Sub capabilities is the current credential not allowed to use?"},
	{Name: "schema_types", Prompt: "List each Pub/Sub schema with its type."},
	{Name: "snapshot_count", Prompt: "How many Pub/Sub snapshots exist in the project?"},
	{Name: "peak_backlog", Prompt: "What was the peak number of undelivered messages for the payments-worker subscription between 2026-01-01 00:00 and 00:03 UTC, according to Cloud Monitoring?"},
	{Name: "payment_events", Prompt: "Replay the retained messages of the payments topic and count them per event attribute. You may create a temporary subscription for this."},
	{Name: "cross_project", Prompt: "Using the pubsub worker's helpers, which project does the topic name 'projects/shared-infra/topics/events' belong to?"},
	{Name: "retention_days", Prompt: "Using the pubsub worker's helpers, how many days is the API duration '1209600s'?"},
	{Name: "subscription_ids", Prompt: "Using the pubsub worker's helpers, what are the ids of the resource names 'projects/p/topics/orders' and 'projects/p/subscriptions/orders-worker'?"},
}

// CatalogTags are the catalog-level discovery tags.
var CatalogTags = map[string]string{
	"vgi.title":              "Google Cloud Pub/Sub",
	"vgi.subtitle":           "Topics, subscriptions, schemas, IAM, backlog metrics and opt-in message replay from Google Cloud Pub/Sub.",
	"vgi.doc_llm":            catalogDocLLM,
	"vgi.doc_md":             catalogDocMDLong,
	"vgi.source_url":         SourceURL,
	"vgi.icon_url":           "https://query.farm/media-kit/products/vgi/vgi-logo.png",
	"vgi.license":            "0BSD",
	"vgi.author":             "Query.Farm",
	"vgi.copyright":          "Copyright 2026 Query Farm LLC - https://query.farm",
	"vgi.keywords":           keywordsArray("google cloud pub/sub, pubsub, gcp, messaging, topics, subscriptions, backlog, consumer lag, iam, cloud monitoring"),
	"vgi.support_contact":    SourceURL + "/issues",
	"vgi.support_policy_url": SourceURL + "#support",
	// Public analyst tasks for `vgi-lint simulate`; private graders live in vgi-agent-tests.yaml.
	"vgi.agent_test_tasks": mustJSON(agentTestTasks),
}

// schemaTags builds the main schema's tags; allExamples feeds vgi.example_queries.
func schemaTags(allExamples []exampleQuery) map[string]string {
	return map[string]string{
		"vgi.title":           "Pub/Sub Functions",
		"domain":              "messaging",
		"vgi.keywords":        keywordsArray("topics, subscriptions, snapshots, schemas, iam, backlog, time series, messages"),
		"vgi.doc_llm":         schemaDocLLM,
		"vgi.doc_md":          SchemaDocMD,
		"vgi.categories":      mustJSON(SchemaCategories),
		"vgi.example_queries": mustJSON(allExamples),
	}
}
