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

const catalogDocLLM = "Read Google Cloud Pub/Sub from SQL. Every function needs a gcp_pubsub DuckDB secret: CREATE SECRET " +
	"(TYPE gcp_pubsub, PROJECT_ID 'p', SERVICE_ACCOUNT_JSON '<key file>' or ACCESS_TOKEN '<token>'; for the emulator ENDPOINT " +
	"'localhost:8085' and no credentials). Local Application Default Credentials are used only when the ATTACH sets " +
	"use_application_default_credentials true. topics(), subscriptions(topic := ...), snapshots() and schemas() / " +
	"schema_revisions(schema) list resources with typed settings (retention seconds, ack deadline, dead-letter and retry policy, " +
	"exactly-once, push endpoint host only). iam_policies() flattens resource IAM to one row per role and member. " +
	"time_series(metric, start_time, end_time) returns Cloud Monitoring points as long rows (labels MAP, ts, value) and " +
	"subscription_backlog() the current consumer lag per subscription. read_messages(topic, allow_temporary_subscription := true) " +
	"replays retained messages through a temporary subscription it creates and deletes; it never pulls from an existing " +
	"subscription. access_check() shows which of these the credential may use. Everything else is read-only."

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
	"Credentials come from a `gcp_pubsub` DuckDB secret (service account key or access token)."

const schemaDocLLM = "The main schema holds every function; all are table functions taking project := (default the secret's " +
	"PROJECT_ID) and secret_name := (default the gcp_pubsub secret whose scope matches). Resource names are accepted as ids " +
	"('orders') or full names ('projects/p/topics/orders'); name columns hold full names, so join subscriptions.topic = " +
	"topics.name. Durations are `DOUBLE` seconds, times `TIMESTAMPTZ`, labels and message attributes `MAP(VARCHAR, VARCHAR)`. " +
	"read_messages shares the messaging columns of the other VGI messaging workers: timestamp (publish time), key (ordering " +
	"key), payload `BLOB`, payload_text, payload_bytes, headers (attributes). Monitoring functions need roles/monitoring.viewer."

// SchemaDocMD is the main schema's human-facing description (also its comment).
const SchemaDocMD = "## main\n\n" +
	"Every function the Pub/Sub worker exposes. Listings call the Pub/Sub REST API with read-only GETs; metrics come from " +
	"Cloud Monitoring; `read_messages` is the one function that creates (and deletes) a resource, and only when you opt in.\n\n" +
	"Start with `access_check()` and `subscriptions()`; use `subscription_backlog()` to find lagging consumers."

// agentTask is one public analyst task for `vgi-lint simulate`.
type agentTask struct {
	Name   string `json:"name"`
	Prompt string `json:"prompt"`
}

// agentTestTasks are graded privately by vgi-agent-tests.yaml.
var agentTestTasks = []agentTask{
	{Name: "formal_greeting", Prompt: "Using the pubsub worker, greet the name 'Grace' formally."},
	{Name: "sum_of_squares", Prompt: "Using the pubsub worker, what is the sum of the squared column for the first 10 rows " +
		"of the sequence with the default step?"},
	{Name: "stepped_sequence", Prompt: "List the n values of a 4-row sequence that steps by 5, in ascending order."},
	{Name: "items_per_category", Prompt: "The items API is running at https://vgi-starter-api.query-farm.services/v1. How many items does it have in each category?"},
}

// CatalogTags are the catalog-level discovery tags.
var CatalogTags = map[string]string{
	"vgi.title":              "VGI Google Cloud Pub/Sub Worker",
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
