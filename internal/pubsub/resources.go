// Metadata listings: topics, subscriptions, snapshots, schemas and schema
// revisions. All are read-only GETs against the Pub/Sub REST API
// (https://cloud.google.com/pubsub/docs/reference/rest), paged with the API's
// pageToken.

package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
)

// ---------------------------------------------------------------- topics

type topicJSON struct {
	Name                     string            `json:"name"`
	Labels                   map[string]string `json:"labels"`
	KmsKeyName               string            `json:"kmsKeyName"`
	MessageRetentionDuration string            `json:"messageRetentionDuration"`
	State                    string            `json:"state"`
	SatisfiesPzs             *bool             `json:"satisfiesPzs"`
	MessageStoragePolicy     *struct {
		AllowedPersistenceRegions []string `json:"allowedPersistenceRegions"`
		EnforceInTransit          *bool    `json:"enforceInTransit"`
	} `json:"messageStoragePolicy"`
	SchemaSettings *struct {
		Schema          string `json:"schema"`
		Encoding        string `json:"encoding"`
		FirstRevisionID string `json:"firstRevisionId"`
		LastRevisionID  string `json:"lastRevisionId"`
	} `json:"schemaSettings"`
	IngestionDataSourceSettings map[string]json.RawMessage `json:"ingestionDataSourceSettings"`
	MessageTransforms           json.RawMessage            `json:"messageTransforms"`
}

var topicColumns = []arrow.Field{
	column("project", str, "Project that owns the topic.", false),
	column("topic", str, "Topic id (the last part of the name).", false),
	column("name", str, "Full resource name, projects/<project>/topics/<topic>.", false),
	column("labels", strMap, "Resource labels (empty map when none).", false),
	column("message_retention_seconds", f64, "How long the topic retains published messages (topic retention), in seconds; NULL when retention is off. Retention is what lets read_messages replay past messages.", true),
	column("schema", str, "Full name of the schema messages must conform to; NULL when none.", true),
	column("schema_encoding", str, "Message encoding required by the schema: 'JSON' or 'BINARY'.", true),
	column("schema_first_revision_id", str, "Oldest schema revision messages may use; NULL = any.", true),
	column("schema_last_revision_id", str, "Newest schema revision messages may use; NULL = any.", true),
	column("kms_key_name", str, "Cloud KMS key protecting the topic (CMEK); NULL = Google-managed encryption.", true),
	column("allowed_persistence_regions", strList, "Regions where messages may be stored (message storage policy); empty = no restriction.", false),
	column("enforce_in_transit", boolean, "Whether the storage policy is also enforced in transit.", true),
	column("ingestion_source", str, "Import topic source: 'awsKinesis', 'cloudStorage', 'azureEventHubs', 'awsMsk', 'confluentCloud'; NULL for an ordinary topic.", true),
	column("ingestion_settings", str, "The ingestion data source settings as JSON (open shape); NULL for an ordinary topic.", true),
	column("message_transforms", str, "Single Message Transforms (UDFs) as JSON; NULL when none.", true),
	column("state", str, "Topic state: 'ACTIVE' or 'INGESTION_RESOURCE_ERROR'; NULL when the API omits it (emulator).", true),
	column("satisfies_pzs", boolean, "Whether the topic satisfies Physical Zone Separation.", true),
}

func topicRow(t topicJSON) []any {
	var schema, enc, first, last any
	if s := t.SchemaSettings; s != nil {
		schema, enc, first, last = optString(s.Schema), optString(s.Encoding), optString(s.FirstRevisionID), optString(s.LastRevisionID)
	}
	regions := []string{}
	var enforce any
	if p := t.MessageStoragePolicy; p != nil {
		regions = listOf(p.AllowedPersistenceRegions)
		enforce = optBool(p.EnforceInTransit)
	}
	var source, settings any
	for k, v := range t.IngestionDataSourceSettings {
		if k == "platformLogsSettings" {
			continue
		}
		source = k
		_ = v
	}
	if source != nil {
		b, _ := json.Marshal(t.IngestionDataSourceSettings)
		settings = string(b)
	}
	return []any{projectOf(t.Name), shortID(t.Name), t.Name, labelsOf(t.Labels), durationSeconds(t.MessageRetentionDuration),
		schema, enc, first, last, optString(t.KmsKeyName), regions, enforce, source, settings,
		rawJSON(t.MessageTransforms), optString(t.State), optBool(t.SatisfiesPzs)}
}

type topicsArgs struct {
	Topic      *string `vgi:"name=topic,doc=Only this topic (id or full name): one GET instead of a listing."`
	Project    *string `vgi:"name=project,doc=Google Cloud project id; default the gcp_pubsub secret's PROJECT_ID."`
	PageSize   int64   `vgi:"name=page_size,default=100,ge=1,le=1000,doc=Resources requested per API page (pageSize); pages are fetched lazily."`
	SecretName *string `vgi:"name=secret_name,doc=Name of the gcp_pubsub secret to use when several exist."`
}

func (a topicsArgs) secret() string  { return deref(a.SecretName) }
func (a topicsArgs) project() string { return deref(a.Project) }

var topicsExamples = []vgi.CatalogExample{
	{SQL: "SELECT topic, message_retention_seconds / 86400 AS retention_days, schema, labels['team'] AS team FROM pubsub.main.topics() ORDER BY topic",
		Description: "Every topic with its retention, schema and owning team label"},
	{SQL: "SELECT topic FROM pubsub.main.topics() WHERE kms_key_name IS NULL AND message_retention_seconds IS NULL ORDER BY topic",
		Description: "Topics with neither CMEK nor retention (cannot be replayed)"},
}

// notFound reports whether err is a 404.
func notFound(err error) bool {
	code, status := statusOf(err)
	return code == 404 || status == "NOT_FOUND"
}

func newTopics() vgi.TableFunction {
	return newList(&listFunc[topicsArgs]{
		name: "topics", columns: topicColumns,
		meta: meta("Topics", "resources", "topics, labels, retention, schema settings, kms, cmek, ingestion, import topics, message storage policy",
			"List the project's Pub/Sub topics (projects.topics.list). Named args: topic := (id or full name; one GET), project := "+
				"(default the secret's PROJECT_ID), page_size, secret_name. Columns: project, topic (id), name, labels `MAP`, "+
				"message_retention_seconds (`DOUBLE`, NULL = no retention), schema settings, kms_key_name, allowed_persistence_regions `LIST`, "+
				"ingestion_source/ingestion_settings (import topics), message_transforms, state. Join to subscriptions on topics.name = subscriptions.topic.",
			"## topics(topic := NULL, project := NULL)\n\nEvery topic in the project with labels, retention, schema settings, "+
				"CMEK key, storage policy and import-topic (ingestion) settings.",
			"List Pub/Sub topics with labels, retention, schema, KMS and ingestion settings", topicColumns, topicsExamples),
		fetch: func(ctx context.Context, c *conn, a topicsArgs, cursor string) (page, error) {
			if t := deref(a.Topic); t != "" {
				name, err := resourceName(c.Project, "topics", t)
				if err != nil {
					return page{}, err
				}
				var tj topicJSON
				if err := c.pubsub(ctx, "GET", namePath(name), nil, nil, &tj); err != nil {
					if notFound(err) {
						return page{}, nil
					}
					return page{}, err
				}
				return page{Rows: [][]any{topicRow(tj)}}, nil
			}
			var resp struct {
				Topics        []topicJSON `json:"topics"`
				NextPageToken string      `json:"nextPageToken"`
			}
			if err := c.pubsub(ctx, "GET", c.projectPath()+"/topics", pageParams(a.PageSize, cursor), nil, &resp); err != nil {
				return page{}, err
			}
			rows := make([][]any, len(resp.Topics))
			for i, t := range resp.Topics {
				rows[i] = topicRow(t)
			}
			return page{Rows: rows, Next: resp.NextPageToken}, nil
		},
	})
}

func pageParams(size int64, cursor string) url.Values {
	p := url.Values{"pageSize": {strconv.FormatInt(size, 10)}}
	if cursor != "" {
		p.Set("pageToken", cursor)
	}
	return p
}

// ---------------------------------------------------------------- subscriptions

type subscriptionJSON struct {
	Name       string `json:"name"`
	Topic      string `json:"topic"`
	PushConfig *struct {
		PushEndpoint string `json:"pushEndpoint"`
		OidcToken    *struct {
			ServiceAccountEmail string `json:"serviceAccountEmail"`
		} `json:"oidcToken"`
		NoWrapper *struct {
			WriteMetadata bool `json:"writeMetadata"`
		} `json:"noWrapper"`
	} `json:"pushConfig"`
	BigqueryConfig *struct {
		Table               string `json:"table"`
		State               string `json:"state"`
		ServiceAccountEmail string `json:"serviceAccountEmail"`
	} `json:"bigqueryConfig"`
	CloudStorageConfig *struct {
		Bucket              string `json:"bucket"`
		FilenamePrefix      string `json:"filenamePrefix"`
		State               string `json:"state"`
		ServiceAccountEmail string `json:"serviceAccountEmail"`
		AvroConfig          *struct {
			WriteMetadata bool `json:"writeMetadata"`
		} `json:"avroConfig"`
	} `json:"cloudStorageConfig"`
	AckDeadlineSeconds       *int64            `json:"ackDeadlineSeconds"`
	RetainAckedMessages      *bool             `json:"retainAckedMessages"`
	MessageRetentionDuration string            `json:"messageRetentionDuration"`
	Labels                   map[string]string `json:"labels"`
	EnableMessageOrdering    *bool             `json:"enableMessageOrdering"`
	ExpirationPolicy         *struct {
		TTL string `json:"ttl"`
	} `json:"expirationPolicy"`
	Filter           string `json:"filter"`
	DeadLetterPolicy *struct {
		DeadLetterTopic     string `json:"deadLetterTopic"`
		MaxDeliveryAttempts *int64 `json:"maxDeliveryAttempts"`
	} `json:"deadLetterPolicy"`
	RetryPolicy *struct {
		MinimumBackoff string `json:"minimumBackoff"`
		MaximumBackoff string `json:"maximumBackoff"`
	} `json:"retryPolicy"`
	Detached                      *bool  `json:"detached"`
	EnableExactlyOnceDelivery     *bool  `json:"enableExactlyOnceDelivery"`
	TopicMessageRetentionDuration string `json:"topicMessageRetentionDuration"`
	State                         string `json:"state"`
}

var subscriptionColumns = []arrow.Field{
	column("project", str, "Project that owns the subscription.", false),
	column("subscription", str, "Subscription id (the last part of the name).", false),
	column("name", str, "Full resource name, projects/<project>/subscriptions/<id>.", false),
	column("topic", str, "Full name of the topic it receives from; '_deleted-topic_' when the topic was deleted.", false),
	column("type", str, "Delivery type: 'pull', 'push', 'bigquery' or 'cloud_storage'.", false),
	column("ack_deadline_seconds", i64, "Seconds a subscriber has to acknowledge a message before redelivery.", true),
	column("message_retention_seconds", f64, "How long unacknowledged (and, with retain_acked_messages, acknowledged) messages are kept, in seconds.", true),
	column("retain_acked_messages", boolean, "Whether acknowledged messages are kept (needed to seek backwards in time).", false),
	column("enable_message_ordering", boolean, "Whether messages with the same ordering key are delivered in order.", false),
	column("enable_exactly_once_delivery", boolean, "Whether exactly-once delivery is enabled.", false),
	column("filter", str, "Subscription filter on message attributes; NULL = every message.", true),
	column("labels", strMap, "Resource labels (empty map when none).", false),
	column("expiration_ttl_seconds", f64, "Inactivity period after which the subscription is deleted, in seconds; NULL = never expires.", true),
	column("dead_letter_topic", str, "Topic undeliverable messages are forwarded to; NULL = no dead-letter policy.", true),
	column("max_delivery_attempts", i64, "Delivery attempts before a message is dead-lettered (5-100); NULL without a dead-letter policy.", true),
	column("retry_minimum_backoff_seconds", f64, "Minimum redelivery delay of the retry policy, in seconds; NULL = immediate redelivery.", true),
	column("retry_maximum_backoff_seconds", f64, "Maximum redelivery delay of the retry policy, in seconds.", true),
	column("detached", boolean, "Whether the subscription was detached from its topic (no longer receives messages).", false),
	column("push_endpoint_host", str, "Host of the push endpoint (push subscriptions). Only the host is returned: endpoint paths and query strings can contain tokens.", true),
	column("push_oidc_service_account", str, "Service account whose OIDC identity push requests carry; NULL when unauthenticated. No token or audience is returned.", true),
	column("push_unwrapped", boolean, "Whether push delivers the raw payload (no Pub/Sub wrapper); NULL for non-push subscriptions.", true),
	column("bigquery_table", str, "Destination table (project.dataset.table) of a BigQuery subscription.", true),
	column("cloud_storage_bucket", str, "Destination bucket of a Cloud Storage subscription.", true),
	column("export_state", str, "State of a BigQuery or Cloud Storage export, e.g. 'ACTIVE', 'PERMISSION_DENIED', 'NOT_FOUND'.", true),
	column("export_service_account", str, "Service account that writes BigQuery / Cloud Storage exports; NULL = the Pub/Sub service agent.", true),
	column("topic_message_retention_seconds", f64, "The topic's retention as seen by this subscription, in seconds.", true),
	column("state", str, "Subscription state: 'ACTIVE' or 'RESOURCE_ERROR'; NULL when the API omits it (emulator).", true),
}

func bv(b *bool) bool { return b != nil && *b }

func subscriptionRow(s subscriptionJSON) []any {
	typ := "pull"
	var host, oidc, unwrapped, bqTable, bucket, exportState, exportSA any
	switch {
	case s.BigqueryConfig != nil && s.BigqueryConfig.Table != "":
		typ = "bigquery"
		bqTable, exportState, exportSA = s.BigqueryConfig.Table, optString(s.BigqueryConfig.State), optString(s.BigqueryConfig.ServiceAccountEmail)
	case s.CloudStorageConfig != nil && s.CloudStorageConfig.Bucket != "":
		typ = "cloud_storage"
		bucket, exportState, exportSA = s.CloudStorageConfig.Bucket, optString(s.CloudStorageConfig.State), optString(s.CloudStorageConfig.ServiceAccountEmail)
	case s.PushConfig != nil && s.PushConfig.PushEndpoint != "":
		typ = "push"
		host = pushHost(s.PushConfig.PushEndpoint)
		if s.PushConfig.OidcToken != nil {
			oidc = optString(s.PushConfig.OidcToken.ServiceAccountEmail)
		}
		unwrapped = s.PushConfig.NoWrapper != nil
	}
	var ttl any
	if s.ExpirationPolicy != nil {
		ttl = durationSeconds(s.ExpirationPolicy.TTL) // {} = never expires -> NULL
	}
	var dlt, maxAttempts any
	if d := s.DeadLetterPolicy; d != nil && d.DeadLetterTopic != "" {
		dlt = d.DeadLetterTopic
		maxAttempts = int64(5) // the API default when unset
		if d.MaxDeliveryAttempts != nil {
			maxAttempts = *d.MaxDeliveryAttempts
		}
	}
	var minB, maxB any
	if r := s.RetryPolicy; r != nil {
		minB, maxB = durationSeconds(r.MinimumBackoff), durationSeconds(r.MaximumBackoff)
	}
	return []any{projectOf(s.Name), shortID(s.Name), s.Name, s.Topic, typ, optInt(s.AckDeadlineSeconds),
		durationSeconds(s.MessageRetentionDuration), bv(s.RetainAckedMessages), bv(s.EnableMessageOrdering),
		bv(s.EnableExactlyOnceDelivery), optString(s.Filter), labelsOf(s.Labels), ttl, dlt, maxAttempts, minB, maxB,
		bv(s.Detached), host, oidc, unwrapped, bqTable, bucket, exportState, exportSA,
		durationSeconds(s.TopicMessageRetentionDuration), optString(s.State)}
}

type subscriptionsArgs struct {
	Topic        *string `vgi:"name=topic,doc=Only subscriptions of this topic (id or full name). Pushed into the API: the topic's own subscription listing."`
	Subscription *string `vgi:"name=subscription,doc=Only this subscription (id or full name): one GET instead of a listing."`
	Project      *string `vgi:"name=project,doc=Google Cloud project id; default the gcp_pubsub secret's PROJECT_ID."`
	PageSize     int64   `vgi:"name=page_size,default=100,ge=1,le=1000,doc=Resources requested per API page (pageSize); pages are fetched lazily."`
	SecretName   *string `vgi:"name=secret_name,doc=Name of the gcp_pubsub secret to use when several exist."`
}

func (a subscriptionsArgs) secret() string  { return deref(a.SecretName) }
func (a subscriptionsArgs) project() string { return deref(a.Project) }

var subscriptionsExamples = []vgi.CatalogExample{
	{SQL: "SELECT subscription, type, ack_deadline_seconds, dead_letter_topic, enable_exactly_once_delivery FROM pubsub.main.subscriptions() ORDER BY subscription",
		Description: "Every subscription with its delivery type, ack deadline and dead-letter topic"},
	{SQL: "SELECT t.topic, count(s.name) AS subscriptions FROM pubsub.main.topics() t LEFT JOIN pubsub.main.subscriptions() s ON s.topic = t.name GROUP BY t.topic ORDER BY subscriptions, t.topic",
		Description: "Subscriptions per topic: topics with zero subscriptions drop every message"},
}

func (c *conn) getSubscription(ctx context.Context, name string) (*subscriptionJSON, error) {
	var sj subscriptionJSON
	if err := c.pubsub(ctx, "GET", namePath(name), nil, nil, &sj); err != nil {
		return nil, err
	}
	return &sj, nil
}

func newSubscriptions() vgi.TableFunction {
	return newList(&listFunc[subscriptionsArgs]{
		name: "subscriptions", columns: subscriptionColumns,
		meta: meta("Subscriptions", "resources", "subscriptions, pull, push, bigquery subscription, cloud storage subscription, ack deadline, dead letter, retry policy, exactly once, filter, ordering",
			"List Pub/Sub subscriptions (projects.subscriptions.list). Named args: topic := (only that topic's subscriptions, via "+
				"projects.topics.subscriptions.list), subscription := (one GET), project :=, page_size, secret_name. Columns: subscription, name, topic "+
				"(full name), type ('pull','push','bigquery','cloud_storage'), ack_deadline_seconds, message_retention_seconds, retain_acked_messages, "+
				"enable_message_ordering, enable_exactly_once_delivery, filter, labels `MAP`, expiration_ttl_seconds (NULL = never), dead_letter_topic, "+
				"max_delivery_attempts, retry backoff seconds, detached, push_endpoint_host (host only, never tokens), push_oidc_service_account, "+
				"bigquery_table, cloud_storage_bucket, export_state, state. For backlog / lag use subscription_backlog().",
			"## subscriptions(topic := NULL, subscription := NULL, project := NULL)\n\nEvery subscription with its delivery type and "+
				"delivery settings: ack deadline, retention, filter, dead-letter and retry policy, exactly-once, ordering. Push endpoints "+
				"are reduced to their host.",
			"List Pub/Sub subscriptions with delivery type, ack deadline, dead-letter, retry and push settings", subscriptionColumns, subscriptionsExamples),
		fetch: func(ctx context.Context, c *conn, a subscriptionsArgs, cursor string) (page, error) {
			if s := deref(a.Subscription); s != "" {
				name, err := resourceName(c.Project, "subscriptions", s)
				if err != nil {
					return page{}, err
				}
				sj, err := c.getSubscription(ctx, name)
				if err != nil {
					if notFound(err) {
						return page{}, nil
					}
					return page{}, err
				}
				return page{Rows: [][]any{subscriptionRow(*sj)}}, nil
			}
			if t := deref(a.Topic); t != "" {
				topic, err := resourceName(c.Project, "topics", t)
				if err != nil {
					return page{}, err
				}
				var resp struct {
					Subscriptions []string `json:"subscriptions"`
					NextPageToken string   `json:"nextPageToken"`
				}
				if err := c.pubsub(ctx, "GET", namePath(topic)+"/subscriptions", pageParams(a.PageSize, cursor), nil, &resp); err != nil {
					if notFound(err) {
						return page{}, nil
					}
					return page{}, err
				}
				var rows [][]any
				for _, n := range resp.Subscriptions {
					sj, err := c.getSubscription(ctx, n)
					if err != nil {
						if notFound(err) { // deleted between the listing and the GET
							continue
						}
						return page{}, fmt.Errorf("subscription %s: %w", n, err)
					}
					rows = append(rows, subscriptionRow(*sj))
				}
				return page{Rows: rows, Next: resp.NextPageToken}, nil
			}
			var resp struct {
				Subscriptions []subscriptionJSON `json:"subscriptions"`
				NextPageToken string             `json:"nextPageToken"`
			}
			if err := c.pubsub(ctx, "GET", c.projectPath()+"/subscriptions", pageParams(a.PageSize, cursor), nil, &resp); err != nil {
				return page{}, err
			}
			rows := make([][]any, len(resp.Subscriptions))
			for i, s := range resp.Subscriptions {
				rows[i] = subscriptionRow(s)
			}
			return page{Rows: rows, Next: resp.NextPageToken}, nil
		},
	})
}

// ---------------------------------------------------------------- snapshots

type snapshotJSON struct {
	Name       string            `json:"name"`
	Topic      string            `json:"topic"`
	ExpireTime string            `json:"expireTime"`
	Labels     map[string]string `json:"labels"`
}

var snapshotColumns = []arrow.Field{
	column("project", str, "Project that owns the snapshot.", false),
	column("snapshot", str, "Snapshot id.", false),
	column("name", str, "Full resource name, projects/<project>/snapshots/<id>.", false),
	column("topic", str, "Full name of the topic whose messages the snapshot retains.", false),
	column("expire_time", tstz, "When the snapshot expires (at most 7 days after the oldest unacked message it captured).", true),
	column("labels", strMap, "Resource labels (empty map when none).", false),
}

type snapshotsArgs struct {
	Topic      *string `vgi:"name=topic,doc=Only snapshots of this topic (id or full name), via projects.topics.snapshots.list."`
	Project    *string `vgi:"name=project,doc=Google Cloud project id; default the gcp_pubsub secret's PROJECT_ID."`
	PageSize   int64   `vgi:"name=page_size,default=100,ge=1,le=1000,doc=Resources requested per API page (pageSize); pages are fetched lazily."`
	SecretName *string `vgi:"name=secret_name,doc=Name of the gcp_pubsub secret to use when several exist."`
}

func (a snapshotsArgs) secret() string  { return deref(a.SecretName) }
func (a snapshotsArgs) project() string { return deref(a.Project) }

var snapshotsExamples = []vgi.CatalogExample{
	{SQL: "SELECT snapshot, topic, expire_time FROM pubsub.main.snapshots() ORDER BY expire_time",
		Description: "Snapshots by expiry (the soonest to disappear first)"},
}

func snapshotRow(s snapshotJSON) []any {
	return []any{projectOf(s.Name), shortID(s.Name), s.Name, s.Topic, optTime(s.ExpireTime), labelsOf(s.Labels)}
}

func newSnapshots() vgi.TableFunction {
	return newList(&listFunc[snapshotsArgs]{
		name: "snapshots", columns: snapshotColumns,
		meta: meta("Snapshots", "resources", "snapshots, seek, replay, expire time",
			"List Pub/Sub snapshots (projects.snapshots.list). Named args: topic := (only that topic's snapshots), project :=, page_size, "+
				"secret_name. Columns: snapshot, name, topic, expire_time `TIMESTAMPTZ`, labels `MAP`. A snapshot can be passed to "+
				"read_messages(snapshot := ...) to read the backlog it captured.",
			"## snapshots(topic := NULL, project := NULL)\n\nEvery snapshot with its topic and expiry.",
			"List Pub/Sub snapshots with topic and expiry", snapshotColumns, snapshotsExamples),
		fetch: func(ctx context.Context, c *conn, a snapshotsArgs, cursor string) (page, error) {
			if t := deref(a.Topic); t != "" {
				topic, err := resourceName(c.Project, "topics", t)
				if err != nil {
					return page{}, err
				}
				var resp struct {
					Snapshots     []string `json:"snapshots"`
					NextPageToken string   `json:"nextPageToken"`
				}
				if err := c.pubsub(ctx, "GET", namePath(topic)+"/snapshots", pageParams(a.PageSize, cursor), nil, &resp); err != nil {
					if notFound(err) {
						return page{}, nil
					}
					return page{}, err
				}
				var rows [][]any
				for _, n := range resp.Snapshots {
					var sj snapshotJSON
					if err := c.pubsub(ctx, "GET", namePath(n), nil, nil, &sj); err != nil {
						if notFound(err) {
							continue
						}
						return page{}, fmt.Errorf("snapshot %s: %w", n, err)
					}
					rows = append(rows, snapshotRow(sj))
				}
				return page{Rows: rows, Next: resp.NextPageToken}, nil
			}
			var resp struct {
				Snapshots     []snapshotJSON `json:"snapshots"`
				NextPageToken string         `json:"nextPageToken"`
			}
			if err := c.pubsub(ctx, "GET", c.projectPath()+"/snapshots", pageParams(a.PageSize, cursor), nil, &resp); err != nil {
				return page{}, err
			}
			rows := make([][]any, len(resp.Snapshots))
			for i, s := range resp.Snapshots {
				rows[i] = snapshotRow(s)
			}
			return page{Rows: rows, Next: resp.NextPageToken}, nil
		},
	})
}

// ---------------------------------------------------------------- schemas

type schemaJSON struct {
	Name               string `json:"name"`
	Type               string `json:"type"`
	Definition         string `json:"definition"`
	RevisionID         string `json:"revisionId"`
	RevisionCreateTime string `json:"revisionCreateTime"`
}

var schemaColumns = []arrow.Field{
	column("project", str, "Project that owns the schema.", false),
	column("schema", str, "Schema id.", false),
	column("name", str, "Full resource name, projects/<project>/schemas/<id> (topics reference this in their schema column).", false),
	column("type", str, "'AVRO' or 'PROTOCOL_BUFFER'.", true),
	column("revision_id", str, "Revision id (8 hex characters).", true),
	column("revision_create_time", tstz, "When this revision was committed.", true),
	column("definition", str, "The schema definition text: Avro JSON or a .proto source.", true),
}

func schemaRow(s schemaJSON) []any {
	return []any{projectOf(s.Name), shortID(s.Name), s.Name, optString(s.Type), optString(s.RevisionID),
		optTime(s.RevisionCreateTime), optString(s.Definition)}
}

type schemasArgs struct {
	Project    *string `vgi:"name=project,doc=Google Cloud project id; default the gcp_pubsub secret's PROJECT_ID."`
	PageSize   int64   `vgi:"name=page_size,default=100,ge=1,le=1000,doc=Resources requested per API page (pageSize); pages are fetched lazily."`
	SecretName *string `vgi:"name=secret_name,doc=Name of the gcp_pubsub secret to use when several exist."`
}

func (a schemasArgs) secret() string  { return deref(a.SecretName) }
func (a schemasArgs) project() string { return deref(a.Project) }

var schemasExamples = []vgi.CatalogExample{
	{SQL: "SELECT schema, type, revision_id, length(definition) AS definition_chars FROM pubsub.main.schemas() ORDER BY schema",
		Description: "Schemas with their type and current revision"},
	{SQL: "SELECT s.schema, count(t.name) AS topics FROM pubsub.main.schemas() s LEFT JOIN pubsub.main.topics() t ON t.schema = s.name GROUP BY s.schema ORDER BY topics DESC, s.schema",
		Description: "How many topics use each schema"},
}

func newSchemas() vgi.TableFunction {
	return newList(&listFunc[schemasArgs]{
		name: "schemas", columns: schemaColumns,
		meta: meta("Schemas", "schemas", "schemas, avro, protocol buffers, protobuf, schema registry, definition",
			"List the project's Pub/Sub schemas with their latest revision and full definition (projects.schemas.list?view=FULL). "+
				"Named args: project :=, page_size, secret_name. Columns: schema, name, type ('AVRO' or 'PROTOCOL_BUFFER'), revision_id, "+
				"revision_create_time, definition (Avro JSON / .proto text; parse Avro with definition::JSON). For history use schema_revisions(schema).",
			"## schemas(project := NULL)\n\nEvery schema with its type, latest revision and definition text.",
			"List Pub/Sub schemas with type, latest revision and definition", schemaColumns, schemasExamples),
		fetch: func(ctx context.Context, c *conn, a schemasArgs, cursor string) (page, error) {
			var resp struct {
				Schemas       []schemaJSON `json:"schemas"`
				NextPageToken string       `json:"nextPageToken"`
			}
			p := pageParams(a.PageSize, cursor)
			p.Set("view", "FULL")
			if err := c.pubsub(ctx, "GET", c.projectPath()+"/schemas", p, nil, &resp); err != nil {
				return page{}, err
			}
			rows := make([][]any, len(resp.Schemas))
			for i, s := range resp.Schemas {
				rows[i] = schemaRow(s)
			}
			return page{Rows: rows, Next: resp.NextPageToken}, nil
		},
	})
}

type schemaRevisionsArgs struct {
	Schema     string  `vgi:"pos=0,doc=Schema id or full name (projects/<project>/schemas/<id>)."`
	Project    *string `vgi:"name=project,doc=Google Cloud project id; default the gcp_pubsub secret's PROJECT_ID."`
	PageSize   int64   `vgi:"name=page_size,default=100,ge=1,le=1000,doc=Revisions requested per API page (pageSize); pages are fetched lazily."`
	SecretName *string `vgi:"name=secret_name,doc=Name of the gcp_pubsub secret to use when several exist."`
}

func (a schemaRevisionsArgs) secret() string  { return deref(a.SecretName) }
func (a schemaRevisionsArgs) project() string { return deref(a.Project) }

var schemaRevisionsExamples = []vgi.CatalogExample{
	{SQL: "SELECT revision_id, revision_create_time, definition FROM pubsub.main.schema_revisions('order-avro') ORDER BY revision_create_time DESC",
		Description: "The revision history of a schema, newest first"},
}

func newSchemaRevisions() vgi.TableFunction {
	return newList(&listFunc[schemaRevisionsArgs]{
		name: "schema_revisions", columns: schemaColumns,
		meta: meta("Schema Revisions", "schemas", "schema revisions, schema history, schema evolution, avro, protobuf",
			"Every revision of one schema (projects.schemas.listRevisions?view=FULL), newest first as the API returns them. Positional arg: "+
				"schema id or full name. Named: project :=, page_size, secret_name. Same columns as schemas(): one row per revision. "+
				"Compare with topics.schema_first_revision_id / schema_last_revision_id to see which revisions a topic accepts.",
			"## schema_revisions(schema, project := NULL)\n\nThe revision history of one schema, with each definition.",
			"List every revision of a Pub/Sub schema with its definition", schemaColumns, schemaRevisionsExamples),
		fetch: func(ctx context.Context, c *conn, a schemaRevisionsArgs, cursor string) (page, error) {
			name, err := resourceName(c.Project, "schemas", a.Schema)
			if err != nil {
				return page{}, err
			}
			var resp struct {
				Schemas       []schemaJSON `json:"schemas"`
				NextPageToken string       `json:"nextPageToken"`
			}
			p := pageParams(a.PageSize, cursor)
			p.Set("view", "FULL")
			if err := c.pubsub(ctx, "GET", namePath(name)+":listRevisions", p, nil, &resp); err != nil {
				if notFound(err) {
					return page{}, nil
				}
				return page{}, err
			}
			rows := make([][]any, len(resp.Schemas))
			for i, s := range resp.Schemas {
				rows[i] = schemaRow(s)
			}
			return page{Rows: rows, Next: resp.NextPageToken}, nil
		},
	})
}
