// read_messages: an opt-in, side-effect-contained read of a topic's retained
// messages.
//
// Pub/Sub has no non-destructive peek. Pulling from an existing subscription
// leases its messages: they are hidden from the real subscribers until the
// ack deadline passes, they count as a delivery attempt (pushing messages
// toward a dead-letter topic), and acking them would lose them for good. So
// this worker NEVER pulls from, seeks, or modifies an existing subscription.
//
// Instead, with allow_temporary_subscription := true, it:
//
//  1. creates a NEW subscription on the topic, uniquely named
//     vgi-temp-<time>-<random>, labelled vgi-temp=true, with a 24 h
//     expiration_policy (the minimum Pub/Sub allows) and 10 min message
//     retention, so a leak cleans itself up;
//  2. seeks it to start_time (or to a snapshot) — this replays only messages
//     the TOPIC retains (topic message retention) or the snapshot captured;
//  3. pulls up to max_messages, acknowledging them on the temporary
//     subscription only (each subscription has its own copy of every message,
//     so nothing changes for the real subscribers);
//  4. deletes the temporary subscription, also when any step fails.
//
// The whole sequence runs inside one Process call, so the temporary
// subscription never outlives the call, even over the HTTP transport.
//
// Side effects that remain, and why the read is opt-in: a subscription is
// created and deleted (visible in audit logs, needs
// pubsub.subscriptions.create / delete and pubsub.topics.attachSubscription),
// and the replayed messages are billed as message delivery.
//
// Columns follow the shared messaging vocabulary of the VGI messaging workers
// (vgi-rabbitmq, vgi-nats): sequence, timestamp, key, payload BLOB,
// payload_text, payload_bytes, headers MAP; Pub/Sub's ordering key is `key`,
// its attributes are `headers`, its publish time is `timestamp`.

package pubsub

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

const (
	// TempPrefix starts every temporary subscription id.
	TempPrefix = "vgi-temp-"
	// tempLabel marks temporary subscriptions so leaks are easy to find.
	tempLabel = "vgi-temp"
	// tempTTL is the expiration_policy of a temporary subscription (Pub/Sub's minimum).
	tempTTL = 24 * time.Hour
)

var messageColumns = []arrow.Field{
	column("project", str, "Project of the topic.", false),
	column("topic", str, "Topic id the message was published to.", false),
	column("subscription", str, "The temporary subscription (vgi-temp-...) the message was read through; it is deleted when the read ends.", false),
	column("sequence", i64, "Position of the message in its source. Always NULL: Pub/Sub has no offsets (kept for the shared messaging columns; use timestamp and message_id).", true),
	column("timestamp", tstz, "Publish time assigned by Pub/Sub (publishTime).", false),
	column("key", str, "Ordering key the message was published with; NULL when none.", true),
	column("payload", blob, "Message data bytes (truncated to the truncate argument).", true),
	column("payload_text", str, "The data as text when it is valid UTF-8, else NULL. Parse JSON payloads with payload_text::JSON.", true),
	column("payload_bytes", i64, "Full data size in bytes (before any truncation).", false),
	column("headers", strMap, "Message attributes (empty map when none).", false),
	column("message_id", str, "Server-assigned message id, unique within the topic.", false),
	column("delivery_attempt", i64, "Delivery attempt number; NULL (only set on subscriptions with a dead-letter policy).", true),
}

type messagesArgs struct {
	Topic       string     `vgi:"pos=0,doc=Topic to read (id or full name). Messages are read through a new temporary subscription; existing subscriptions are never touched."`
	AllowTemp   bool       `vgi:"name=allow_temporary_subscription,default=false,doc=Required opt-in. true acknowledges that the read CREATES a temporary subscription on the topic (vgi-temp-..., label vgi-temp=true, 24 h expiration), seeks it, pulls and acks on it only, and DELETES it at the end. Never pulls from an existing subscription."`
	StartTime   *time.Time `vgi:"name=start_time,doc=Replay messages published at or after this time (seek). Default: the oldest the topic retains (now minus its message retention). Needs topic message retention."`
	EndTime     *time.Time `vgi:"name=end_time,doc=Drop messages published after this time."`
	Snapshot    *string    `vgi:"name=snapshot,doc=Instead of start_time, seek the temporary subscription to this snapshot (id or full name) and read the backlog it captured."`
	Filter      *string    `vgi:"name=filter,doc=Pub/Sub filter set on the temporary subscription (server-side), e.g. 'attributes.tenant = \"a\"'. Only attributes can be filtered."`
	MaxMessages int64      `vgi:"name=max_messages,default=100,ge=1,le=10000,doc=Stop after this many messages."`
	WaitSeconds int64      `vgi:"name=wait_seconds,default=10,ge=1,le=300,doc=Upper bound on the time spent pulling; the read also stops once the subscription is drained."`
	Truncate    int64      `vgi:"name=truncate,default=65536,ge=1,le=10485760,doc=Return at most this many bytes of each message (payload_bytes still gives the full size)."`
	Project     *string    `vgi:"name=project,doc=Google Cloud project id; default the gcp_pubsub secret's PROJECT_ID."`
	SecretName  *string    `vgi:"name=secret_name,doc=Name of the gcp_pubsub secret to use when several exist."`
}

func (a messagesArgs) secret() string  { return deref(a.SecretName) }
func (a messagesArgs) project() string { return deref(a.Project) }

var messagesExamples = []vgi.CatalogExample{
	{SQL: "SELECT timestamp, key, headers['tenant'] AS tenant, payload_text::JSON->>'order_id' AS order_id FROM pubsub.main.read_messages('orders', allow_temporary_subscription := true, start_time := TIMESTAMPTZ '2026-01-01 00:00:00+00', max_messages := 50) ORDER BY timestamp, message_id",
		Description: "Replay retained messages of a topic through a temporary subscription (opt-in)"},
	{SQL: "SELECT headers['event'] AS event, count(*) AS messages, max(payload_bytes) AS largest FROM pubsub.main.read_messages('orders', allow_temporary_subscription := true, snapshot := 'before-deploy', max_messages := 1000) GROUP BY event ORDER BY messages DESC",
		Description: "What a snapshot's captured backlog contains, without touching the subscription it was taken from"},
}

const messagesWarning = "Pub/Sub has no read-only peek: pulling from an existing subscription would hide its messages from the real " +
	"subscribers until the ack deadline, count delivery attempts toward dead-lettering, and acking would drop them. read_messages " +
	"therefore never pulls from an existing subscription. With allow_temporary_subscription := true it creates a temporary " +
	"subscription (vgi-temp-<time>-<random>, label vgi-temp=true, 24 h expiration), seeks it to start_time or a snapshot, pulls and " +
	"acks on it only, and deletes it at the end (also on error). That needs pubsub.subscriptions.create/delete and " +
	"pubsub.topics.attachSubscription, is visible in audit logs, and only replays what the topic retains (topic message retention) " +
	"or the snapshot captured"

func checkOptIn(a messagesArgs) error {
	if !a.AllowTemp {
		return errors.New("read_messages needs allow_temporary_subscription := true: " + messagesWarning)
	}
	if a.StartTime != nil && deref(a.Snapshot) != "" {
		return errors.New("give start_time := or snapshot :=, not both")
	}
	if a.StartTime != nil && a.EndTime != nil && !a.StartTime.Before(*a.EndTime) {
		return errors.New("start_time must be before end_time")
	}
	return nil
}

// MessagesState carries the bound arguments and whether the single read ran.
// The temporary subscription lives only inside one Process call.
type MessagesState struct {
	ArgsJSON string
	Done     bool
}

// tempName returns a unique temporary subscription id.
func tempName(now time.Time) string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return TempPrefix + now.UTC().Format("20060102t150405") + "-" + hex.EncodeToString(b)
}

type receivedJSON struct {
	ReceivedMessages []struct {
		AckID   string `json:"ackId"`
		Message struct {
			Data        string            `json:"data"`
			Attributes  map[string]string `json:"attributes"`
			MessageID   string            `json:"messageId"`
			PublishTime string            `json:"publishTime"`
			OrderingKey string            `json:"orderingKey"`
		} `json:"message"`
		DeliveryAttempt *int64 `json:"deliveryAttempt"`
	} `json:"receivedMessages"`
}

type message struct {
	ID       string
	Publish  time.Time
	Key      string
	Data     []byte
	Attrs    map[string]string
	Attempts *int64
}

// readTopic runs the temporary-subscription read.
func (c *conn) readTopic(ctx context.Context, a messagesArgs) (sub string, msgs []message, err error) {
	topic, err := resourceName(c.Project, "topics", a.Topic)
	if err != nil {
		return "", nil, err
	}
	var seek map[string]any
	switch {
	case deref(a.Snapshot) != "":
		snap, err := resourceName(projectOf(topic), "snapshots", deref(a.Snapshot))
		if err != nil {
			return "", nil, err
		}
		seek = map[string]any{"snapshot": snap}
	case a.StartTime != nil:
		seek = map[string]any{"time": formatTime(*a.StartTime)}
	default:
		var t topicJSON
		if err := c.pubsub(ctx, "GET", namePath(topic), nil, nil, &t); err != nil {
			return "", nil, err
		}
		ret, _ := durationSeconds(t.MessageRetentionDuration).(float64)
		if ret <= 0 {
			return "", nil, fmt.Errorf("topic %s has no message retention, so a new subscription cannot see messages published before it; "+
				"enable topic retention (--message-retention-duration), or pass snapshot := to read a snapshot", topic)
		}
		seek = map[string]any{"time": formatTime(time.Now().Add(-time.Duration(ret * float64(time.Second))))}
	}

	sub = "projects/" + c.Project + "/subscriptions/" + tempName(time.Now())
	body := map[string]any{
		"topic":                    topic,
		"ackDeadlineSeconds":       60,
		"messageRetentionDuration": "600s",
		"expirationPolicy":         map[string]any{"ttl": formatDuration(int64(tempTTL / time.Second))},
		"labels":                   map[string]string{tempLabel: "true", "created-by": "vgi-pubsub"},
	}
	if f := deref(a.Filter); f != "" {
		body["filter"] = f
	}
	// Never retried: a repeat could collide with the subscription the first
	// attempt created, and an ALREADY_EXISTS must never lead to a pull.
	if err := c.call(ctx, "PUT", c.Endpoint, namePath(sub), nil, body, nil, false); err != nil {
		return "", nil, fmt.Errorf("creating the temporary subscription: %w", err)
	}
	defer func() {
		dctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if derr := c.pubsub(dctx, "DELETE", namePath(sub), nil, nil, nil); derr != nil && !notFound(derr) {
			slog.Warn("pubsub: could not delete the temporary subscription; it expires on its own after 24h", "subscription", sub, "error", derr)
			if err == nil {
				err = fmt.Errorf("read %d messages but could not delete the temporary subscription %s (it expires after 24h; delete it with "+
					"gcloud pubsub subscriptions delete): %w", len(msgs), sub, derr)
			}
		}
	}()
	if err := c.pubsub(ctx, "POST", namePath(sub)+":seek", nil, seek, nil); err != nil {
		return sub, nil, fmt.Errorf("seeking the temporary subscription: %w", err)
	}
	msgs, err = c.pullAll(ctx, sub, a)
	return sub, msgs, err
}

// pullAll pulls until max_messages, the subscription is drained (repeated
// empty immediate pulls after the first wait), or wait_seconds pass.
func (c *conn) pullAll(ctx context.Context, sub string, a messagesArgs) ([]message, error) {
	deadline := time.Now().Add(time.Duration(a.WaitSeconds) * time.Second)
	seen := map[string]bool{}
	var out []message
	empty := 0
	for first := true; int64(len(out)) < a.MaxMessages && time.Now().Before(deadline); first = false {
		want := min(a.MaxMessages-int64(len(out)), 1000)
		pctx, cancel := context.WithDeadline(ctx, deadline)
		var resp receivedJSON
		// The first pull waits for messages; later pulls return immediately
		// so a drained subscription is noticed without waiting.
		err := c.pubsub(pctx, "POST", namePath(sub)+":pull", nil, map[string]any{"maxMessages": want, "returnImmediately": !first}, &resp)
		cancel()
		if err != nil {
			if errors.Is(pctx.Err(), context.DeadlineExceeded) && ctx.Err() == nil {
				break // waited the whole budget for messages
			}
			return out, fmt.Errorf("pulling from the temporary subscription: %w", err)
		}
		ackIDs := make([]string, 0, len(resp.ReceivedMessages))
		for _, rm := range resp.ReceivedMessages {
			ackIDs = append(ackIDs, rm.AckID)
			m := rm.Message
			if seen[m.MessageID] {
				continue // redelivery
			}
			seen[m.MessageID] = true
			data, err := base64.StdEncoding.DecodeString(m.Data)
			if err != nil {
				return out, fmt.Errorf("message %s: data is not base64: %w", m.MessageID, err)
			}
			pt, _ := optTime(m.PublishTime).(time.Time)
			if a.EndTime != nil && pt.After(*a.EndTime) {
				continue
			}
			if int64(len(out)) < a.MaxMessages {
				out = append(out, message{ID: m.MessageID, Publish: pt, Key: m.OrderingKey, Data: data, Attrs: m.Attributes, Attempts: rm.DeliveryAttempt})
			}
		}
		if len(ackIDs) > 0 {
			// Acks go to the temporary subscription only.
			if err := c.pubsub(ctx, "POST", namePath(sub)+":acknowledge", nil, map[string]any{"ackIds": ackIDs}, nil); err != nil {
				return out, fmt.Errorf("acknowledging on the temporary subscription: %w", err)
			}
		}
		if len(resp.ReceivedMessages) == 0 {
			if first {
				break // nothing arrived within the wait
			}
			empty++
			if empty >= 3 {
				break // drained
			}
			sleep(ctx, 200*time.Millisecond)
			continue
		}
		empty = 0
	}
	sort.SliceStable(out, func(i, j int) bool {
		if !out[i].Publish.Equal(out[j].Publish) {
			return out[i].Publish.Before(out[j].Publish)
		}
		return lessID(out[i].ID, out[j].ID)
	})
	return out, nil
}

// lessID orders numeric message ids numerically, others lexically.
func lessID(a, b string) bool {
	if len(a) != len(b) && strings.Trim(a+b, "0123456789") == "" {
		return len(a) < len(b)
	}
	return a < b
}

type messagesFunc struct{}

func (*messagesFunc) Name() string { return "read_messages" }

func (*messagesFunc) Metadata() vgi.FunctionMetadata {
	return meta("Read Retained Messages", "messages", "messages, replay, peek, payload, attributes, ordering key, publish time, seek, snapshot, retention, temporary subscription",
		"Read a topic's retained messages through a TEMPORARY subscription — only with allow_temporary_subscription := true (required "+
			"opt-in). Positional arg: topic. Named: allow_temporary_subscription, start_time (seek; default the oldest retained message), "+
			"end_time, snapshot := (seek to a snapshot instead), filter := (Pub/Sub attribute filter, server-side), max_messages (default "+
			"100), wait_seconds (default 10), truncate, project, secret_name. "+messagesWarning+". Columns (shared messaging vocabulary): "+
			"project, topic, subscription (the temporary one), sequence (always NULL), timestamp (publish time), key (ordering key), "+
			"payload `BLOB`, payload_text, payload_bytes, headers `MAP` (attributes), message_id, delivery_attempt.",
		"## read_messages(topic, allow_temporary_subscription := true, start_time := NULL, snapshot := NULL)\n\n"+
			"Replays a topic's **retained** messages through a temporary subscription that the worker creates, seeks, drains and deletes. "+
			"Existing subscriptions are never pulled from. Opt-in only, because it creates (and removes) a subscription.",
		"Replay a topic's retained messages through a temporary subscription (opt-in; never touches existing subscriptions)",
		messageColumns, messagesExamples)()
}

func (*messagesFunc) ArgumentSpecs() []vgi.ArgSpec { return vgi.DeriveArgSpecs(messagesArgs{}) }

func (*messagesFunc) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	var a messagesArgs
	if err := vgi.BindArgs(params.Args, &a); err != nil {
		return nil, err
	}
	if err := checkOptIn(a); err != nil {
		return nil, err
	}
	return bindWithSecret[messagesArgs](params, arrow.NewSchema(messageColumns, nil))
}

func (*messagesFunc) NewState(params *vgi.ProcessParams) (*MessagesState, error) {
	var a messagesArgs
	if err := vgi.BindArgs(params.Args, &a); err != nil {
		return nil, err
	}
	if err := checkOptIn(a); err != nil {
		return nil, err
	}
	return &MessagesState{ArgsJSON: mustJSON(a)}, nil
}

func (*messagesFunc) Process(ctx context.Context, params *vgi.ProcessParams, st *MessagesState, out *vgirpc.OutputCollector) error {
	if st.Done {
		return out.Finish()
	}
	st.Done = true
	var a messagesArgs
	if err := json.Unmarshal([]byte(st.ArgsJSON), &a); err != nil {
		return err
	}
	if err := checkOptIn(a); err != nil {
		return err
	}
	c, err := resolveConn(ctx, params.Secrets, params.AttachOptions(), a.secret(), a.project())
	if err != nil {
		return err
	}
	sub, msgs, err := c.readTopic(ctx, a)
	if err != nil {
		return err
	}
	topic, _ := resourceName(c.Project, "topics", a.Topic)
	rows := make([][]any, len(msgs))
	for i, m := range msgs {
		data := m.Data
		if int64(len(data)) > a.Truncate {
			data = data[:a.Truncate]
		}
		var text any
		if utf8.Valid(data) {
			text = string(data)
		}
		attrs := m.Attrs
		if attrs == nil {
			attrs = map[string]string{}
		}
		rows[i] = []any{projectOf(topic), shortID(topic), shortID(sub), nil, m.Publish, optString(m.Key), data, text,
			int64(len(m.Data)), attrs, m.ID, optInt(m.Attempts)}
	}
	if len(rows) == 0 {
		return out.Finish()
	}
	return emitRows(out, messageColumns, rows)
}

func newReadMessages() vgi.TableFunction { return vgi.AsTableFunction[MessagesState](&messagesFunc{}) }
