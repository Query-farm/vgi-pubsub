package pubsub

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
)

func init() { sleep = func(context.Context, time.Duration) {} }

func TestResourceName(t *testing.T) {
	cases := []struct{ in, want, err string }{
		{"orders", "projects/p/topics/orders", ""},
		{"projects/other/topics/shared", "projects/other/topics/shared", ""},
		{"projects/p/subscriptions/x", "", "not a topic name"},
		{"ab", "", "not a valid topic id"},
		{"9lives", "", "not a valid topic id"},
	}
	for _, c := range cases {
		got, err := resourceName("p", "topics", c.in)
		if c.err != "" {
			if err == nil || !strings.Contains(err.Error(), c.err) {
				t.Errorf("%q: want error %q, got %v", c.in, c.err, err)
			}
			continue
		}
		if err != nil || got != c.want {
			t.Errorf("%q: got %q, %v", c.in, got, err)
		}
	}
	if namePath("projects/a:b/topics/t") != "/v1/projects/a:b/topics/t" {
		t.Errorf("namePath: %s", namePath("projects/a:b/topics/t"))
	}
}

func TestValues(t *testing.T) {
	if v := durationSeconds("604800s"); v != 604800.0 {
		t.Errorf("duration: %v", v)
	}
	if v := durationSeconds("1.5s"); v != 1.5 {
		t.Errorf("duration: %v", v)
	}
	if durationSeconds("") != nil {
		t.Error("empty duration should be NULL")
	}
	if h := pushHost("https://hooks.example.com:8443/path?token=SECRET"); h != "hooks.example.com:8443" {
		t.Errorf("pushHost: %v", h)
	}
	if memberType("serviceAccount:a@b") != "serviceAccount" || memberType("allUsers") != "allUsers" {
		t.Error("memberType")
	}
	if !lessID("2", "10") || lessID("10", "2") || !lessID("a", "b") {
		t.Error("lessID")
	}
}

func TestMetricsHelpers(t *testing.T) {
	m, err := metricType("subscription/num_undelivered_messages")
	if err != nil || m != "pubsub.googleapis.com/subscription/num_undelivered_messages" {
		t.Errorf("metricType: %q %v", m, err)
	}
	if m, _ := metricType("compute.googleapis.com/instance/cpu/utilization"); m != "compute.googleapis.com/instance/cpu/utilization" {
		t.Errorf("full type changed: %q", m)
	}
	if _, err := metricType(`x" OR "`); err == nil {
		t.Error("quotes must be rejected")
	}
	if defaultAligner("pubsub.googleapis.com/topic/send_request_count") != "ALIGN_SUM" ||
		defaultAligner("pubsub.googleapis.com/subscription/oldest_unacked_message_age") != "ALIGN_MEAN" {
		t.Error("defaultAligner")
	}
	f := monitoringFilter("m", "projects/p/subscriptions/s", "", `metric.labels.x = "y"`)
	want := `metric.type = "m" AND resource.labels.subscription_id = "s" AND (metric.labels.x = "y")`
	if f != want {
		t.Errorf("filter:\n got %s\nwant %s", f, want)
	}
	var p pointJSON
	_ = json.Unmarshal([]byte(`{"value":{"int64Value":"42"}}`), &p)
	if v, ok := pointValue(p); !ok || v != 42 {
		t.Errorf("int64 point: %v", v)
	}
	var d pointJSON
	_ = json.Unmarshal([]byte(`{"value":{"distributionValue":{"count":"2","mean":3.5}}}`), &d)
	if v, ok := pointValue(d); !ok || v != 3.5 {
		t.Errorf("distribution point: %v", v)
	}
	a := timeSeriesArgs{Metric: "subscription/num_undelivered_messages", AlignmentSeconds: 0, Aligner: "auto", PageSize: 10,
		Reducer: ptr("REDUCE_SUM")}
	if _, err := a.params("p"); err == nil {
		t.Error("reducer without alignment must fail")
	}
}

func ptr(s string) *string { return &s }

func TestRows(t *testing.T) {
	var s subscriptionJSON
	_ = json.Unmarshal([]byte(`{"name":"projects/p/subscriptions/s","topic":"projects/p/topics/t",
		"pushConfig":{"pushEndpoint":"https://h.example/x?token=abc","oidcToken":{"serviceAccountEmail":"sa@p","audience":"secret"}},
		"deadLetterPolicy":{"deadLetterTopic":"projects/p/topics/dl"},"expirationPolicy":{}}`), &s)
	r := subscriptionRow(s)
	if r[4] != "push" || r[18] != "h.example" || r[19] != "sa@p" || r[14] != int64(5) || r[12] != nil {
		t.Errorf("subscription row: %#v", r)
	}
	for _, v := range r {
		if str, ok := v.(string); ok && (strings.Contains(str, "token") || strings.Contains(str, "secret")) {
			t.Errorf("leaked %q", str)
		}
	}
	var tp topicJSON
	_ = json.Unmarshal([]byte(`{"name":"projects/p/topics/t","ingestionDataSourceSettings":{"cloudStorage":{"bucket":"b"},"platformLogsSettings":{}}}`), &tp)
	if tr := topicRow(tp); tr[12] != "cloudStorage" {
		t.Errorf("ingestion source: %v", tr[12])
	}
	rows := policyRows("topic", "projects/p/topics/t", &iamPolicyJSON{Version: 1}, nil)
	if len(rows) != 1 || rows[0][3] != nil {
		t.Errorf("empty policy should be one row: %v", rows)
	}
}

func TestResolveConn(t *testing.T) {
	ctx := context.Background()
	// No secret: explains how to create one.
	if _, err := resolveConn(ctx, vgi.Secrets{}, nil, "", ""); err == nil || !strings.Contains(err.Error(), "CREATE SECRET") {
		t.Errorf("no secret: %v", err)
	}
	secrets := func(f map[string]interface{}) vgi.Secrets {
		f["type"] = SecretType
		return vgi.Secrets{"s": f}
	}
	// Real endpoint without credentials: refused (ADC is opt-in only).
	_, err := resolveConn(ctx, secrets(map[string]interface{}{"project_id": "p"}), nil, "", "")
	if err == nil || !strings.Contains(err.Error(), "use_application_default_credentials") {
		t.Errorf("no credentials: %v", err)
	}
	// Emulator host:port: plain HTTP, no token.
	c, err := resolveConn(ctx, secrets(map[string]interface{}{"project_id": "p", "endpoint": "localhost:8085"}), nil, "", "")
	if err != nil || c.Endpoint != "http://localhost:8085" || c.tokens != nil {
		t.Fatalf("emulator: %+v %v", c, err)
	}
	// project := overrides the secret.
	c, _ = resolveConn(ctx, secrets(map[string]interface{}{"project_id": "p", "access_token": "t"}), nil, "", "other")
	if c.Project != "other" || c.Endpoint != DefaultEndpoint {
		t.Errorf("override: %+v", c)
	}
	if _, err := resolveConn(ctx, secrets(map[string]interface{}{"access_token": "t"}), nil, "", ""); err == nil {
		t.Error("missing project must fail")
	}
}

// fakePubSub records every request made to it.
type fakePubSub struct {
	mu       sync.Mutex
	calls    []string
	bodies   map[string]string
	seekFail bool
	create   int
}

func (f *fakePubSub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	call := r.Method + " " + r.URL.Path
	f.calls = append(f.calls, call)
	f.bodies[call] = string(b)
	pulls := 0
	for _, c := range f.calls {
		if strings.HasSuffix(c, ":pull") {
			pulls++
		}
	}
	f.mu.Unlock()
	switch {
	case r.Method == "PUT":
		if f.create != 0 {
			w.WriteHeader(f.create)
			_, _ = w.Write([]byte(`{"error":{"code":409,"status":"ALREADY_EXISTS"}}`))
			return
		}
		_, _ = w.Write([]byte(`{}`))
	case strings.HasSuffix(r.URL.Path, ":seek"):
		if f.seekFail {
			w.WriteHeader(400)
			_, _ = w.Write([]byte(`{"error":{"code":400,"status":"FAILED_PRECONDITION","message":"no retention"}}`))
			return
		}
		_, _ = w.Write([]byte(`{}`))
	case strings.HasSuffix(r.URL.Path, ":pull"):
		if pulls == 1 {
			_, _ = w.Write([]byte(`{"receivedMessages":[
			 {"ackId":"a1","message":{"data":"aGk=","messageId":"2","publishTime":"2026-01-01T00:00:02Z","attributes":{"k":"v"}}},
			 {"ackId":"a2","message":{"data":"eW8=","messageId":"1","publishTime":"2026-01-01T00:00:01Z","orderingKey":"o"}}]}`))
			return
		}
		if pulls == 2 { // a redelivery of message 1
			_, _ = w.Write([]byte(`{"receivedMessages":[{"ackId":"a3","message":{"data":"eW8=","messageId":"1","publishTime":"2026-01-01T00:00:01Z"}}]}`))
			return
		}
		_, _ = w.Write([]byte(`{}`))
	default:
		_, _ = w.Write([]byte(`{}`))
	}
}

func newFake(t *testing.T) (*fakePubSub, *conn) {
	f := &fakePubSub{bodies: map[string]string{}}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return f, &conn{Project: "p", Endpoint: srv.URL, Monitoring: srv.URL}
}

func TestReadTopicLifecycle(t *testing.T) {
	f, c := newFake(t)
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	sub, msgs, err := c.readTopic(context.Background(), messagesArgs{Topic: "orders", AllowTemp: true, StartTime: &start,
		Filter: ptr(`attributes.k = "v"`), MaxMessages: 10, WaitSeconds: 5, Truncate: 100})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(shortID(sub), TempPrefix) || len(msgs) != 2 || msgs[0].ID != "1" || msgs[1].ID != "2" {
		t.Fatalf("sub %s msgs %+v", sub, msgs)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, call := range f.calls {
		if !strings.Contains(call, "/subscriptions/"+TempPrefix) {
			t.Errorf("request outside the temporary subscription: %s", call)
		}
	}
	if f.calls[0] != "PUT /v1/"+sub || f.calls[len(f.calls)-1] != "DELETE /v1/"+sub {
		t.Errorf("lifecycle: %v", f.calls)
	}
	var created map[string]any
	_ = json.Unmarshal([]byte(f.bodies["PUT /v1/"+sub]), &created)
	if created["topic"] != "projects/p/topics/orders" || created["filter"] != `attributes.k = "v"` ||
		created["expirationPolicy"].(map[string]any)["ttl"] != "86400s" || created["labels"].(map[string]any)["vgi-temp"] != "true" {
		t.Errorf("create body: %v", created)
	}
	if !strings.Contains(f.bodies["POST /v1/"+sub+":seek"], "2026-01-01T00:00:00Z") {
		t.Errorf("seek body: %s", f.bodies["POST /v1/"+sub+":seek"])
	}
}

func TestReadTopicDeletesOnError(t *testing.T) {
	f, c := newFake(t)
	f.seekFail = true
	start := time.Now()
	_, _, err := c.readTopic(context.Background(), messagesArgs{Topic: "orders", AllowTemp: true, StartTime: &start, MaxMessages: 1, WaitSeconds: 1, Truncate: 1})
	if err == nil || !strings.Contains(err.Error(), "seeking") {
		t.Fatalf("want seek error, got %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !strings.HasPrefix(f.calls[len(f.calls)-1], "DELETE ") {
		t.Errorf("temporary subscription not deleted after a failure: %v", f.calls)
	}
}

func TestReadTopicNeverPullsAfterCreateConflict(t *testing.T) {
	f, c := newFake(t)
	f.create = http.StatusConflict
	start := time.Now()
	_, _, err := c.readTopic(context.Background(), messagesArgs{Topic: "orders", AllowTemp: true, StartTime: &start, MaxMessages: 1, WaitSeconds: 1, Truncate: 1})
	if err == nil {
		t.Fatal("want an error")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.calls) != 1 {
		t.Errorf("after a failed create only the PUT may be sent, got %v", f.calls)
	}
}

func TestOptIn(t *testing.T) {
	if err := checkOptIn(messagesArgs{Topic: "t"}); err == nil || !strings.Contains(err.Error(), "allow_temporary_subscription") {
		t.Errorf("opt-in: %v", err)
	}
}

func TestRetry(t *testing.T) {
	n := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n++
		if n == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(429)
			return
		}
		_, _ = w.Write([]byte(`{"topics":[{"name":"projects/p/topics/a"}]}`))
	}))
	defer srv.Close()
	c := &conn{Project: "p", Endpoint: srv.URL}
	var out struct {
		Topics []topicJSON `json:"topics"`
	}
	if err := c.pubsub(context.Background(), "GET", "/v1/projects/p/topics", nil, nil, &out); err != nil || len(out.Topics) != 1 || n != 2 {
		t.Errorf("retry: %v %d %d", err, len(out.Topics), n)
	}
}
