// Backlog and throughput metrics from Cloud Monitoring
// (projects.timeSeries.list, https://cloud.google.com/monitoring/api/ref_v3/rest/v3/projects.timeSeries/list).
//
// time_series returns long rows in vgi-prometheus's shape (labels MAP, ts,
// value) for any Pub/Sub metric. subscription_backlog is the common question
// answered directly: the latest undelivered-message count and oldest unacked
// age (consumer lag) per subscription.
//
// Pushdown: the metric, subscription / topic and any extra filter become the
// Monitoring filter; start/end become the request interval; alignment and
// cross-series reduction run server-side.

package pubsub

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
)

type pointJSON struct {
	Interval struct {
		StartTime string `json:"startTime"`
		EndTime   string `json:"endTime"`
	} `json:"interval"`
	Value struct {
		Int64Value        *string  `json:"int64Value"`
		DoubleValue       *float64 `json:"doubleValue"`
		BoolValue         *bool    `json:"boolValue"`
		DistributionValue *struct {
			Count string  `json:"count"`
			Mean  float64 `json:"mean"`
		} `json:"distributionValue"`
	} `json:"value"`
}

type timeSeriesJSON struct {
	Metric struct {
		Type   string            `json:"type"`
		Labels map[string]string `json:"labels"`
	} `json:"metric"`
	Resource struct {
		Type   string            `json:"type"`
		Labels map[string]string `json:"labels"`
	} `json:"resource"`
	MetricKind string      `json:"metricKind"`
	ValueType  string      `json:"valueType"`
	Unit       string      `json:"unit"`
	Points     []pointJSON `json:"points"`
}

type timeSeriesResp struct {
	TimeSeries      []timeSeriesJSON `json:"timeSeries"`
	NextPageToken   string           `json:"nextPageToken"`
	ExecutionErrors []struct {
		Message string `json:"message"`
	} `json:"executionErrors"`
}

// pointValue converts a point's typed value to a float (NaN when absent).
func pointValue(p pointJSON) (float64, bool) {
	v := p.Value
	switch {
	case v.DoubleValue != nil:
		return *v.DoubleValue, true
	case v.Int64Value != nil:
		i, err := strconv.ParseInt(*v.Int64Value, 10, 64)
		return float64(i), err == nil
	case v.BoolValue != nil:
		if *v.BoolValue {
			return 1, true
		}
		return 0, true
	case v.DistributionValue != nil:
		return v.DistributionValue.Mean, true
	}
	return math.NaN(), false
}

// metricType expands 'subscription/num_undelivered_messages' to the full
// pubsub.googleapis.com/... type; full types are kept.
func metricType(m string) (string, error) {
	m = strings.TrimSpace(m)
	if m == "" {
		return "", errors.New("metric is empty: give e.g. 'subscription/num_undelivered_messages'")
	}
	if strings.ContainsAny(m, "\" ") {
		return "", fmt.Errorf("metric %q must not contain quotes or spaces", m)
	}
	if !strings.Contains(m, ".googleapis.com/") && !strings.HasPrefix(m, "custom.") && !strings.HasPrefix(m, "external.") {
		m = "pubsub.googleapis.com/" + strings.TrimPrefix(m, "/")
	}
	return m, nil
}

// defaultAligner: gauges are averaged, counters/deltas summed per period.
func defaultAligner(metric string) string {
	if strings.HasSuffix(metric, "_count") || strings.HasSuffix(metric, "byte_cost") {
		return "ALIGN_SUM"
	}
	return "ALIGN_MEAN"
}

// quote renders a Monitoring filter string literal.
func quote(s string) string { return strconv.Quote(s) }

// monitoringFilter builds the filter expression.
func monitoringFilter(metric, subscription, topic, extra string) string {
	parts := []string{"metric.type = " + quote(metric)}
	if subscription != "" {
		parts = append(parts, "resource.labels.subscription_id = "+quote(shortID(subscription)))
	}
	if topic != "" {
		parts = append(parts, "resource.labels.topic_id = "+quote(shortID(topic)))
	}
	if extra = strings.TrimSpace(extra); extra != "" {
		parts = append(parts, "("+extra+")")
	}
	return strings.Join(parts, " AND ")
}

// monitoring calls projects.timeSeries.list.
func (c *conn) listTimeSeries(ctx context.Context, p url.Values) (*timeSeriesResp, error) {
	if c.Monitoring == DefaultMonitoringEndpoint && c.tokens == nil {
		return nil, errors.New("cloud Monitoring needs credentials: the gcp_pubsub secret points at an emulator/proxy without SERVICE_ACCOUNT_JSON or ACCESS_TOKEN " +
			"(the Pub/Sub emulator has no metrics); set MONITORING_ENDPOINT for a compatible endpoint")
	}
	var resp timeSeriesResp
	err := c.call(ctx, "GET", c.Monitoring, "/v3/projects/"+url.PathEscape(c.Project)+"/timeSeries", p, nil, &resp, true)
	if err != nil {
		return nil, err
	}
	for _, e := range resp.ExecutionErrors {
		if e.Message != "" {
			return nil, fmt.Errorf("cloud monitoring: %s", e.Message)
		}
	}
	return &resp, nil
}

func mergedLabels(ts timeSeriesJSON) map[string]string {
	m := map[string]string{}
	for k, v := range ts.Resource.Labels {
		m[k] = v
	}
	for k, v := range ts.Metric.Labels {
		m[k] = v
	}
	return m
}

// ---------------------------------------------------------------- time_series

var timeSeriesColumns = []arrow.Field{
	column("project", str, "Project the series belongs to (resource label project_id).", false),
	column("metric", str, "Full metric type, e.g. 'pubsub.googleapis.com/subscription/num_undelivered_messages'.", false),
	column("resource_type", str, "Monitored resource type: 'pubsub_subscription', 'pubsub_topic' or 'pubsub_snapshot'.", false),
	column("topic", str, "Topic id (resource label topic_id); NULL for subscription metrics.", true),
	column("subscription", str, "Subscription id (resource label subscription_id); NULL for topic metrics.", true),
	column("labels", strMap, "Every resource and metric label of the series (after any group_by reduction).", false),
	column("ts", tstz, "End of the point's interval (the sample time for gauges).", false),
	column("start_ts", tstz, "Start of the point's interval (for delta metrics, the aligned period start).", true),
	column("value", f64, "Point value: int64/double as DOUBLE, booleans as 0/1, distributions as their mean.", true),
	column("metric_kind", str, "'GAUGE', 'DELTA' or 'CUMULATIVE' (after alignment).", true),
	column("value_type", str, "'INT64', 'DOUBLE', 'BOOL' or 'DISTRIBUTION'.", true),
	column("unit", str, "Unit of the value, e.g. 's', 'By', '1'.", true),
}

type timeSeriesArgs struct {
	Metric           string     `vgi:"pos=0,doc=Cloud Monitoring metric type. A path without a domain is taken as a Pub/Sub metric (pubsub.googleapis.com/ is prefixed); a full metric type is used as given."`
	StartTime        *time.Time `vgi:"name=start_time,doc=Interval start; default end_time minus one hour."`
	EndTime          *time.Time `vgi:"name=end_time,doc=Interval end; default now."`
	Subscription     *string    `vgi:"name=subscription,doc=Only this subscription (id or full name): pushed into the filter as resource.labels.subscription_id."`
	Topic            *string    `vgi:"name=topic,doc=Only this topic (id or full name): pushed into the filter as resource.labels.topic_id."`
	Filter           *string    `vgi:"name=filter,doc=Extra Cloud Monitoring filter ANDed to the metric filter, e.g. 'metric.labels.response_code = \"success\"'."`
	AlignmentSeconds int64      `vgi:"name=alignment_seconds,default=60,ge=0,le=86400,doc=Alignment period in seconds (0 = raw points, no alignment)."`
	Aligner          string     `vgi:"name=aligner,default=auto,choices='auto,ALIGN_NONE,ALIGN_MEAN,ALIGN_MAX,ALIGN_MIN,ALIGN_SUM,ALIGN_COUNT,ALIGN_RATE,ALIGN_DELTA,ALIGN_NEXT_OLDER,ALIGN_PERCENTILE_99,ALIGN_PERCENTILE_95,ALIGN_PERCENTILE_50',doc=Per-series aligner; 'auto' sums *_count and byte_cost metrics and averages the rest."`
	Reducer          *string    `vgi:"name=reducer,choices='REDUCE_SUM,REDUCE_MEAN,REDUCE_MAX,REDUCE_MIN,REDUCE_COUNT,REDUCE_PERCENTILE_99',doc=Optional cross-series reducer run server-side, e.g. 'REDUCE_SUM' (with group_by to keep some labels)."`
	GroupBy          *string    `vgi:"name=group_by,doc=Comma-separated labels kept by the reducer, e.g. 'resource.label.topic_id'."`
	Project          *string    `vgi:"name=project,doc=Google Cloud project id; default the gcp_pubsub secret's PROJECT_ID."`
	PageSize         int64      `vgi:"name=page_size,default=1000,ge=1,le=100000,doc=Points per API page; pages are fetched lazily."`
	SecretName       *string    `vgi:"name=secret_name,doc=Name of the gcp_pubsub secret to use when several exist."`
}

func (a timeSeriesArgs) secret() string  { return deref(a.SecretName) }
func (a timeSeriesArgs) project() string { return deref(a.Project) }

var timeSeriesExamples = []vgi.CatalogExample{
	{SQL: "SELECT subscription, max(value) AS peak_backlog FROM pubsub.main.time_series('subscription/num_undelivered_messages', start_time := TIMESTAMPTZ '2026-01-01 00:00:00+00', end_time := TIMESTAMPTZ '2026-01-02 00:00:00+00') GROUP BY subscription ORDER BY peak_backlog DESC",
		Description: "Peak backlog per subscription over a day"},
	{SQL: "SELECT ts, labels['response_code'] AS response_code, value FROM pubsub.main.time_series('topic/send_request_count', topic := 'orders', alignment_seconds := 300) ORDER BY ts, response_code",
		Description: "Publish requests to a topic per 5 minutes by response code (last hour)"},
}

// timeWindow resolves start/end defaults.
func timeWindow(start, end *time.Time, def time.Duration) (time.Time, time.Time, error) {
	e := time.Now().UTC()
	if end != nil {
		e = *end
	}
	s := e.Add(-def)
	if start != nil {
		s = *start
	}
	if !s.Before(e) {
		return s, e, fmt.Errorf("start_time (%s) must be before end_time (%s)", formatTime(s), formatTime(e))
	}
	return s, e, nil
}

func (a timeSeriesArgs) params(project string) (url.Values, error) {
	m, err := metricType(a.Metric)
	if err != nil {
		return nil, err
	}
	s, e, err := timeWindow(a.StartTime, a.EndTime, time.Hour)
	if err != nil {
		return nil, err
	}
	p := url.Values{
		"filter":             {monitoringFilter(m, deref(a.Subscription), deref(a.Topic), deref(a.Filter))},
		"interval.startTime": {formatTime(s)},
		"interval.endTime":   {formatTime(e)},
		"pageSize":           {strconv.FormatInt(a.PageSize, 10)},
	}
	aligner := a.Aligner
	if aligner == "auto" || aligner == "" {
		aligner = defaultAligner(m)
	}
	if a.AlignmentSeconds > 0 && aligner != "ALIGN_NONE" {
		p.Set("aggregation.alignmentPeriod", formatDuration(a.AlignmentSeconds))
		p.Set("aggregation.perSeriesAligner", aligner)
		if r := deref(a.Reducer); r != "" {
			p.Set("aggregation.crossSeriesReducer", r)
			for _, g := range strings.Split(deref(a.GroupBy), ",") {
				if g = strings.TrimSpace(g); g != "" {
					p.Add("aggregation.groupByFields", g)
				}
			}
		}
	} else if deref(a.Reducer) != "" {
		return nil, errors.New("reducer := needs alignment (alignment_seconds > 0 and an aligner other than ALIGN_NONE)")
	}
	return p, nil
}

func seriesRows(c *conn, resp *timeSeriesResp) [][]any {
	var rows [][]any
	for _, ts := range resp.TimeSeries {
		labels := mergedLabels(ts)
		proj := ts.Resource.Labels["project_id"]
		if proj == "" {
			proj = c.Project
		}
		pts := append([]pointJSON(nil), ts.Points...)
		sort.SliceStable(pts, func(i, j int) bool { return pts[i].Interval.EndTime < pts[j].Interval.EndTime })
		for _, p := range pts {
			end := optTime(p.Interval.EndTime)
			if end == nil {
				continue
			}
			var val any
			if v, ok := pointValue(p); ok {
				val = v
			}
			rows = append(rows, []any{proj, ts.Metric.Type, ts.Resource.Type, optString(ts.Resource.Labels["topic_id"]),
				optString(ts.Resource.Labels["subscription_id"]), labels, end, optTime(p.Interval.StartTime), val,
				optString(ts.MetricKind), optString(ts.ValueType), optString(ts.Unit)})
		}
	}
	return rows
}

func newTimeSeries() vgi.TableFunction {
	return newList(&listFunc[timeSeriesArgs]{
		name: "time_series", columns: timeSeriesColumns,
		validate: func(a timeSeriesArgs) error { _, err := a.params(""); return err },
		meta: meta("Metric Time Series", "metrics", "metrics, cloud monitoring, time series, backlog, num_undelivered_messages, oldest_unacked_message_age, send_request_count, throughput, lag",
			"Cloud Monitoring time series for a Pub/Sub metric (projects.timeSeries.list) as long rows: one per series and point. Positional "+
				"arg: metric ('subscription/num_undelivered_messages', 'subscription/oldest_unacked_message_age', 'topic/send_request_count', "+
				"'subscription/ack_message_count', 'topic/message_sizes', ...; pubsub.googleapis.com/ is prefixed, or give a full type). Named: "+
				"start_time / end_time `TIMESTAMPTZ` (default the last hour), subscription := / topic := (pushed into the filter), filter := (extra "+
				"Monitoring filter), alignment_seconds (default 60; 0 = raw), aligner ('auto' sums *_count metrics, averages the rest), reducer + "+
				"group_by (server-side cross-series aggregation), project, page_size, secret_name. Columns: project, metric, resource_type, topic, "+
				"subscription, labels `MAP`, ts, start_ts, value `DOUBLE`, metric_kind, value_type, unit. Needs roles/monitoring.viewer; the emulator has no metrics.",
			"## time_series(metric, start_time := now() - 1h, end_time := now())\n\nPub/Sub metrics from Cloud Monitoring as long rows "+
				"(labels map, `ts`, `value`), aligned and optionally reduced server-side.",
			"Cloud Monitoring time series for a Pub/Sub metric as long rows (labels, ts, value)", timeSeriesColumns, timeSeriesExamples),
		fetch: func(ctx context.Context, c *conn, a timeSeriesArgs, cursor string) (page, error) {
			p, err := a.params(c.Project)
			if err != nil {
				return page{}, err
			}
			if cursor != "" {
				p.Set("pageToken", cursor)
			}
			resp, err := c.listTimeSeries(ctx, p)
			if err != nil {
				return page{}, err
			}
			return page{Rows: seriesRows(c, resp), Next: resp.NextPageToken}, nil
		},
	})
}

// ---------------------------------------------------------------- subscription_backlog

var backlogColumns = []arrow.Field{
	column("project", str, "Project of the subscription.", false),
	column("subscription", str, "Subscription id.", false),
	column("ts", tstz, "Time of the latest point used (the newest of the two metrics).", false),
	column("undelivered_messages", i64, "Messages not yet acknowledged (num_undelivered_messages): the consumer lag in messages. NULL when the metric has no recent point.", true),
	column("oldest_unacked_age_seconds", f64, "Age of the oldest unacknowledged message in seconds (oldest_unacked_message_age): the consumer lag in time.", true),
}

type backlogArgs struct {
	EndTime         *time.Time `vgi:"name=end_time,doc=Look back from this time; default now."`
	LookbackMinutes int64      `vgi:"name=lookback_minutes,default=10,ge=2,le=1440,doc=How far back to look for the latest point (metrics are sampled every minute and arrive a few minutes late)."`
	Subscription    *string    `vgi:"name=subscription,doc=Only this subscription (id or full name): pushed into the Monitoring filter."`
	Project         *string    `vgi:"name=project,doc=Google Cloud project id; default the gcp_pubsub secret's PROJECT_ID."`
	SecretName      *string    `vgi:"name=secret_name,doc=Name of the gcp_pubsub secret to use when several exist."`
}

func (a backlogArgs) secret() string  { return deref(a.SecretName) }
func (a backlogArgs) project() string { return deref(a.Project) }

var backlogExamples = []vgi.CatalogExample{
	{SQL: "SELECT subscription, undelivered_messages, oldest_unacked_age_seconds / 60 AS lag_minutes FROM pubsub.main.subscription_backlog() ORDER BY oldest_unacked_age_seconds DESC NULLS LAST",
		Description: "Which subscriptions are furthest behind right now"},
	{SQL: "SELECT s.subscription, s.dead_letter_topic, b.undelivered_messages FROM pubsub.main.subscriptions() s JOIN pubsub.main.subscription_backlog() b ON b.subscription = s.subscription WHERE b.undelivered_messages > 1000 ORDER BY b.undelivered_messages DESC",
		Description: "Backlogged subscriptions with their dead-letter settings"},
}

// latestPoints returns subscription -> (latest end time, value) of one metric.
func (c *conn) latestPoints(ctx context.Context, metric, subscription string, start, end time.Time) (map[string]struct {
	T time.Time
	V float64
}, error) {
	out := map[string]struct {
		T time.Time
		V float64
	}{}
	p := url.Values{
		"filter":                       {monitoringFilter(metric, subscription, "", "")},
		"interval.startTime":           {formatTime(start)},
		"interval.endTime":             {formatTime(end)},
		"aggregation.alignmentPeriod":  {"60s"},
		"aggregation.perSeriesAligner": {"ALIGN_MAX"},
		"pageSize":                     {"10000"},
	}
	for token := ""; ; {
		if token != "" {
			p.Set("pageToken", token)
		}
		resp, err := c.listTimeSeries(ctx, p)
		if err != nil {
			return nil, err
		}
		for _, ts := range resp.TimeSeries {
			sub := ts.Resource.Labels["subscription_id"]
			for _, pt := range ts.Points {
				t, ok := optTime(pt.Interval.EndTime).(time.Time)
				v, okv := pointValue(pt)
				if !ok || !okv {
					continue
				}
				if cur, seen := out[sub]; !seen || t.After(cur.T) {
					out[sub] = struct {
						T time.Time
						V float64
					}{t, v}
				}
			}
		}
		if token = resp.NextPageToken; token == "" {
			return out, nil
		}
	}
}

func newSubscriptionBacklog() vgi.TableFunction {
	return newList(&listFunc[backlogArgs]{
		name: "subscription_backlog", columns: backlogColumns,
		meta: meta("Subscription Backlog", "metrics", "backlog, consumer lag, undelivered messages, oldest unacked message age, num_undelivered_messages, stuck subscribers",
			"Current consumer lag per subscription from Cloud Monitoring: the latest num_undelivered_messages and oldest_unacked_message_age "+
				"points within the lookback (two timeSeries.list calls, aligned to 1 minute with ALIGN_MAX). Named args: lookback_minutes (default "+
				"10), subscription := (pushed into the filter), project, secret_name. Columns: project, subscription, ts, undelivered_messages "+
				"`BIGINT`, oldest_unacked_age_seconds `DOUBLE`. Join to subscriptions() on subscription. For history use time_series(). Needs "+
				"roles/monitoring.viewer.",
			"## subscription_backlog(lookback_minutes := 10, subscription := NULL)\n\nThe latest backlog size and oldest-unacked age of "+
				"every subscription: how far each consumer is behind.",
			"Latest backlog size and oldest unacked message age (consumer lag) per subscription", backlogColumns, backlogExamples),
		fetch: func(ctx context.Context, c *conn, a backlogArgs, _ string) (page, error) {
			end := time.Now().UTC().Truncate(time.Minute)
			if a.EndTime != nil {
				end = a.EndTime.UTC()
			}
			start := end.Add(-time.Duration(a.LookbackMinutes) * time.Minute)
			sub := deref(a.Subscription)
			n, err := c.latestPoints(ctx, "pubsub.googleapis.com/subscription/num_undelivered_messages", sub, start, end)
			if err != nil {
				return page{}, err
			}
			age, err := c.latestPoints(ctx, "pubsub.googleapis.com/subscription/oldest_unacked_message_age", sub, start, end)
			if err != nil {
				return page{}, err
			}
			keys := map[string]bool{}
			for k := range n {
				keys[k] = true
			}
			for k := range age {
				keys[k] = true
			}
			names := make([]string, 0, len(keys))
			for k := range keys {
				names = append(names, k)
			}
			sort.Strings(names)
			rows := make([][]any, 0, len(names))
			for _, s := range names {
				var count, ageV any
				var ts time.Time
				if p, ok := n[s]; ok {
					count, ts = int64(p.V), p.T
				}
				if p, ok := age[s]; ok {
					ageV = p.V
					if p.T.After(ts) {
						ts = p.T
					}
				}
				rows = append(rows, []any{c.Project, s, ts, count, ageV})
			}
			return page{Rows: rows}, nil
		},
	})
}
