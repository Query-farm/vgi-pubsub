// access_check: feature-detect what the credential may read, one cheap call
// per capability, so a permission gap shows up as a row instead of as a
// confusing failure deep inside a query.

package pubsub

import (
	"context"
	"net/url"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
)

var accessColumns = []arrow.Field{
	column("capability", str, "What was checked: list_topics, list_subscriptions, list_snapshots, list_schemas, read_iam_policy, attach_subscription, monitoring_metrics.", false),
	column("used_by", str, "The functions that need this capability.", false),
	column("api_call", str, "The REST call made to check it.", false),
	column("available", boolean, "true when the call succeeded; false when it was refused; NULL when it could not be checked (e.g. no topic to test against).", true),
	column("http_status", i64, "HTTP status of the check call; NULL when skipped.", true),
	column("status", str, "Google canonical status of a failure, e.g. 'PERMISSION_DENIED', 'UNIMPLEMENTED' (emulator); 'OK' on success.", true),
	column("message", str, "Error message or why the check was skipped; NULL on success.", true),
}

type accessArgs struct {
	Project    *string `vgi:"name=project,doc=Google Cloud project id; default the gcp_pubsub secret's PROJECT_ID."`
	SecretName *string `vgi:"name=secret_name,doc=Name of the gcp_pubsub secret to use when several exist."`
}

func (a accessArgs) secret() string  { return deref(a.SecretName) }
func (a accessArgs) project() string { return deref(a.Project) }

var accessExamples = []vgi.CatalogExample{
	{SQL: "SELECT capability, available, status, used_by FROM pubsub.main.access_check() ORDER BY capability",
		Description: "Which parts of the worker this credential can use"},
}

func checkRow(capability, usedBy, call string, err error) []any {
	if err == nil {
		return []any{capability, usedBy, call, true, int64(200), "OK", nil}
	}
	code, status := statusOf(err)
	var hs any
	if code != 0 {
		hs = int64(code)
	}
	return []any{capability, usedBy, call, false, hs, optString(status), err.Error()}
}

func skipRow(capability, usedBy, call, why string) []any {
	return []any{capability, usedBy, call, nil, nil, nil, why}
}

func newAccessCheck() vgi.TableFunction {
	return newList(&listFunc[accessArgs]{
		name: "access_check", columns: accessColumns,
		meta: meta("Access Check", "status", "permissions, access, iam roles, feature detection, troubleshooting, credentials",
			"Feature-detect what the gcp_pubsub credential can do: one cheap read per capability (list topics / subscriptions / snapshots / "+
				"schemas with pageSize 1, getIamPolicy and testIamPermissions(pubsub.topics.attachSubscription) on the first topic, and a "+
				"Cloud Monitoring timeSeries.list). Named args: project, secret_name. Columns: capability, used_by, api_call, available "+
				"(`BOOLEAN`; NULL = not checkable), http_status, status (e.g. PERMISSION_DENIED, UNIMPLEMENTED on the emulator), message. "+
				"Run it first when a query fails with a permission error.",
			"## access_check(project := NULL)\n\nOne row per capability with whether the credential may use it: a quick way to see "+
				"which roles are missing.",
			"Check which Pub/Sub and Monitoring reads the credential is allowed to make", accessColumns, accessExamples),
		fetch: func(ctx context.Context, c *conn, _ accessArgs, _ string) (page, error) {
			var rows [][]any
			var firstTopic string
			lists := []struct{ cap, coll, used string }{
				{"list_topics", "topics", "topics, iam_policies"},
				{"list_subscriptions", "subscriptions", "subscriptions, iam_policies"},
				{"list_snapshots", "snapshots", "snapshots, iam_policies"},
				{"list_schemas", "schemas", "schemas, schema_revisions"},
			}
			for _, l := range lists {
				names, _, err := c.listNames(ctx, l.coll, 1, "")
				if l.coll == "topics" && err == nil && len(names) > 0 {
					firstTopic = names[0]
				}
				rows = append(rows, checkRow(l.cap, l.used, "GET projects/"+c.Project+"/"+l.coll, err))
			}
			if firstTopic == "" {
				rows = append(rows,
					skipRow("read_iam_policy", "iam_policies", "GET <topic>:getIamPolicy", "no topic to check against"),
					skipRow("attach_subscription", "read_messages", "POST <topic>:testIamPermissions", "no topic to check against"))
			} else {
				_, err := c.getPolicy(ctx, firstTopic)
				rows = append(rows, checkRow("read_iam_policy", "iam_policies", "GET "+firstTopic+":getIamPolicy", err))
				var tp struct {
					Permissions []string `json:"permissions"`
				}
				perm := "pubsub.topics.attachSubscription"
				err = c.pubsub(ctx, "POST", namePath(firstTopic)+":testIamPermissions", nil, map[string]any{"permissions": []string{perm}}, &tp)
				row := checkRow("attach_subscription", "read_messages", "POST "+firstTopic+":testIamPermissions", err)
				if err == nil && len(tp.Permissions) == 0 {
					row[3], row[5], row[6] = false, "PERMISSION_DENIED", "the credential lacks "+perm+" on "+firstTopic
				}
				rows = append(rows, row)
			}
			end := time.Now().UTC().Truncate(time.Minute)
			p := url.Values{
				"filter":             {monitoringFilter("pubsub.googleapis.com/subscription/num_undelivered_messages", "", "", "")},
				"interval.startTime": {formatTime(end.Add(-5 * time.Minute))},
				"interval.endTime":   {formatTime(end)},
				"pageSize":           {"1"},
			}
			_, err := c.listTimeSeries(ctx, p)
			rows = append(rows, checkRow("monitoring_metrics", "time_series, subscription_backlog", "GET projects/"+c.Project+"/timeSeries", err))
			return page{Rows: rows}, nil
		},
	})
}
