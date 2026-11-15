// iam_policies: IAM policies of topics, subscriptions, snapshots and schemas,
// flattened to one row per (resource, role, member). Read with getIamPolicy
// (requestedPolicyVersion 3, so conditional bindings come back); never set.

package pubsub

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
)

type iamPolicyJSON struct {
	Version  int64  `json:"version"`
	Etag     string `json:"etag"`
	Bindings []struct {
		Role      string   `json:"role"`
		Members   []string `json:"members"`
		Condition *struct {
			Title       string `json:"title"`
			Description string `json:"description"`
			Expression  string `json:"expression"`
		} `json:"condition"`
	} `json:"bindings"`
}

var iamColumns = []arrow.Field{
	column("project", str, "Project that owns the resource.", false),
	column("resource_type", str, "'topic', 'subscription', 'snapshot' or 'schema'.", false),
	column("resource", str, "Full resource name the policy is attached to.", false),
	column("role", str, "Role granted, e.g. 'roles/pubsub.subscriber'; NULL on an error row or for a resource with an empty policy.", true),
	column("member", str, "Principal granted the role, e.g. 'serviceAccount:app@p.iam.gserviceaccount.com', 'allUsers'.", true),
	column("member_type", str, "Principal kind: 'user', 'serviceAccount', 'group', 'domain', 'principal', 'allUsers', 'allAuthenticatedUsers', 'deleted'...", true),
	column("condition_title", str, "Title of the IAM condition on the binding; NULL when unconditional.", true),
	column("condition_expression", str, "CEL expression of the IAM condition; NULL when unconditional.", true),
	column("policy_version", i64, "IAM policy version (3 when conditions are present).", true),
	column("etag", str, "Policy etag.", true),
	column("error", str, "Why the policy could not be read: the API status, e.g. 'PERMISSION_DENIED', or 'UNIMPLEMENTED' / 'INVALID_ARGUMENT' on the emulator; NULL when it was read.", true),
}

var iamKinds = []string{"topic", "subscription", "snapshot", "schema"}

type iamArgs struct {
	ResourceType string  `vgi:"name=resource_type,default=all,choices='all,topic,subscription,snapshot,schema',doc=Which kind of resource to read policies for; 'all' walks topics then subscriptions then snapshots then schemas."`
	Resource     *string `vgi:"name=resource,doc=Only this resource (id or full name; needs resource_type other than 'all'): one getIamPolicy call."`
	Project      *string `vgi:"name=project,doc=Google Cloud project id; default the gcp_pubsub secret's PROJECT_ID."`
	PageSize     int64   `vgi:"name=page_size,default=100,ge=1,le=1000,doc=Resources listed per API page; one getIamPolicy call per resource."`
	SecretName   *string `vgi:"name=secret_name,doc=Name of the gcp_pubsub secret to use when several exist."`
}

func (a iamArgs) secret() string  { return deref(a.SecretName) }
func (a iamArgs) project() string { return deref(a.Project) }

var iamExamples = []vgi.CatalogExample{
	{SQL: "SELECT resource_type, resource, role, member FROM pubsub.main.iam_policies() WHERE member IN ('allUsers', 'allAuthenticatedUsers') ORDER BY resource",
		Description: "Public grants on any topic, subscription, snapshot or schema"},
	{SQL: "SELECT member, list(DISTINCT role ORDER BY role) AS roles FROM pubsub.main.iam_policies(resource_type := 'topic') WHERE error IS NULL AND member IS NOT NULL GROUP BY member ORDER BY member",
		Description: "Which principals hold which roles on topics"},
}

// policyRows flattens one policy.
func policyRows(kind, name string, p *iamPolicyJSON, readErr error) [][]any {
	proj := projectOf(name)
	if readErr != nil {
		_, status := statusOf(readErr)
		msg := status
		if msg == "" {
			msg = readErr.Error()
		}
		return [][]any{{proj, kind, name, nil, nil, nil, nil, nil, nil, nil, msg}}
	}
	var rows [][]any
	for _, b := range p.Bindings {
		var title, expr any
		if b.Condition != nil {
			title, expr = optString(b.Condition.Title), optString(b.Condition.Expression)
		}
		for _, m := range b.Members {
			rows = append(rows, []any{proj, kind, name, b.Role, m, memberType(m), title, expr, p.Version, optString(p.Etag), nil})
		}
	}
	if len(rows) == 0 { // keep the resource visible: it has an empty policy
		rows = append(rows, []any{proj, kind, name, nil, nil, nil, nil, nil, p.Version, optString(p.Etag), nil})
	}
	return rows
}

func (c *conn) getPolicy(ctx context.Context, name string) (*iamPolicyJSON, error) {
	var p iamPolicyJSON
	err := c.pubsub(ctx, "GET", namePath(name)+":getIamPolicy", url.Values{"options.requestedPolicyVersion": {"3"}}, nil, &p)
	return &p, err
}

// policyReadable is true for errors that describe the resource rather than
// the request (reported as error rows instead of failing the scan).
func policyReadable(err error) bool {
	code, status := statusOf(err)
	// INVALID_ARGUMENT: the emulator rejects getIamPolicy on snapshots.
	return code == 400 || code == 403 || code == 404 || code == 501 ||
		status == "INVALID_ARGUMENT" || status == "PERMISSION_DENIED" || status == "NOT_FOUND" || status == "UNIMPLEMENTED"
}

// listNames lists one page of resource names of a collection.
func (c *conn) listNames(ctx context.Context, collection string, size int64, token string) ([]string, string, error) {
	var resp map[string]any
	p := pageParams(size, token)
	if err := c.pubsub(ctx, "GET", c.projectPath()+"/"+collection, p, nil, &resp); err != nil {
		return nil, "", err
	}
	var names []string
	if items, ok := resp[collection].([]any); ok {
		for _, it := range items {
			if m, ok := it.(map[string]any); ok {
				if n, ok := m["name"].(string); ok {
					names = append(names, n)
				}
			}
		}
	}
	next, _ := resp["nextPageToken"].(string)
	return names, next, nil
}

func newIAMPolicies() vgi.TableFunction {
	return newList(&listFunc[iamArgs]{
		name: "iam_policies", columns: iamColumns,
		validate: func(a iamArgs) error {
			if deref(a.Resource) != "" && a.ResourceType == "all" {
				return fmt.Errorf("resource := needs resource_type := 'topic', 'subscription', 'snapshot' or 'schema'")
			}
			return nil
		},
		meta: meta("IAM Policies", "security", "iam, policy, bindings, roles, members, permissions, access control, public access, allUsers",
			"IAM policies of Pub/Sub topics, subscriptions, snapshots and schemas (getIamPolicy, policy version 3), flattened to one row per "+
				"(resource, role, member). Named args: resource_type := 'all' (default; walks every topic, subscription, snapshot and schema, one "+
				"call each) | 'topic' | 'subscription' | 'snapshot' | 'schema'; resource := (one resource, needs resource_type); project :=, "+
				"page_size, secret_name. Columns: resource_type, resource, role, member, member_type, condition_title, condition_expression, "+
				"policy_version, etag, error. Resources whose policy cannot be read (PERMISSION_DENIED, or UNIMPLEMENTED on the emulator) "+
				"produce one row with error set instead of failing; a resource with an empty policy produces one row with role NULL. Project-level "+
				"IAM (inherited grants) is not included.",
			"## iam_policies(resource_type := 'all', resource := NULL, project := NULL)\n\nResource-level IAM bindings, one row per "+
				"role and member. Unreadable policies show up as rows with `error` set.",
			"IAM bindings of Pub/Sub topics, subscriptions, snapshots and schemas, one row per role and member", iamColumns, iamExamples),
		fetch: func(ctx context.Context, c *conn, a iamArgs, cursor string) (page, error) {
			if r := deref(a.Resource); r != "" {
				name, err := resourceName(c.Project, a.ResourceType+"s", r)
				if err != nil {
					return page{}, err
				}
				p, err := c.getPolicy(ctx, name)
				if err != nil && !policyReadable(err) {
					return page{}, err
				}
				return page{Rows: policyRows(a.ResourceType, name, p, err)}, nil
			}
			kinds := iamKinds
			if a.ResourceType != "all" {
				kinds = []string{a.ResourceType}
			}
			// cursor = "<kind index>|<page token>"
			idx, token := 0, ""
			if cursor != "" {
				k, t, _ := strings.Cut(cursor, "|")
				_, _ = fmt.Sscanf(k, "%d", &idx)
				token = t
			}
			kind := kinds[idx]
			names, next, err := c.listNames(ctx, kind+"s", a.PageSize, token)
			if err != nil {
				if !policyReadable(err) {
					return page{}, err
				}
				// The listing itself is not permitted/implemented: say so once.
				names, next = nil, ""
			}
			var rows [][]any
			for _, n := range names {
				p, err := c.getPolicy(ctx, n)
				if err != nil && !policyReadable(err) {
					return page{}, fmt.Errorf("%s %s: %w", kind, n, err)
				}
				rows = append(rows, policyRows(kind, n, p, err)...)
			}
			switch {
			case next != "":
				return page{Rows: rows, Next: fmt.Sprintf("%d|%s", idx, next)}, nil
			case idx+1 < len(kinds):
				return page{Rows: rows, Next: fmt.Sprintf("%d|", idx+1)}, nil
			}
			return page{Rows: rows}, nil
		},
	})
}
