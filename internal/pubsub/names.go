// Resource names and Google JSON value formats.

package pubsub

import (
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// shortID returns the last path segment of a resource name
// ("projects/p/topics/orders" -> "orders").
func shortID(name string) string {
	if i := strings.LastIndex(name, "/"); i >= 0 {
		return name[i+1:]
	}
	return name
}

// projectOf returns the project of a resource name ("projects/p/..." -> "p").
func projectOf(name string) string {
	parts := strings.Split(name, "/")
	if len(parts) >= 2 && parts[0] == "projects" {
		return parts[1]
	}
	return ""
}

// idPattern is a Pub/Sub topic / subscription / snapshot / schema id.
var idPattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9._~+%-]{2,254}$`)

// resourceName accepts an id ("orders") or a full name
// ("projects/p/topics/orders") of the given collection and returns the full
// name. A full name may belong to another project (shared topics).
func resourceName(project, collection, s string) (string, error) {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "projects/") {
		parts := strings.Split(s, "/")
		if len(parts) != 4 || parts[2] != collection || parts[1] == "" || !idPattern.MatchString(parts[3]) {
			return "", fmt.Errorf("%q is not a %s name: want projects/<project>/%s/<id> or just <id>", s, singular(collection), collection)
		}
		return s, nil
	}
	if !idPattern.MatchString(s) {
		return "", fmt.Errorf("%q is not a valid %s id (3-255 characters, starting with a letter)", s, singular(collection))
	}
	return "projects/" + project + "/" + collection + "/" + s, nil
}

func singular(collection string) string { return strings.TrimSuffix(collection, "s") }

// namePath escapes a resource name for a URL path, keeping the slashes.
func namePath(name string) string {
	parts := strings.Split(name, "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}
	return "/v1/" + strings.Join(parts, "/")
}

// durationSeconds parses a google.protobuf.Duration JSON value ("604800s",
// "1.5s"); "" is NULL.
func durationSeconds(s string) any {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	f, err := strconv.ParseFloat(strings.TrimSuffix(s, "s"), 64)
	if err != nil {
		return nil
	}
	return f
}

// formatDuration renders seconds as a google.protobuf.Duration JSON value.
func formatDuration(sec int64) string { return strconv.FormatInt(sec, 10) + "s" }

// formatTime renders a time as RFC 3339 in UTC, as the APIs expect.
func formatTime(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

// pushHost returns only the host (and port) of a push endpoint URL: paths and
// query strings can carry tokens, so they are never returned.
func pushHost(endpoint string) any {
	if endpoint == "" {
		return nil
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" {
		return nil
	}
	return u.Host
}

// memberType splits an IAM member ("user:ada@example.com") into its type.
func memberType(member string) string {
	if i := strings.Index(member, ":"); i > 0 {
		return member[:i]
	}
	return member // allUsers, allAuthenticatedUsers
}
