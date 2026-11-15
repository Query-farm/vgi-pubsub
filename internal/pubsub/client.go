// Connection, credentials and the HTTP/JSON client for the Pub/Sub and
// Cloud Monitoring REST APIs.
//
// Every function reads a `gcp_pubsub` DuckDB secret on every call:
//
//	CREATE SECRET (TYPE gcp_pubsub, PROJECT_ID 'my-project',
//	    SERVICE_ACCOUNT_JSON '{"type": "service_account", ...}');
//	CREATE SECRET (TYPE gcp_pubsub, PROJECT_ID 'p', ACCESS_TOKEN 'ya29....');
//	CREATE SECRET (TYPE gcp_pubsub, PROJECT_ID 'p', ENDPOINT 'localhost:8085');   -- emulator
//
// Application Default Credentials (gcloud, GCE metadata, GOOGLE_APPLICATION_CREDENTIALS)
// are never used implicitly: only when the ATTACH sets
// use_application_default_credentials true and no secret supplies credentials.
// Tokens are cached in memory per process and never stored in scan state,
// which travels back to DuckDB over HTTP.

package pubsub

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

const (
	// SecretType is the DuckDB secret type every function reads.
	SecretType = "gcp_pubsub"
	// MaxAttempts bounds retries of 429/500/502/503/504 responses and network errors.
	MaxAttempts = 4
	// Timeout bounds every request (a pull uses its own, shorter deadline).
	Timeout = 60 * time.Second
	// DefaultEndpoint is the Pub/Sub REST API root.
	DefaultEndpoint = "https://pubsub.googleapis.com"
	// DefaultMonitoringEndpoint is the Cloud Monitoring REST API root.
	DefaultMonitoringEndpoint = "https://monitoring.googleapis.com"
	// optADC is the ATTACH option that opts in to Application Default Credentials.
	optADC = "use_application_default_credentials"
)

// scopes requested for service-account and ADC tokens: Pub/Sub plus
// read-only Monitoring.
var scopes = []string{
	"https://www.googleapis.com/auth/pubsub",
	"https://www.googleapis.com/auth/monitoring.read",
}

// userAgent identifies the worker to the API.
const userAgent = "vgi-pubsub-worker/" + Version

func secretField(name, doc string, redact bool) arrow.Field {
	keys := []string{"vgi_doc"}
	vals := []string{doc}
	if redact {
		keys = append(keys, "redact")
		vals = append(vals, "true")
	}
	return arrow.Field{Name: name, Type: arrow.BinaryTypes.String, Nullable: true,
		Metadata: arrow.NewMetadata(keys, vals)}
}

// APISecretType is registered with DuckDB so users can run
// CREATE SECRET (TYPE gcp_pubsub, PROJECT_ID '...', SERVICE_ACCOUNT_JSON '...').
var APISecretType = vgi.SecretTypeSpec{
	Name:        SecretType,
	Description: "Google Cloud Pub/Sub connection: project, credentials (service account key JSON or OAuth access token) and optional endpoint overrides (emulator).",
	Schema: arrow.NewSchema([]arrow.Field{
		secretField("project_id", "Default Google Cloud project id, e.g. 'my-project'. Functions take project := to override it.", false),
		secretField("service_account_json", "Service account key file contents (JSON). The worker signs a JWT and exchanges it at the key's token_uri for an access token.", true),
		secretField("access_token", "OAuth 2.0 access token (e.g. from `gcloud auth print-access-token`), sent as a bearer token. Expires after about an hour.", true),
		secretField("endpoint", "Pub/Sub API root. Default https://pubsub.googleapis.com. For the emulator give its host:port (the PUBSUB_EMULATOR_HOST value) or http://host:port; no credentials are sent unless the secret has them.", false),
		secretField("monitoring_endpoint", "Cloud Monitoring API root for time_series and subscription_backlog. Default https://monitoring.googleapis.com.", false),
		secretField("quota_project_id", "Project billed for API quota, sent as x-goog-user-project (needed by some user credentials).", false),
	}, &secretSchemaMetadata),
}

// secretSchemaMetadata links the secret type to its documentation.
var secretSchemaMetadata = arrow.NewMetadata(
	[]string{"vgi_doc_url"},
	[]string{SourceURL + "#secrets-and-configuration"})

// secretRequirements is declared by every API function.
var secretRequirements = []vgi.SecretRequirement{{SecretType: SecretType}}

// AttachOptionSpecs are the ATTACH options this worker accepts.
var AttachOptionSpecs = []vgi.AttachOptionSpec{
	{Name: optADC, Type: arrow.FixedWidthTypes.Boolean, Description: "Opt in to Google Application Default " +
		"Credentials (GOOGLE_APPLICATION_CREDENTIALS, gcloud user credentials, or the GCE/GKE metadata server) " +
		"when the gcp_pubsub secret has no SERVICE_ACCOUNT_JSON or ACCESS_TOKEN. Off by default: the worker never " +
		"reads local credentials unless this is true."},
}

// secretNamed is implemented by every argument struct.
type secretNamed interface{ secret() string }

// bindWithSecret binds a table function's schema. When secret_name names a
// secret, the first bind asks the extension to resolve that secret by name
// (two-phase bind); otherwise only the default secret of the type is sent.
func bindWithSecret[A secretNamed](params *vgi.BindParams, schema *arrow.Schema) (*vgi.BindResponse, error) {
	var a A
	if err := vgi.BindArgs(params.Args, &a); err != nil {
		return nil, err
	}
	if name := a.secret(); name != "" && !params.ResolvedSecretsProvided {
		return &vgi.BindResponse{SecretScopeRequest: []vgi.SecretLookup{{SecretType: SecretType, SecretName: name}}}, nil
	}
	return vgi.BindSchema(schema)
}

// conn is a resolved connection, built per call and never stored in state.
type conn struct {
	Project    string
	Endpoint   string
	Monitoring string
	tokens     oauth2.TokenSource // nil: send no Authorization (emulator)
	quota      string
}

// normalizeEndpoint accepts https://host, http://host:port or host:port (the
// PUBSUB_EMULATOR_HOST form, which means plain HTTP).
func normalizeEndpoint(raw, def, field string) (string, error) {
	s := strings.TrimRight(strings.TrimSpace(raw), "/")
	if s == "" {
		return def, nil
	}
	if !strings.Contains(s, "://") {
		s = "http://" + s
	}
	u, err := url.Parse(s)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", fmt.Errorf("the gcp_pubsub secret's %s must be host:port or an http(s) URL, got %q", field, raw)
	}
	s = strings.TrimSuffix(s, "/v1")
	s = strings.TrimSuffix(s, "/v3")
	return s, nil
}

var (
	tokenMu    sync.Mutex
	tokenCache = map[string]oauth2.TokenSource{}
)

// cachedSource returns a reusable token source keyed by a hash of the
// credential material, so a service-account token is minted once per hour,
// not once per request.
func cachedSource(key string, mk func() (oauth2.TokenSource, error)) (oauth2.TokenSource, error) {
	sum := sha256.Sum256([]byte(key))
	k := hex.EncodeToString(sum[:])
	tokenMu.Lock()
	defer tokenMu.Unlock()
	if ts, ok := tokenCache[k]; ok {
		return ts, nil
	}
	ts, err := mk()
	if err != nil {
		return nil, err
	}
	ts = oauth2.ReuseTokenSource(nil, ts)
	tokenCache[k] = ts
	return ts, nil
}

// lookupSecret returns the fields of the named secret, or the default one.
func lookupSecret(secrets vgi.Secrets, name string) (map[string]interface{}, bool, error) {
	if name != "" {
		fields, ok := secrets.LookupNamed(SecretType, name)
		if !ok {
			return nil, false, fmt.Errorf("no gcp_pubsub secret named %q: create it with CREATE SECRET %s (TYPE gcp_pubsub, PROJECT_ID '...', SERVICE_ACCOUNT_JSON '...')", name, name)
		}
		return fields, true, nil
	}
	fields, ok := secrets.Lookup(SecretType)
	return fields, ok, nil
}

// resolveConn turns the secret (and the ADC opt-in) into a connection.
// project overrides the secret's PROJECT_ID.
func resolveConn(ctx context.Context, secrets vgi.Secrets, attach *vgi.AttachOptions, name, project string) (*conn, error) {
	fields, have, err := lookupSecret(secrets, name)
	if err != nil {
		return nil, err
	}
	get := func(k string) string {
		if !have {
			return ""
		}
		return strings.TrimSpace(vgi.RenderSecretValue(fields[k]))
	}
	useADC := false
	if attach != nil {
		useADC, _ = attach.Bool(optADC)
	}
	c := &conn{Project: strings.TrimSpace(project), quota: get("quota_project_id")}
	if c.Project == "" {
		c.Project = get("project_id")
	}
	if c.Endpoint, err = normalizeEndpoint(get("endpoint"), DefaultEndpoint, "ENDPOINT"); err != nil {
		return nil, err
	}
	if c.Monitoring, err = normalizeEndpoint(get("monitoring_endpoint"), DefaultMonitoringEndpoint, "MONITORING_ENDPOINT"); err != nil {
		return nil, err
	}
	switch {
	case get("access_token") != "":
		c.tokens = oauth2.StaticTokenSource(&oauth2.Token{AccessToken: get("access_token"), TokenType: "Bearer"})
	case get("service_account_json") != "":
		keyJSON := get("service_account_json")
		ts, err := cachedSource("sa:"+keyJSON, func() (oauth2.TokenSource, error) {
			cfg, err := google.JWTConfigFromJSON([]byte(keyJSON), scopes...)
			if err != nil {
				return nil, fmt.Errorf("the gcp_pubsub secret's SERVICE_ACCOUNT_JSON is not a service account key file: %v", err)
			}
			return cfg.TokenSource(context.Background()), nil
		})
		if err != nil {
			return nil, err
		}
		if c.Project == "" {
			var k struct {
				ProjectID string `json:"project_id"`
			}
			_ = json.Unmarshal([]byte(keyJSON), &k)
			c.Project = k.ProjectID
		}
		c.tokens = ts
	case useADC:
		creds, err := google.FindDefaultCredentials(ctx, scopes...)
		if err != nil {
			return nil, fmt.Errorf("use_application_default_credentials: no Application Default Credentials found: %v", err)
		}
		if c.Project == "" {
			c.Project = creds.ProjectID
		}
		c.tokens = creds.TokenSource
	case c.Endpoint != DefaultEndpoint:
		// An emulator or a proxy that authenticates on its own: no token.
	case !have:
		return nil, errors.New("no gcp_pubsub secret found: create one with CREATE SECRET (TYPE gcp_pubsub, PROJECT_ID 'my-project', " +
			"SERVICE_ACCOUNT_JSON '<key file contents>') or ACCESS_TOKEN '<token>'; for the emulator use ENDPOINT 'localhost:8085'. " +
			"To use local gcloud/ADC credentials instead, ATTACH with use_application_default_credentials true")
	default:
		return nil, errors.New("the gcp_pubsub secret has no SERVICE_ACCOUNT_JSON or ACCESS_TOKEN (only the emulator, via ENDPOINT, works without credentials); " +
			"or ATTACH with use_application_default_credentials true to use local ADC")
	}
	if c.Project == "" {
		return nil, errors.New("no Google Cloud project: set PROJECT_ID in the gcp_pubsub secret or pass project := 'my-project'")
	}
	if !validProject(c.Project) {
		return nil, fmt.Errorf("invalid project id %q", c.Project)
	}
	return c, nil
}

// validProject accepts project ids (and domain-scoped ids like example.com:proj).
func validProject(p string) bool {
	if p == "" || len(p) > 100 {
		return false
	}
	for _, r := range p {
		ok := r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-' || r == '.' || r == ':' || r == '_'
		if !ok {
			return false
		}
	}
	return true
}

// apiError is a failed API call, carrying Google's canonical status.
type apiError struct {
	HTTP    int
	Status  string // e.g. PERMISSION_DENIED, NOT_FOUND, UNIMPLEMENTED
	Message string
	Call    string
}

func (e *apiError) Error() string {
	msg := fmt.Sprintf("%s: HTTP %d", e.Call, e.HTTP)
	if e.Status != "" {
		msg += " " + e.Status
	}
	if e.Message != "" {
		msg += ": " + e.Message
	}
	if e.HTTP == http.StatusForbidden || e.Status == "PERMISSION_DENIED" {
		msg += " (check the credential's IAM roles: roles/pubsub.viewer for metadata, roles/monitoring.viewer for metrics)"
	}
	return msg
}

// statusOf returns the canonical status of an API error, or "" for other errors.
func statusOf(err error) (int, string) {
	var ae *apiError
	if errors.As(err, &ae) {
		return ae.HTTP, ae.Status
	}
	return 0, ""
}

// client is shared so connections are reused.
var client = &http.Client{Timeout: Timeout}

// sleep is replaced in tests so retries don't wait.
var sleep = func(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// retryDelay honours a Retry-After header (seconds or HTTP date), else backs off exponentially.
func retryDelay(header string, attempt int) time.Duration {
	header = strings.TrimSpace(header)
	if s, err := strconv.Atoi(header); err == nil && s >= 0 {
		return min(time.Duration(s)*time.Second, 30*time.Second)
	}
	if t, err := http.ParseTime(header); err == nil {
		return min(max(time.Until(t), 0), 30*time.Second)
	}
	return time.Duration(250<<attempt) * time.Millisecond
}

func retryable(status int) bool {
	return status == http.StatusTooManyRequests || status == http.StatusInternalServerError ||
		status == http.StatusBadGateway || status == http.StatusServiceUnavailable || status == http.StatusGatewayTimeout
}

// encodeParams sorts keys so request URLs are deterministic (recorded
// fixtures key on them).
func encodeParams(params url.Values) string {
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		for _, v := range params[k] {
			if b.Len() > 0 {
				b.WriteByte('&')
			}
			b.WriteString(url.QueryEscape(k))
			b.WriteByte('=')
			b.WriteString(url.QueryEscape(v))
		}
	}
	return b.String()
}

// googleError is the JSON error envelope of Google REST APIs.
type googleError struct {
	Error struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Status  string `json:"status"`
	} `json:"error"`
}

// call performs one REST call against root+path, retrying rate-limited and
// unavailable responses, and decodes the JSON response into out (when non-nil).
// retry=false is used for non-idempotent calls (creating the temporary
// subscription), which are never repeated.
func (c *conn) call(ctx context.Context, method, root, path string, params url.Values, body any, out any, retry bool) error {
	target := root + path
	if q := encodeParams(params); q != "" {
		target += "?" + q
	}
	var payload []byte
	if body != nil {
		var err error
		if payload, err = json.Marshal(body); err != nil {
			return err
		}
	}
	name := method + " " + path
	for attempt := 1; ; attempt++ {
		var rd io.Reader
		if payload != nil {
			rd = bytes.NewReader(payload)
		}
		req, err := http.NewRequestWithContext(ctx, method, target, rd)
		if err != nil {
			return err
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", userAgent)
		if payload != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if c.quota != "" {
			req.Header.Set("x-goog-user-project", c.quota)
		}
		if c.tokens != nil {
			tok, err := c.tokens.Token()
			if err != nil {
				return fmt.Errorf("getting a Google access token: %w", scrubTokenError(err))
			}
			req.Header.Set("Authorization", "Bearer "+tok.AccessToken)
		}
		resp, err := client.Do(req)
		if err != nil {
			if !retry || attempt >= MaxAttempts || ctx.Err() != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
			sleep(ctx, retryDelay("", attempt))
			continue
		}
		data, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<30))
		_ = resp.Body.Close()
		if retry && retryable(resp.StatusCode) && attempt < MaxAttempts {
			slog.Debug("pubsub: retrying", "call", name, "status", resp.StatusCode, "attempt", attempt)
			sleep(ctx, retryDelay(resp.Header.Get("Retry-After"), attempt))
			continue
		}
		if readErr != nil {
			return fmt.Errorf("%s: reading response: %w", name, readErr)
		}
		if resp.StatusCode/100 != 2 {
			e := &apiError{HTTP: resp.StatusCode, Call: name}
			var ge googleError
			if json.Unmarshal(data, &ge) == nil && (ge.Error.Status != "" || ge.Error.Message != "") {
				e.Status, e.Message = ge.Error.Status, ge.Error.Message
			} else {
				snippet := strings.TrimSpace(string(data))
				if len(snippet) > 300 {
					snippet = snippet[:300]
				}
				e.Message = snippet
			}
			return e
		}
		if out == nil || len(bytes.TrimSpace(data)) == 0 {
			return nil
		}
		if err := json.Unmarshal(data, out); err != nil {
			return fmt.Errorf("%s: decoding response: %w", name, err)
		}
		return nil
	}
}

// scrubTokenError keeps token-endpoint failures readable without echoing
// response bodies that could contain credential material.
func scrubTokenError(err error) error {
	var re *oauth2.RetrieveError
	if errors.As(err, &re) {
		code := 0
		if re.Response != nil {
			code = re.Response.StatusCode
		}
		msg := re.ErrorCode
		if re.ErrorDescription != "" {
			msg += ": " + re.ErrorDescription
		}
		return fmt.Errorf("token endpoint returned HTTP %d %s", code, msg)
	}
	return err
}

// pubsub calls the Pub/Sub API (path starts with /v1/).
func (c *conn) pubsub(ctx context.Context, method, path string, params url.Values, body, out any) error {
	return c.call(ctx, method, c.Endpoint, path, params, body, out, true)
}

// projectPath is /v1/projects/<project>.
func (c *conn) projectPath() string { return "/v1/projects/" + url.PathEscape(c.Project) }
