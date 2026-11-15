// A small framework for paged list functions: declare the arguments, the
// columns and a fetch that returns one page of rows of Go values plus the next
// page token; the shared code handles state, secrets, pagination and Arrow
// building.
//
// The cursor (the API's nextPageToken, or a function-specific cursor string)
// lives in exported state fields, so a long listing resumes correctly when
// DuckDB talks to the worker over HTTP and each page is a separate request.

package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

var (
	str     = arrow.BinaryTypes.String
	i64     = arrow.PrimitiveTypes.Int64
	f64     = arrow.PrimitiveTypes.Float64
	boolean = arrow.FixedWidthTypes.Boolean
	blob    = arrow.BinaryTypes.Binary
	tstz    = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	strMap  = arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String)
	strList = arrow.ListOf(arrow.BinaryTypes.String)
)

// ListState carries the bound arguments (JSON), the page cursor and whether
// the listing is finished. Credentials are never stored here.
type ListState struct {
	ArgsJSON string
	Cursor   string
	Done     bool
}

// page is one fetched page: rows plus the cursor for the next call ("" = done).
type page struct {
	Rows [][]any
	Next string
}

// listFunc is a paged table function with argument struct A.
type listFunc[A projected] struct {
	name     string
	meta     func() vgi.FunctionMetadata
	columns  []arrow.Field
	validate func(A) error
	fetch    func(ctx context.Context, c *conn, a A, cursor string) (page, error)
}

// projected is implemented by every argument struct: the secret name and the
// project override.
type projected interface {
	secretNamed
	project() string
}

func (f *listFunc[A]) Name() string                   { return f.name }
func (f *listFunc[A]) Metadata() vgi.FunctionMetadata { return f.meta() }
func (f *listFunc[A]) ArgumentSpecs() []vgi.ArgSpec   { var a A; return vgi.DeriveArgSpecs(a) }
func (f *listFunc[A]) schema() *arrow.Schema          { return arrow.NewSchema(f.columns, nil) }
func (f *listFunc[A]) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	var a A
	if err := vgi.BindArgs(params.Args, &a); err != nil {
		return nil, err
	}
	if f.validate != nil {
		if err := f.validate(a); err != nil {
			return nil, err
		}
	}
	return bindWithSecret[A](params, f.schema())
}

func (f *listFunc[A]) NewState(params *vgi.ProcessParams) (*ListState, error) {
	var a A
	if err := vgi.BindArgs(params.Args, &a); err != nil {
		return nil, err
	}
	if f.validate != nil {
		if err := f.validate(a); err != nil {
			return nil, err
		}
	}
	b, err := json.Marshal(a)
	if err != nil {
		return nil, err
	}
	return &ListState{ArgsJSON: string(b)}, nil
}

func (f *listFunc[A]) Process(ctx context.Context, params *vgi.ProcessParams, st *ListState, out *vgirpc.OutputCollector) error {
	if st.Done {
		return out.Finish()
	}
	var a A
	if err := json.Unmarshal([]byte(st.ArgsJSON), &a); err != nil {
		return err
	}
	c, err := resolveConn(ctx, params.Secrets, params.AttachOptions(), a.secret(), a.project())
	if err != nil {
		return err
	}
	// Skip empty pages so every Process call either emits rows or finishes.
	for {
		pg, err := f.fetch(ctx, c, a, st.Cursor)
		if err != nil {
			return err
		}
		st.Cursor = pg.Next
		st.Done = pg.Next == ""
		if len(pg.Rows) > 0 {
			return emitRows(out, f.columns, pg.Rows)
		}
		if st.Done {
			return out.Finish()
		}
	}
}

func newList[A projected](f *listFunc[A]) vgi.TableFunction {
	return vgi.AsTableFunction[ListState](f)
}

// emitRows builds and emits one batch (an empty page emits nothing).
func emitRows(out *vgirpc.OutputCollector, fields []arrow.Field, rows [][]any) error {
	if len(rows) == 0 {
		return nil
	}
	cols, err := buildGeneric(fields, rows)
	if err != nil {
		return err
	}
	for _, col := range cols {
		defer col.Release()
	}
	return out.EmitArrays(cols, int64(len(rows)))
}

// meta builds a function's metadata with result-column docs and examples.
func meta(title, category, keywords, docLLM, docMD, desc string, cols []arrow.Field, examples []vgi.CatalogExample) func() vgi.FunctionMetadata {
	return func() vgi.FunctionMetadata {
		tags := objectTags(docs{Title: title, Category: category, Keywords: keywords, DocLLM: docLLM, DocMD: docMD})
		rc := make([]resultColumn, len(cols))
		for i, f := range cols {
			d, _ := f.Metadata.GetValue("comment")
			rc[i] = resultColumn{Name: f.Name, Type: duckType(f.Type), Description: d}
		}
		tags["vgi.result_columns_schema"] = mustJSON(rc)
		return vgi.FunctionMetadata{
			Description:     desc,
			Stability:       vgi.StabilityVolatile,
			Tags:            withExamples(tags, examples),
			RequiredSecrets: secretRequirements,
			Examples:        examples,
		}
	}
}

func duckType(t arrow.DataType) string {
	switch t.ID() {
	case arrow.STRING:
		return "VARCHAR"
	case arrow.BINARY:
		return "BLOB"
	case arrow.INT64:
		return "BIGINT"
	case arrow.FLOAT64:
		return "DOUBLE"
	case arrow.BOOL:
		return "BOOLEAN"
	case arrow.TIMESTAMP:
		return "TIMESTAMPTZ"
	case arrow.MAP:
		return "MAP(VARCHAR, VARCHAR)"
	case arrow.LIST:
		return "VARCHAR[]"
	}
	return t.String()
}

// ---------------------------------------------------------------- values

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return strings.TrimSpace(*s)
}

// optString is NULL for "".
func optString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// optTime parses an RFC 3339 time; "" and the zero time are NULL.
func optTime(s string) any {
	if s == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil || t.Year() <= 1970 && t.Unix() <= 0 {
		return nil
	}
	return t
}

// optBool is NULL for a missing field.
func optBool(b *bool) any {
	if b == nil {
		return nil
	}
	return *b
}

// optInt is NULL for a missing field.
func optInt(i *int64) any {
	if i == nil {
		return nil
	}
	return *i
}

// labelsOf returns a non-nil map (an empty MAP, never NULL).
func labelsOf(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}

// listOf returns a non-nil list.
func listOf(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// rawJSON is NULL for absent/empty JSON, else the compact text.
func rawJSON(r json.RawMessage) any {
	s := strings.TrimSpace(string(r))
	if s == "" || s == "null" || s == "{}" || s == "[]" {
		return nil
	}
	return s
}

// buildGeneric builds columns from rows of Go values. Supported values per
// column type: string; []byte; int64; float64; bool; time.Time;
// map[string]string; []string; nil for NULL.
func buildGeneric(fields []arrow.Field, rows [][]any) ([]arrow.Array, error) {
	mem := memory.NewGoAllocator()
	out := make([]arrow.Array, 0, len(fields))
	release := func() {
		for _, a := range out {
			a.Release()
		}
	}
	for ci, fd := range fields {
		b := array.NewBuilder(mem, fd.Type)
		for ri, r := range rows {
			v := r[ci]
			if v == nil {
				if !fd.Nullable {
					b.Release()
					release()
					return nil, fmt.Errorf("internal: NULL in non-null column %s (row %d)", fd.Name, ri)
				}
				b.AppendNull()
				continue
			}
			if err := appendGeneric(b, v); err != nil {
				b.Release()
				release()
				return nil, fmt.Errorf("column %s: %w", fd.Name, err)
			}
		}
		out = append(out, b.NewArray())
		b.Release()
	}
	return out, nil
}

func appendGeneric(b array.Builder, v any) error {
	switch bb := b.(type) {
	case *array.StringBuilder:
		s, ok := v.(string)
		if !ok {
			return fmt.Errorf("want string, got %T", v)
		}
		bb.Append(s)
	case *array.BinaryBuilder:
		s, ok := v.([]byte)
		if !ok {
			return fmt.Errorf("want []byte, got %T", v)
		}
		bb.Append(s)
	case *array.Int64Builder:
		i, ok := v.(int64)
		if !ok {
			return fmt.Errorf("want int64, got %T", v)
		}
		bb.Append(i)
	case *array.Float64Builder:
		f, ok := v.(float64)
		if !ok {
			return fmt.Errorf("want float64, got %T", v)
		}
		bb.Append(f)
	case *array.BooleanBuilder:
		x, ok := v.(bool)
		if !ok {
			return fmt.Errorf("want bool, got %T", v)
		}
		bb.Append(x)
	case *array.TimestampBuilder:
		t, ok := v.(time.Time)
		if !ok {
			return fmt.Errorf("want time, got %T", v)
		}
		bb.Append(arrow.Timestamp(t.UnixMicro()))
	case *array.ListBuilder:
		l, ok := v.([]string)
		if !ok {
			return fmt.Errorf("want []string, got %T", v)
		}
		bb.Append(true)
		vb := bb.ValueBuilder().(*array.StringBuilder)
		for _, s := range l {
			vb.Append(s)
		}
	case *array.MapBuilder:
		m, ok := v.(map[string]string)
		if !ok {
			return fmt.Errorf("want map, got %T", v)
		}
		bb.Append(true)
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		kb := bb.KeyBuilder().(*array.StringBuilder)
		ib := bb.ItemBuilder().(*array.StringBuilder)
		for _, k := range keys {
			kb.Append(k)
			ib.Append(m[k])
		}
	default:
		return fmt.Errorf("unsupported builder %T", b)
	}
	return nil
}
