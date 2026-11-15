// Offline helpers: scalars that need no secret and no network. They join
// Cloud Monitoring ids to resource names and decode the API's duration text,
// and carry the worker's executable examples.
//
//	SELECT pubsub.main.resource_id('projects/p/subscriptions/orders-worker');      -- orders-worker
//	SELECT pubsub.main.resource_project('projects/p/subscriptions/orders-worker'); -- p
//	SELECT pubsub.main.duration_seconds('604800s');                                -- 604800.0

package pubsub

import (
	"context"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type nameArgs struct {
	Name *array.String `vgi:"pos=0,const=false,doc=A Pub/Sub resource name such as 'projects/my-project/topics/orders'. NULL gives NULL."`
}

type durationArgs struct {
	Duration *array.String `vgi:"pos=0,const=false,doc=A google.protobuf.Duration JSON value such as '604800s' or '1.5s'. NULL or invalid text gives NULL."`
}

// executableExample is one vgi.executable_examples entry.
type executableExample struct {
	Description string  `json:"description"`
	SQL         string  `json:"sql"`
	Expected    [][]any `json:"expected_result"`
}

// textScalar is a vectorized one-column scalar over text.
type textScalar struct {
	name       string
	docs       docs
	desc       string
	examples   []vgi.CatalogExample
	executable []executableExample
	result     arrow.DataType
	eval       func(b array.Builder, s string)
}

func (f *textScalar) Name() string { return f.name }

func (f *textScalar) Metadata() vgi.FunctionMetadata {
	tags := withExamples(objectTags(f.docs), f.examples)
	tags["vgi.executable_examples"] = mustJSON(f.executable)
	return vgi.FunctionMetadata{Description: f.desc, Stability: vgi.StabilityConsistent, Tags: tags, Examples: f.examples}
}

func (f *textScalar) process(batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	col := batch.Column(0)
	b := array.NewBuilder(memory.NewGoAllocator(), f.result)
	defer b.Release()
	for i := 0; i < int(batch.NumRows()); i++ {
		if col.IsNull(i) {
			b.AppendNull()
			continue
		}
		f.eval(b, vgi.GetStringValue(col, i))
	}
	arr := b.NewArray()
	defer arr.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "result", Type: f.result, Nullable: true}}, nil)
	return array.NewRecordBatch(schema, []arrow.Array{arr}, batch.NumRows()), nil
}

type nameScalar struct{ textScalar }

func (f *nameScalar) OnBindTyped(*nameArgs, *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(f.result)
}

func (f *nameScalar) ProcessTyped(_ context.Context, _ *nameArgs, _ *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	return f.process(batch)
}

type durationScalar struct{ textScalar }

func (f *durationScalar) OnBindTyped(*durationArgs, *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(f.result)
}

func (f *durationScalar) ProcessTyped(_ context.Context, _ *durationArgs, _ *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	return f.process(batch)
}

var resourceIDExamples = []vgi.CatalogExample{
	{SQL: "SELECT pubsub.main.resource_id(name) AS subscription, topic FROM pubsub.main.subscriptions() ORDER BY subscription",
		Description: "Short subscription ids next to their topic"},
}

var resourceProjectExamples = []vgi.CatalogExample{
	{SQL: "SELECT pubsub.main.resource_project(topic) AS topic_project, count(*) AS subscriptions FROM pubsub.main.subscriptions() GROUP BY ALL ORDER BY topic_project",
		Description: "Subscriptions per project of their topic (cross-project subscriptions show up here)"},
}

var durationExamples = []vgi.CatalogExample{
	{SQL: "SELECT pubsub.main.duration_seconds(d) / 86400 AS days FROM (VALUES ('604800s'), ('86400s')) t(d) ORDER BY days",
		Description: "Convert API duration text to days"},
}

func newTools() []vgi.ScalarFunction {
	resourceID := &nameScalar{textScalar{
		name: "resource_id", result: arrow.BinaryTypes.String,
		docs: docs{Title: "Resource Id", Category: "tools", Keywords: "resource name, id, short name, join, subscription_id, topic_id",
			DocLLM: "resource_id(name `VARCHAR`) -> `VARCHAR`: the last segment of a Pub/Sub resource name ('projects/p/topics/orders' -> " +
				"'orders'); text without slashes is returned unchanged. Use it to join name columns to Cloud Monitoring ids " +
				"(time_series.subscription / topic hold ids). Offline, no secret.",
			DocMD: "## resource_id(name)\n\nThe id at the end of a resource name: `projects/p/topics/orders` gives `orders`."},
		desc:     "The id (last segment) of a Pub/Sub resource name",
		examples: resourceIDExamples,
		executable: []executableExample{{Description: "Ids of a topic and a subscription name",
			SQL:      "SELECT pubsub.main.resource_id(n) AS id FROM (VALUES ('projects/p/topics/orders'), ('projects/p/subscriptions/orders-worker')) t(n) ORDER BY id",
			Expected: [][]any{{"orders"}, {"orders-worker"}}}},
		eval: func(b array.Builder, s string) { b.(*array.StringBuilder).Append(shortID(s)) },
	}}
	resourceProject := &nameScalar{textScalar{
		name: "resource_project", result: arrow.BinaryTypes.String,
		docs: docs{Title: "Resource Project", Category: "tools", Keywords: "resource name, project, cross-project, shared topic",
			DocLLM: "resource_project(name `VARCHAR`) -> `VARCHAR`: the project of a resource name ('projects/p/topics/orders' -> 'p'); " +
				"NULL when the text is not a projects/... name. Use it to spot subscriptions on topics in another project. Offline, no secret.",
			DocMD: "## resource_project(name)\n\nThe project segment of a resource name, or NULL."},
		desc:     "The project of a Pub/Sub resource name",
		examples: resourceProjectExamples,
		executable: []executableExample{{Description: "Project of a topic name, and NULL for a bare id",
			SQL:      "SELECT n, pubsub.main.resource_project(n) AS project FROM (VALUES ('projects/shared-infra/topics/events'), ('events')) t(n) ORDER BY n",
			Expected: [][]any{{"events", nil}, {"projects/shared-infra/topics/events", "shared-infra"}}}},
		eval: func(b array.Builder, s string) {
			if p := projectOf(strings.TrimSpace(s)); p != "" {
				b.(*array.StringBuilder).Append(p)
				return
			}
			b.AppendNull()
		},
	}}
	duration := &durationScalar{textScalar{
		name: "duration_seconds", result: arrow.PrimitiveTypes.Float64,
		docs: docs{Title: "Duration Seconds", Category: "tools", Keywords: "duration, seconds, retention, ack deadline, protobuf duration",
			DocLLM: "duration_seconds(duration `VARCHAR`) -> `DOUBLE`: parse a google.protobuf.Duration JSON value ('604800s', '1.5s') " +
				"into seconds; NULL for NULL or invalid text. The table functions already return durations as `DOUBLE` seconds; use this " +
				"on durations found inside JSON columns such as ingestion_settings. Offline, no secret.",
			DocMD: "## duration_seconds(duration)\n\n`'604800s'` gives `604800.0`."},
		desc:     "Seconds in a google.protobuf.Duration text value such as '604800s'",
		examples: durationExamples,
		executable: []executableExample{{Description: "A week, a fraction and an invalid value",
			SQL:      "SELECT d, pubsub.main.duration_seconds(d) AS seconds FROM (VALUES ('604800s'), ('1.5s'), ('soon')) t(d) ORDER BY d",
			Expected: [][]any{{"1.5s", "1.5"}, {"604800s", "604800.0"}, {"soon", nil}}}},
		eval: func(b array.Builder, s string) {
			if v, ok := durationSeconds(s).(float64); ok {
				b.(*array.Float64Builder).Append(v)
				return
			}
			b.AppendNull()
		},
	}}
	return []vgi.ScalarFunction{
		vgi.AsScalarFunction[nameArgs](resourceID),
		vgi.AsScalarFunction[nameArgs](resourceProject),
		vgi.AsScalarFunction[durationArgs](duration),
	}
}
