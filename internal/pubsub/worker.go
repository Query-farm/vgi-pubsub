// Package pubsub exposes Google Cloud Pub/Sub (and its Cloud Monitoring metrics) as a VGI catalog.
//
// To add a function: write it (see resources.go for a paged listing),
// then register it in NewWorker below.
//
// Run it:
//
//	vgi-pubsub-worker                      # stdio: what DuckDB launches by default
//	vgi-pubsub-worker --http --port 8000   # HTTP: for Docker / hosted deploys
//	vgi-pubsub-worker --help
//
// Attach it from DuckDB:
//
//	ATTACH 'pubsub' (TYPE vgi, LOCATION '/abs/path/to/bin/vgi-pubsub-worker');
package pubsub

import (
	"github.com/Query-farm/vgi-go/vgi"
)

// allExamples lists every function's examples for the schema-level tag.
func allExamples() []exampleQuery {
	return exampleQueries(topicsExamples, subscriptionsExamples, snapshotsExamples, schemasExamples, schemaRevisionsExamples,
		iamExamples, timeSeriesExamples, backlogExamples, messagesExamples, accessExamples)
}

// NewWorker builds the worker: one catalog, one main schema. Extra options
// (logging, storage) are appended after the catalog options.
func NewWorker(opts ...vgi.WorkerOption) *vgi.Worker {
	sourceURL, version := SourceURL, Version
	base := []vgi.WorkerOption{
		vgi.WithCatalogName(CatalogName),
		vgi.WithCatalogComment(CatalogDocMD),
		vgi.WithCatalogTags(CatalogTags),
		vgi.WithCatalogInfo(vgi.CatalogInfo{SourceURL: &sourceURL, ImplementationVersion: &version}),
		vgi.WithSchemaComments(map[string]string{"main": SchemaDocMD}),
		vgi.WithSchemaTags(map[string]map[string]string{"main": schemaTags(allExamples())}),
		vgi.WithSecretTypes(APISecretType),
		vgi.WithAttachOptions(AttachOptionSpecs...),
	}
	w := vgi.NewWorker(append(base, opts...)...)
	for _, f := range []vgi.TableFunction{newTopics(), newSubscriptions(), newSnapshots(), newSchemas(), newSchemaRevisions(),
		newIAMPolicies(), newTimeSeries(), newSubscriptionBacklog(), newReadMessages(), newAccessCheck()} {
		w.RegisterTable(f)
	}
	return w
}
