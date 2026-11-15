// Command vgi-pubsub-worker serves the pubsub catalog to DuckDB.
//
//	vgi-pubsub-worker                                  # stdio (what DuckDB launches)
//	vgi-pubsub-worker --http --host 0.0.0.0 --port 8000  # HTTP, GET /health
//	vgi-pubsub-worker --unix /tmp/pubsub.sock         # AF_UNIX (launch: transport)
//
// Attach it from DuckDB:
//
//	ATTACH 'pubsub' (TYPE vgi, LOCATION '/abs/path/to/bin/vgi-pubsub-worker');
package main

import (
	"os"

	"github.com/Query-farm/vgi-go/vgi"

	"github.com/Query-farm/vgi-pubsub/internal/pubsub"
	"github.com/Query-farm/vgi-pubsub/internal/serve"
)

func main() {
	serve.Main("vgi-pubsub-worker", os.Args[1:], func() *vgi.Worker { return pubsub.NewWorker() })
}
