package rpc

import (
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

var (
	// taken from ocgrpc (https://github.com/census-instrumentation/opencensus-go/blob/master/plugin/ocgrpc/stats_common.go)
	latencyDistribution = view.Distribution(0, 0.01, 0.05, 0.1, 0.3, 0.6, 0.8, 1, 2, 3, 4, 5, 6, 8, 10, 13, 16, 20, 25, 30, 40, 50, 65, 80, 100, 130, 160, 200, 250, 300, 400, 500, 650, 800, 1000, 2000, 5000, 10000, 20000, 50000, 100000)

	bytesDistribution = view.Distribution(0, 24, 32, 64, 128, 256, 512, 1024, 2048, 4096, 16384, 65536, 262144, 1048576)
)

// opencensus keys, equivalent to prometheus tags
var (
	// ServiceKey is a metrics tag for which RPC service was hit.
	ServiceKey = makeKey("service")
	// MethodKey is a metrics tag for which RPC method was hit.
	MethodKey = makeKey("method")
)

// opencensus metrics
var (
	// RequestCountMetric is the number times a RPC request has been made.
	RequestCountMetric = stats.Int64("libp2p_gorpc/request_count", "Number of requests", stats.UnitDimensionless)
	// RequestLatencyMetric is how long a RPC request took to complete.
	RequestLatencyMetric = stats.Float64("libp2p_gorpc/request_latency", "Latency of RPC request", stats.UnitMilliseconds)
)

// opencensus views, which is just the aggregation of the metrics
var (
	RequestCountView = &view.View{
		Measure:     RequestCountMetric,
		TagKeys:     []tag.Key{ServiceKey, MethodKey},
		Aggregation: view.Sum(),
	}

	RequestLatencyView = &view.View{
		Name:        "libp2p_gorpc/request_latency",
		Measure:     RequestLatencyMetric,
		TagKeys:     []tag.Key{ServiceKey, MethodKey},
		Aggregation: latencyDistribution,
	}

	DefaultViews = []*view.View{
		RequestCountView,
		RequestLatencyView,
	}
)

func makeKey(name string) tag.Key {
	key, err := tag.NewKey(name)
	if err != nil {
		logger.Fatal(err)
	}
	return key
}
