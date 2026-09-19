package middleware

import (
	"sync/atomic"
)

// Request counters for the /api/v1/metrics snapshot. They live in the
// middleware package (not api/v1) because the Logging middleware is the
// instrumentation point and api/v1 imports middleware — keeping them here
// avoids an import cycle. Counters are atomic: no lock on the hot path.
var (
	metricRequests2xx   atomic.Uint64
	metricRequests4xx   atomic.Uint64
	metricRequests5xx   atomic.Uint64
	metricRequestsOther atomic.Uint64
)

// RecordRequestStatus is called by the Logging middleware for every finished
// request.
func RecordRequestStatus(status int) {
	switch {
	case status >= 200 && status < 300:
		metricRequests2xx.Add(1)
	case status >= 400 && status < 500:
		metricRequests4xx.Add(1)
	case status >= 500:
		metricRequests5xx.Add(1)
	default:
		metricRequestsOther.Add(1)
	}
}

// SnapshotRequests returns the current counter values for the metrics
// endpoint.
func SnapshotRequests() (ok2xx, other1xx3xx, clientErr4xx, serverErr5xx uint64) {
	return metricRequests2xx.Load(),
		metricRequestsOther.Load(),
		metricRequests4xx.Load(),
		metricRequests5xx.Load()
}
