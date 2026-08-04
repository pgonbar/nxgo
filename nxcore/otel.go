package nxcore

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/nayarsystems/nxgo/nxotel"
)

var rpcOtel = nxotel.New("github.com/nayarsystems/nxgo", nxotel.Client, nexusErrorType)

// recordRPCCall records duration and request count for one Nexus RPC call.
func recordRPCCall(ctx context.Context, start time.Time, method string, err error) {
	rpcOtel.RecordCall(ctx, start, method, "", err)
}

// nexusErrorType returns a short string describing the Nexus/JSON-RPC error,
// suitable for use as the error.type metric attribute.
func nexusErrorType(err error) string {
	return nxotel.ErrorTypeFromCode(err, ErrStr, "unknown")
}

// injectTraceparent injects the W3C traceparent from ctx into
// params["@metadata"]["traceparent"], following the same @metadata convention
// nxsugar already uses for trackid.
//
// Returns the (possibly cloned+enriched) params value. If ctx has no active
// span, or if params is not a map type, the original value is returned unchanged.
func injectTraceparent(ctx context.Context, params interface{}) interface{} {
	if !trace.SpanFromContext(ctx).SpanContext().IsValid() {
		return params // no active span — nothing to inject
	}

	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	tp := carrier["traceparent"]
	if tp == "" {
		return params
	}

	// Build a copy of the params map so we never mutate the caller's value.
	var pm map[string]interface{}
	switch v := params.(type) {
	case map[string]interface{}:
		pm = v
	case nil:
		pm = map[string]interface{}{}
	default:
		return params // non-map params: skip silently
	}

	cloned := make(map[string]interface{}, len(pm)+1)
	for k, v := range pm {
		cloned[k] = v
	}

	md, _ := cloned["@metadata"].(map[string]interface{})
	if md == nil {
		md = map[string]interface{}{}
	} else {
		mdCloned := make(map[string]interface{}, len(md)+1)
		for k, v := range md {
			mdCloned[k] = v
		}
		md = mdCloned
	}
	md["traceparent"] = tp
	cloned["@metadata"] = md

	return cloned
}

// startClientSpan starts an OTel client span for a Nexus RPC call.
func startClientSpan(ctx context.Context, method string) (context.Context, trace.Span) {
	return rpcOtel.StartSpan(ctx, method, "")
}

// endClientSpan sets the span status based on err and ends it.
func endClientSpan(span trace.Span, err error) {
	rpcOtel.EndSpan(span, err)
}
