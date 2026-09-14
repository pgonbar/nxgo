package nxcore

import (
	"context"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/jaracil/ei"
	"github.com/nayarsystems/nxgo/nxotel"
)

var rpcOtel = nxotel.New("github.com/nayarsystems/nxgo", nxotel.Client, nexusErrorType)

// recordRPCCall records metrics for one Nexus RPC call.
func recordRPCCall(ctx context.Context, start time.Time, method string, err error, extra ...attribute.KeyValue) {
	rpcOtel.RecordCall(ctx, start, method, "", err, extra...)
}

// nexusErrorType returns a short string describing the Nexus/JSON-RPC error.
func nexusErrorType(err error) string {
	return nxotel.ErrorTypeFromCode(err, ErrStr, "unknown")
}

// injectTraceparent injects the W3C traceparent from ctx into
// params["@metadata"]["traceparent"]
//
// Returns the (possibly cloned+enriched) params value. If ctx has no active
// span, or if params is not a map type, the original value is returned unchanged.
// Map params are normalized through ei so named map types (ei.M and friends)
// are handled too: a raw type switch on map[string]interface{} would silently
// skip them.
func injectTraceparent(ctx context.Context, params interface{}) interface{} {
	if !trace.SpanFromContext(ctx).SpanContext().IsValid() {
		return params
	}

	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	tp := carrier["traceparent"]
	if tp == "" {
		return params
	}

	// Build a copy of the params map so we never mutate the caller's value.
	var pm map[string]interface{}
	if params == nil {
		pm = map[string]interface{}{}
	} else {
		var err error
		if pm, err = ei.N(params).MapStr(); err != nil {
			return params // non-map params: skip silently
		}
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
func startClientSpan(ctx context.Context, method string, extra ...attribute.KeyValue) (context.Context, trace.Span) {
	return rpcOtel.StartSpan(ctx, method, "", extra...)
}

// serviceFromMethod derives the Nexus service path from a task method by
// cutting at the last dot and keeping the trailing dot, matching the path
// convention of the Nexus task table ("a.b.c" -> "a.b."). Returns "" when the
// method has no dot.
func serviceFromMethod(method string) string {
	i := strings.LastIndex(method, ".")
	if i < 0 {
		return ""
	}
	return method[:i+1]
}

// endClientSpan sets the span status based on err and ends it.
func endClientSpan(span trace.Span, err error) {
	rpcOtel.EndSpan(span, err)
}
