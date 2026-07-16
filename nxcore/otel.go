package nxcore

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const (
	// rpcSystem is the value used for rpc.system on all Nexus spans/metrics
	// (OTel RPC semantic conventions).
	rpcSystem = "nexus"

	instrScope = "github.com/nayarsystems/nxgo"
)

// rpcInstruments holds the OTel metric instruments for outbound Nexus RPC.
// They are created lazily on first use so that otel.SetMeterProvider has
// already been called by the time the instruments are registered.
type rpcInstruments struct {
	duration metric.Float64Histogram // rpc.client.request.duration (ms)
	requests metric.Int64Counter     // rpc.client.requests
}

var (
	instruments     *rpcInstruments
	instrumentsOnce sync.Once
)

// getInstruments returns the singleton rpcInstruments, initialising them
// against the current global MeterProvider on the first call.
func getInstruments() *rpcInstruments {
	instrumentsOnce.Do(func() {
		meter := otel.GetMeterProvider().Meter(instrScope)

		dur, err := meter.Float64Histogram(
			"rpc.client.request.duration",
			metric.WithDescription("Duration of outbound Nexus JSON-RPC calls"),
			metric.WithUnit("ms"),
		)
		if err != nil {
			dur, _ = noop.NewMeterProvider().Meter("").Float64Histogram("")
		}

		req, err := meter.Int64Counter(
			"rpc.client.requests",
			metric.WithDescription("Total outbound Nexus JSON-RPC calls"),
		)
		if err != nil {
			req, _ = noop.NewMeterProvider().Meter("").Int64Counter("")
		}

		instruments = &rpcInstruments{duration: dur, requests: req}
	})
	return instruments
}

// recordRPCCall records duration and request count for one Nexus RPC call.
func recordRPCCall(ctx context.Context, start time.Time, method string, err error) {
	instr := getInstruments()
	durMs := float64(time.Since(start).Microseconds()) / 1000.0

	attrs := []attribute.KeyValue{
		attribute.String("rpc.system", rpcSystem),
		attribute.String("rpc.method", method),
	}
	if err != nil {
		attrs = append(attrs, attribute.String("error.type", nexusErrorType(err)))
	}

	opt := metric.WithAttributes(attrs...)
	instr.duration.Record(ctx, durMs, opt)
	instr.requests.Add(ctx, 1, opt)
}

// nexusErrorType returns a short string describing the Nexus/JSON-RPC error,
// suitable for use as the error.type metric attribute.
func nexusErrorType(err error) string {
	if rpcErr, ok := err.(*JsonRpcErr); ok {
		if s, ok := ErrStr[rpcErr.Cod]; ok {
			return s
		}
		return fmt.Sprintf("rpc_error_%d", rpcErr.Cod)
	}
	return "unknown"
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
	return otel.Tracer(instrScope).Start(ctx, method,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("rpc.system", rpcSystem),
			attribute.String("rpc.method", method),
		),
	)
}

// endClientSpan sets the span status based on err and ends it.
func endClientSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.SetAttributes(attribute.String("status", "error"))
	} else {
		span.SetAttributes(attribute.String("status", "ok"))
	}
	span.End()
}
