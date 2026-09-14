package nxcore

import (
	"context"
	"testing"

	"github.com/jaracil/ei"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// TestInjectTraceparent guards the params normalization: named map types
// (ei.M) must be enriched too, not only raw map[string]interface{}.
// Regression test for send_daily/send_failed_events arriving without
// traceparent: the nxsugar wrapper built nil-params metadata as ei.M, which a
// raw type switch silently skipped.
func TestInjectTraceparent(t *testing.T) {
	origProp := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	defer otel.SetTextMapPropagator(origProp)

	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	ctx, span := tp.Tracer("test").Start(context.Background(), "op")
	defer span.End()

	// Named map type (ei.M): must carry traceparent afterwards.
	params := ei.M{"foo": "bar"}
	out := injectTraceparent(ctx, params)
	om, err := ei.N(out).MapStr()
	if err != nil {
		t.Fatalf("expected map, got %T: %v", out, err)
	}
	if v := ei.N(om).M("@metadata").M("traceparent").StringZ(); v == "" {
		t.Fatal("traceparent not injected into ei.M params")
	}
	if om["foo"] != "bar" {
		t.Fatal("original keys lost")
	}
	if _, mutated := params["@metadata"]; mutated {
		t.Fatal("caller params were mutated")
	}

	// Raw map type: enriched as before.
	out = injectTraceparent(ctx, map[string]interface{}{"a": 1})
	om, err = ei.N(out).MapStr()
	if err != nil {
		t.Fatalf("expected map, got %T: %v", out, err)
	}
	if v := ei.N(om).M("@metadata").M("traceparent").StringZ(); v == "" {
		t.Fatal("traceparent not injected into map params")
	}

	// nil params: a new map carrying the traceparent.
	out = injectTraceparent(ctx, nil)
	om, err = ei.N(out).MapStr()
	if err != nil {
		t.Fatalf("expected map, got %T: %v", out, err)
	}
	if v := ei.N(om).M("@metadata").M("traceparent").StringZ(); v == "" {
		t.Fatal("traceparent not injected into nil params")
	}

	// Non-map params: returned unchanged.
	s := "not-a-map"
	if got := injectTraceparent(ctx, s); got != s {
		t.Fatalf("non-map params changed: %v", got)
	}

	// Context without an active span: params returned unchanged (no @metadata added).
	plain := ei.M{"a": 1}
	got := injectTraceparent(context.Background(), plain)
	if gm, err := ei.N(got).MapStr(); err == nil {
		if _, ok := gm["@metadata"]; ok {
			t.Fatal("spanless ctx must not alter params")
		}
	} else {
		t.Fatalf("expected map, got %T: %v", got, err)
	}
}
