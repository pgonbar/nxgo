// Package nxotel provides shared OpenTelemetry helpers for Nexus RPC
// instrumentation. Both the client side and the server side
// use it so metric names, span attributes and status handling
// stay consistent across the whole Nexus ecosystem.
package nxotel

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
)

// Side indicates which side of the RPC the instrumentation belongs to.
type Side string

const (
	// Server side (inbound task processing).
	Server Side = "server"
	// Client side (outbound JSON-RPC calls).
	Client Side = "client"
)

// rpcSystem is the value used for rpc.system on all Nexus spans/metrics.
const rpcSystem = "nexus"

// RPCError is satisfied by the JsonRpcErr types which expose Code() int.
type RPCError interface {
	Code() int
}

// ErrorTyper maps an error to a low-cardinality string suitable for the
// error.type metric attribute.
type ErrorTyper func(err error) string

// instruments holds the OTel metric instruments. They are created lazily on
// first use so that otel.SetMeterProvider has already been called by the
// application before the instruments are registered.
type instruments struct {
	duration metric.Float64Histogram
	requests metric.Int64Counter
}

// Otel encapsulates the OTel instrumentation for one RPC side.
type Otel struct {
	scope      string
	side       Side
	errorTyper ErrorTyper
	instrs     *instruments
	instrsOnce sync.Once
}

// New creates an Otel instrumenter for the given scope and side. A nil
// errorTyper falls back to a generic one that classifies errors by code.
func New(scope string, side Side, errorTyper ErrorTyper) *Otel {
	if errorTyper == nil {
		errorTyper = func(err error) string {
			return ErrorTypeFromCode(err, nil, "unknown")
		}
	}
	return &Otel{scope: scope, side: side, errorTyper: errorTyper}
}

func (o *Otel) durationName() string {
	return "rpc." + string(o.side) + ".duration"
}

func (o *Otel) counterName() string {
	return "rpc." + string(o.side) + ".requests"
}

func (o *Otel) getInstruments() *instruments {
	o.instrsOnce.Do(func() {
		meter := otel.GetMeterProvider().Meter(o.scope)

		dur, err := meter.Float64Histogram(
			o.durationName(),
			metric.WithDescription(fmt.Sprintf("Duration of %s Nexus RPC calls", o.side)),
			metric.WithUnit("ms"),
		)
		if err != nil {
			dur, _ = noop.NewMeterProvider().Meter("").Float64Histogram("")
			otel.Handle(fmt.Errorf("nxotel: failed to create %s histogram: %w", o.durationName(), err))
		}

		req, err := meter.Int64Counter(
			o.counterName(),
			metric.WithDescription(fmt.Sprintf("Total %s Nexus RPC calls", o.side)),
		)
		if err != nil {
			req, _ = noop.NewMeterProvider().Meter("").Int64Counter("")
			otel.Handle(fmt.Errorf("nxotel: failed to create %s counter: %w", o.counterName(), err))
		}

		o.instrs = &instruments{duration: dur, requests: req}
	})
	return o.instrs
}

// StartSpan starts an RPC span. service is the Nexus path for the server side
// and may be empty on the client side (the client does not know the remote
// service path).
func (o *Otel) StartSpan(ctx context.Context, method, service string) (context.Context, trace.Span) {
	kind := trace.SpanKindClient
	if o.side == Server {
		kind = trace.SpanKindServer
	}

	attrs := []attribute.KeyValue{
		attribute.String("rpc.system", rpcSystem),
		attribute.String("rpc.method", method),
	}
	if service != "" {
		attrs = append(attrs, attribute.String("rpc.service", service))
	}

	return otel.Tracer(o.scope).Start(ctx, method,
		trace.WithSpanKind(kind),
		trace.WithAttributes(attrs...),
	)
}

// EndSpan sets the span status based on err and ends it. Successful spans are
// left with an Unset status (per OTel spec Unset already means "completed
// without error"); only errors set an explicit Error status.
func (o *Otel) EndSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}

// RecordCall records duration and request count for one RPC call.
func (o *Otel) RecordCall(ctx context.Context, start time.Time, method, service string, err error) {
	instr := o.getInstruments()
	durMs := float64(time.Since(start).Microseconds()) / 1000.0

	attrs := []attribute.KeyValue{
		attribute.String("rpc.system", rpcSystem),
		attribute.String("rpc.method", method),
	}
	if service != "" {
		attrs = append(attrs, attribute.String("rpc.service", service))
	}
	if err != nil {
		attrs = append(attrs, attribute.String("error.type", o.errorTyper(err)))
	}

	opt := metric.WithAttributes(attrs...)
	instr.duration.Record(ctx, durMs, opt)
	instr.requests.Add(ctx, 1, opt)
}

// ErrorTypeFromCode builds a default error.type string from a numeric code,
// honouring an optional ErrStr-like map. It is used by module-provided
// ErrorTyper implementations.
func ErrorTypeFromCode(err error, errStr map[int]string, unknown string) string {
	var rpcErr RPCError
	if errors.As(err, &rpcErr) {
		if s, ok := errStr[rpcErr.Code()]; ok {
			return s
		}
		return fmt.Sprintf("rpc_error_%d", rpcErr.Code())
	}
	return unknown
}
