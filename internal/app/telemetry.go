package app

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type Telemetry struct {
	Tracer    trace.Tracer
	Logger    otellog.Logger
	Counter   metric.Int64Counter
	Duration  metric.Float64Histogram
	Native    metric.Float64Gauge
	tp        *sdktrace.TracerProvider
	lp        *sdklog.LoggerProvider
	mp        *sdkmetric.MeterProvider
	mu        sync.Mutex
	lastError string
	Enabled   bool
}

func NewTelemetry(ctx context.Context, c Config) (*Telemetry, error) {
	attrs := []attribute.KeyValue{
		attribute.String("service.name", c.Service), attribute.String("service.namespace", c.Namespace),
		attribute.String("service.version", c.Version), attribute.String("deployment.environment.name", c.Environment)}
	if c.Mode == "cloud" {
		attrs = append(attrs, attribute.String("cloud.provider", "aws"), attribute.String("cloud.region", c.Region), attribute.String("cloud.account.id", os.Getenv("AWS_ACCOUNT_ID")), attribute.String("faas.name", os.Getenv("AWS_LAMBDA_FUNCTION_NAME")), attribute.String("faas.instance", os.Getenv("AWS_LAMBDA_LOG_STREAM_NAME")))
	} else {
		attrs = append(attrs, attribute.String("sample.runtime", "local"))
	}
	r := resource.NewWithAttributes("https://opentelemetry.io/schemas/1.40.0", attrs...)
	t := &Telemetry{Enabled: c.IngestKey != ""}
	traceOptions := []sdktrace.TracerProviderOption{sdktrace.WithResource(r), sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample()))}
	logOptions := []sdklog.LoggerProviderOption{sdklog.WithResource(r)}
	metricOptions := []sdkmetric.Option{sdkmetric.WithResource(r)}
	if t.Enabled {
		headers := map[string]string{"X-CORRIDORA-KEY": c.IngestKey}
		te, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(c.Endpoint+"/v1/traces"), otlptracehttp.WithHeaders(headers), otlptracehttp.WithTimeout(time.Second), otlptracehttp.WithRetry(otlptracehttp.RetryConfig{Enabled: false}))
		if err != nil {
			return nil, err
		}
		le, err := otlploghttp.New(ctx, otlploghttp.WithEndpointURL(c.Endpoint+"/v1/logs"), otlploghttp.WithHeaders(headers), otlploghttp.WithTimeout(time.Second), otlploghttp.WithRetry(otlploghttp.RetryConfig{Enabled: false}))
		if err != nil {
			return nil, err
		}
		me, err := otlpmetrichttp.New(ctx, otlpmetrichttp.WithEndpointURL(c.Endpoint+"/v1/metrics"), otlpmetrichttp.WithHeaders(headers), otlpmetrichttp.WithTimeout(time.Second), otlpmetrichttp.WithRetry(otlpmetrichttp.RetryConfig{Enabled: false}))
		if err != nil {
			return nil, err
		}
		traceOptions = append(traceOptions, sdktrace.WithBatcher(te, sdktrace.WithBatchTimeout(time.Second), sdktrace.WithMaxQueueSize(256)))
		logOptions = append(logOptions, sdklog.WithProcessor(sdklog.NewBatchProcessor(le, sdklog.WithExportInterval(time.Second), sdklog.WithMaxQueueSize(256))))
		metricOptions = append(metricOptions, sdkmetric.WithReader(sdkmetric.NewPeriodicReader(me, sdkmetric.WithInterval(30*time.Second), sdkmetric.WithTimeout(time.Second))))
	}
	t.tp = sdktrace.NewTracerProvider(traceOptions...)
	t.lp = sdklog.NewLoggerProvider(logOptions...)
	t.mp = sdkmetric.NewMeterProvider(metricOptions...)
	t.Tracer = t.tp.Tracer("github.com/corridora/sample-reviews")
	t.Logger = t.lp.Logger("sample-reviews")
	meter := t.mp.Meter("sample-reviews")
	t.Counter, _ = meter.Int64Counter("reviews.operations")
	t.Duration, _ = meter.Float64Histogram("reviews.operation.duration", metric.WithUnit("s"))
	t.Native, _ = meter.Float64Gauge("aws.native.window.value", metric.WithDescription("Latest observed native AWS aggregation window; source unit/statistic/period are attributes"))
	otel.SetTextMapPropagator(propagation.TraceContext{})
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) { t.error(err) }))
	return t, nil
}

func (t *Telemetry) error(err error) {
	t.mu.Lock()
	t.lastError = "export failed; inspect connectivity and scoped key"
	t.mu.Unlock()
	fmt.Fprintln(os.Stderr, "telemetry export failed; inspect connectivity and scoped key")
}
func (t *Telemetry) Health() any {
	t.mu.Lock()
	defer t.mu.Unlock()
	return map[string]any{"enabled": t.Enabled, "last_export_error": t.lastError}
}
func (t *Telemetry) Flush() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()
	var failures []error
	for _, f := range []func(context.Context) error{t.tp.ForceFlush, t.lp.ForceFlush, t.mp.ForceFlush} {
		if err := f(ctx); err != nil {
			t.error(err)
			failures = append(failures, err)
		}
	}
	if len(failures) == 0 {
		t.mu.Lock()
		t.lastError = ""
		t.mu.Unlock()
	}
	return errors.Join(failures...)
}
func (t *Telemetry) Shutdown(ctx context.Context) {
	_ = t.tp.Shutdown(ctx)
	_ = t.lp.Shutdown(ctx)
	_ = t.mp.Shutdown(ctx)
}
func (t *Telemetry) Log(ctx context.Context, event, text string, attrs ...attribute.KeyValue) {
	t.LogAt(ctx, time.Now(), event, text, attrs...)
}
func (t *Telemetry) LogAt(ctx context.Context, at time.Time, event, text string, attrs ...attribute.KeyValue) {
	var r otellog.Record
	r.SetTimestamp(at)
	r.SetObservedTimestamp(time.Now())
	r.SetSeverity(otellog.SeverityInfo)
	r.SetBody(otellog.StringValue(text))
	values := []otellog.KeyValue{otellog.String("event.name", event)}
	for _, a := range attrs {
		switch a.Value.Type() {
		case attribute.BOOL:
			values = append(values, otellog.Bool(string(a.Key), a.Value.AsBool()))
		case attribute.INT64:
			values = append(values, otellog.Int64(string(a.Key), a.Value.AsInt64()))
		default:
			values = append(values, otellog.String(string(a.Key), a.Value.Emit()))
		}
	}
	r.AddAttributes(values...)
	t.Logger.Emit(ctx, r)
}
func (t *Telemetry) Operation(ctx context.Context, name string, kind trace.SpanKind, f func(context.Context) error) error {
	ctx, span := t.Tracer.Start(ctx, name, trace.WithSpanKind(kind))
	start := time.Now()
	err := f(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	t.Counter.Add(ctx, 1, metric.WithAttributes(attribute.String("operation", name), attribute.Bool("error", err != nil)))
	t.Duration.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(attribute.String("operation", name)))
	span.End()
	return err
}
