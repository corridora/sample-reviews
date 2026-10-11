package app

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"go.opentelemetry.io/otel/trace"
	logpb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	metricpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

func testConfig() Config {
	return Config{Mode: "local", Port: "4102", ControlToken: "test-control", Endpoint: "http://127.0.0.1:1", RUMScript: "http://127.0.0.1:1/rum.js", Service: "sample-reviews-api", Namespace: "corridora-samples.reviews", Environment: "test", Version: "test-build", DailyStart: "13:20", Window: 600 * time.Second, Region: "us-east-1"}
}
func testApp(t *testing.T) (*App, *MemoryStore) {
	t.Helper()
	c := testConfig()
	tel, e := NewTelemetry(context.Background(), c)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { tel.Shutdown(context.Background()) })
	m := NewMemoryStore()
	a := New(c, tel, m)
	a.Now = func() time.Time { return time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC) }
	return a, m
}
func invoke(a *App, method, path, body, auth string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if auth != "" {
		r.Header.Set("Authorization", auth)
	}
	w := httptest.NewRecorder()
	a.Handler().ServeHTTP(w, r)
	return w
}
func TestDailyWindowAndOverrideExpiry(t *testing.T) {
	a, _ := testApp(t)
	a.Now = func() time.Time { return time.Date(2026, 10, 11, 13, 20, 0, 0, time.UTC) }
	s, e := a.Scenario(context.Background())
	if e != nil || s.Phase != "regression" || s.Expires != a.Now().Add(600*time.Second).Unix() {
		t.Fatalf("daily window: %+v %v", s, e)
	}
	s, e = a.SetScenario(context.Background(), "healthy", 2)
	if e != nil {
		t.Fatal(e)
	}
	again, _ := a.Scenario(context.Background())
	if again.Phase != "healthy" {
		t.Fatal("override lost")
	}
	now := a.Now()
	a.Now = func() time.Time { return now.Add(3 * time.Second) }
	again, _ = a.Scenario(context.Background())
	if again.Phase != "regression" {
		t.Fatal("expired override masked daily window")
	}
	a.Now = func() time.Time { return time.Date(2026, 10, 11, 13, 30, 0, 0, time.UTC) }
	again, _ = a.Scenario(context.Background())
	if again.Phase != "healthy" {
		t.Fatal("daily window did not expire")
	}
}
func TestDailyWindowCrossesUTCMidnight(t *testing.T) {
	a, _ := testApp(t)
	a.C.DailyStart = "23:55"
	a.Now = func() time.Time { return time.Date(2026, 10, 12, 0, 1, 0, 0, time.UTC) }
	s, e := a.Scenario(context.Background())
	if e != nil || s.Phase != "regression" || s.Release != "schema-v2-2026-10-11" || s.Expires != time.Date(2026, 10, 12, 0, 5, 0, 0, time.UTC).Unix() {
		t.Fatalf("previous-day window lost: %+v %v", s, e)
	}
	a.Now = func() time.Time { return time.Date(2026, 10, 12, 0, 5, 0, 0, time.UTC) }
	s, _ = a.Scenario(context.Background())
	if s.Phase != "healthy" {
		t.Fatal("midnight window did not recover")
	}
}
func TestControlAuthenticationAndDuration(t *testing.T) {
	a, _ := testApp(t)
	for _, auth := range []string{"", "test-control", "Basic test-control", "Bearer bad"} {
		w := invoke(a, "POST", "/api/demo/scenario", `{"phase":"regression","duration_seconds":120}`, auth)
		if w.Code != 401 {
			t.Fatalf("accepted auth %q", auth)
		}
	}
	for _, body := range []string{`{"phase":"regression","duration_seconds":0}`, `{"phase":"regression","duration_seconds":601}`, `{"phase":"regression","duration_seconds":1.5}`, `{"phase":"unknown","duration_seconds":10}`} {
		if w := invoke(a, "POST", "/api/demo/scenario", body, "Bearer test-control"); w.Code != 400 {
			t.Fatalf("accepted %s", body)
		}
	}
	w := invoke(a, "POST", "/api/demo/scenario", `{"phase":"regression","duration_seconds":120}`, "Bearer test-control")
	if w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal(w.Code, w.Body.String())
	}
	a.C.ControlToken = ""
	if invoke(a, "POST", "/api/demo/scenario", `{}`, "Bearer ").Code != 401 {
		t.Fatal("empty token accepted")
	}
}
func TestModerationRetriesHealthyControlAndRecovery(t *testing.T) {
	a, m := testApp(t)
	ctx := context.Background()
	_, _ = a.SetScenario(ctx, "regression", 120)
	r, e := a.Submit(ctx, "demo-product", "Seeded schema fixture", 4)
	if e != nil {
		t.Fatal(e)
	}
	msg := <-m.Jobs
	for attempt := 1; attempt <= 3; attempt++ {
		if a.Moderate(ctx, msg, attempt) == nil {
			t.Fatal("expected schema rejection")
		}
	}
	var stored Review
	_ = m.Load(ctx, "review#"+r.ID, &stored)
	if stored.Status != "failed" || stored.Attempts != 3 {
		t.Fatalf("bad retry bound %+v", stored)
	}
	control, e := a.Submit(ctx, "control-product", "Healthy fixture", 5)
	if e != nil {
		t.Fatal(e)
	}
	if a.Moderate(ctx, <-m.Jobs, 1) != nil {
		t.Fatal("control affected")
	}
	_ = m.Load(ctx, "review#"+control.ID, &stored)
	if stored.Status != "approved" {
		t.Fatal("control not approved")
	}
	now := a.Now()
	a.Now = func() time.Time { return now.Add(121 * time.Second) }
	if e = a.Moderate(ctx, msg, 4); e != nil {
		t.Fatal("expired regression remained active", e)
	}
	_ = m.Load(ctx, "review#"+r.ID, &stored)
	if stored.Status != "approved" {
		t.Fatal("recovery did not approve")
	}
	if e = a.Moderate(ctx, msg, 5); e != nil {
		t.Fatal("duplicate approved message not idempotent")
	}
}
func TestLeafContractsNoRecursion(t *testing.T) {
	a, _ := testApp(t)
	a.C.RecommenderURL = "https://127.0.0.1:1"
	for _, p := range []string{"/internal/review-summary?product_id=demo-product", "/internal/price-fixture"} {
		w := invoke(a, "GET", p, "", "")
		if w.Code != 200 {
			t.Fatal(w.Code)
		}
		var out map[string]any
		if json.Unmarshal(w.Body.Bytes(), &out) != nil {
			t.Fatal("invalid leaf JSON")
		}
		if p == "/internal/price-fixture" && out["fictional"] != true {
			t.Fatal("fixture not fictional")
		}
	}
	a.C.DownstreamToken = "dependency-only"
	if invoke(a, "GET", "/internal/review-summary", "", "").Code != 401 {
		t.Fatal("dependency auth ignored")
	}
	if invoke(a, "GET", "/internal/review-summary", "", "Bearer dependency-only").Code != 200 {
		t.Fatal("dependency auth rejected")
	}
}

type captured struct {
	mu        sync.Mutex
	traces    []*tracepb.ExportTraceServiceRequest
	logs      []*logpb.ExportLogsServiceRequest
	metrics   []*metricpb.ExportMetricsServiceRequest
	badHeader bool
}

func capture(t *testing.T) (*httptest.Server, *captured) {
	t.Helper()
	c := &captured{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		c.mu.Lock()
		defer c.mu.Unlock()
		if r.Header.Get("X-CORRIDORA-KEY") != "local-key" {
			c.badHeader = true
		}
		switch r.URL.Path {
		case "/v1/traces":
			v := &tracepb.ExportTraceServiceRequest{}
			if e := proto.Unmarshal(b, v); e != nil {
				t.Error(e)
			}
			c.traces = append(c.traces, v)
		case "/v1/logs":
			v := &logpb.ExportLogsServiceRequest{}
			if e := proto.Unmarshal(b, v); e != nil {
				t.Error(e)
			}
			c.logs = append(c.logs, v)
		case "/v1/metrics":
			v := &metricpb.ExportMetricsServiceRequest{}
			if e := proto.Unmarshal(b, v); e != nil {
				t.Error(e)
			}
			c.metrics = append(c.metrics, v)
		default:
			t.Error("wrong signal path", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(200)
	}))
	t.Cleanup(server.Close)
	return server, c
}
func TestOTLPAllSignalsAndW3CParentage(t *testing.T) {
	server, sink := capture(t)
	c := testConfig()
	c.Endpoint = server.URL
	c.IngestKey = "local-key"
	tel, e := NewTelemetry(context.Background(), c)
	if e != nil {
		t.Fatal(e)
	}
	defer tel.Shutdown(context.Background())
	a := New(c, tel, NewMemoryStore())
	a.Now = func() time.Time { return time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC) }
	req := httptest.NewRequest("GET", "/api/trace-check", nil)
	req.Header.Set("traceparent", "00-00112233445566778899aabbccddeeff-1122334455667788-01")
	rec := httptest.NewRecorder()
	a.Handler().ServeHTTP(rec, req)
	_ = tel.Operation(context.Background(), "fixture.dependency", trace.SpanKindClient, func(context.Context) error { return nil })
	if e = tel.Flush(); e != nil {
		t.Fatal(e)
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.logs) == 0 || len(sink.metrics) == 0 || sink.badHeader {
		t.Fatal("missing/malformed signals")
	}
	found := false
	for _, r := range sink.traces {
		for _, rs := range r.ResourceSpans {
			for _, scope := range rs.ScopeSpans {
				for _, s := range scope.Spans {
					if s.Name == "GET /api/trace-check" {
						found = true
						if hex.EncodeToString(s.TraceId) != "00112233445566778899aabbccddeeff" || hex.EncodeToString(s.ParentSpanId) != "1122334455667788" || s.Kind.String() != "SPAN_KIND_SERVER" {
							t.Fatalf("wrong parentage %+v", s)
						}
					}
				}
			}
		}
	}
	if !found {
		t.Fatal("SERVER span not exported")
	}
}
func TestUnsampledIncomingContextRespected(t *testing.T) {
	server, sink := capture(t)
	c := testConfig()
	c.Endpoint = server.URL
	c.IngestKey = "local-key"
	tel, _ := NewTelemetry(context.Background(), c)
	defer tel.Shutdown(context.Background())
	a := New(c, tel, NewMemoryStore())
	req := httptest.NewRequest("GET", "/api/trace-check", nil)
	req.Header.Set("traceparent", "00-00112233445566778899aabbccddeeff-1122334455667788-00")
	a.Handler().ServeHTTP(httptest.NewRecorder(), req)
	_ = tel.Flush()
	sink.mu.Lock()
	defer sink.mu.Unlock()
	for _, r := range sink.traces {
		for _, rs := range r.ResourceSpans {
			for _, scope := range rs.ScopeSpans {
				for _, s := range scope.Spans {
					if s.Name == "GET /api/trace-check" {
						t.Fatal("unsampled request exported")
					}
				}
			}
		}
	}
}
func TestQueueConsumerParentAndPartialFailure(t *testing.T) {
	server, sink := capture(t)
	c := testConfig()
	c.Endpoint = server.URL
	c.IngestKey = "local-key"
	tel, e := NewTelemetry(context.Background(), c)
	if e != nil {
		t.Fatal(e)
	}
	defer tel.Shutdown(context.Background())
	m := NewMemoryStore()
	a := New(c, tel, m)
	a.Now = func() time.Time { return time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC) }
	ctx, producer := a.T.Tracer.Start(context.Background(), "producer", trace.WithSpanKind(trace.SpanKindProducer))
	r, _ := a.Submit(ctx, "demo-product", "Message context fixture", 4)
	producer.End()
	msg := <-m.Jobs
	if msg.Headers["traceparent"] == "" {
		t.Fatal("producer context missing")
	}
	_, _ = a.SetScenario(context.Background(), "regression", 120)
	body, _ := json.Marshal(msg)
	event := events.SQSEvent{Records: []events.SQSMessage{{MessageId: "message-1", Body: string(body), Attributes: map[string]string{"ApproximateReceiveCount": "2"}}}}
	raw, _ := json.Marshal(event)
	result, e := a.Lambda(context.Background(), raw)
	if e != nil {
		t.Fatal(e)
	}
	batch := result.(events.SQSEventResponse)
	if len(batch.BatchItemFailures) != 1 || batch.BatchItemFailures[0].ItemIdentifier != "message-1" {
		t.Fatal("partial failure omitted")
	}
	var stored Review
	_ = m.Load(context.Background(), "review#"+r.ID, &stored)
	if stored.Attempts != 2 {
		t.Fatal("receive count lost")
	}
	parts := strings.Split(msg.Headers["traceparent"], "-")
	found := false
	sink.mu.Lock()
	defer sink.mu.Unlock()
	for _, r := range sink.traces {
		for _, rs := range r.ResourceSpans {
			for _, scope := range rs.ScopeSpans {
				for _, span := range scope.Spans {
					if span.Name == "moderation.process" {
						found = true
						if hex.EncodeToString(span.TraceId) != parts[1] || hex.EncodeToString(span.ParentSpanId) != parts[2] || span.Kind.String() != "SPAN_KIND_CONSUMER" {
							t.Fatal("consumer context/parentage lost")
						}
					}
				}
			}
		}
	}
	if !found {
		t.Fatal("consumer span missing")
	}
}
func TestLambdaHTTPContextExtraction(t *testing.T) {
	a, _ := testApp(t)
	event := events.APIGatewayV2HTTPRequest{RawPath: "/api/trace-check", Headers: map[string]string{"traceparent": "00-00112233445566778899aabbccddeeff-1122334455667788-01"}, RequestContext: events.APIGatewayV2HTTPRequestContext{DomainName: "example.test", HTTP: events.APIGatewayV2HTTPRequestContextHTTPDescription{Method: "GET"}}}
	raw, _ := json.Marshal(event)
	out, e := a.Lambda(context.Background(), raw)
	if e != nil {
		t.Fatal(e)
	}
	response := out.(events.APIGatewayV2HTTPResponse)
	body, _ := base64.StdEncoding.DecodeString(response.Body)
	if response.StatusCode != 200 || !strings.Contains(string(body), "00112233445566778899aabbccddeeff") {
		t.Fatal("Lambda W3C context lost", string(body))
	}
}
func cloudWatchEvent(t *testing.T) json.RawMessage {
	t.Helper()
	payload := map[string]any{"messageType": "DATA_MESSAGE", "owner": "123456789012", "logGroup": "/aws/lambda/reviews-api", "logStream": "stream", "logEvents": []map[string]any{{"id": "event-1", "timestamp": 1700000000123, "message": "REPORT RequestId: seeded request"}, {"id": "event-2", "timestamp": 1700000000124, "message": "application message omitted"}}}
	var buffer bytes.Buffer
	gz := gzip.NewWriter(&buffer)
	_ = json.NewEncoder(gz).Encode(payload)
	_ = gz.Close()
	raw, _ := json.Marshal(map[string]any{"awslogs": map[string]string{"data": base64.StdEncoding.EncodeToString(buffer.Bytes())}})
	return raw
}
func TestCloudWatchConversionTimestampAndCheckpoint(t *testing.T) {
	server, sink := capture(t)
	c := testConfig()
	c.Endpoint = server.URL
	c.IngestKey = "local-key"
	tel, _ := NewTelemetry(context.Background(), c)
	defer tel.Shutdown(context.Background())
	a := New(c, tel, NewMemoryStore())
	raw := cloudWatchEvent(t)
	if _, e := a.CloudWatchLogs(context.Background(), raw); e != nil {
		t.Fatal(e)
	}
	if _, e := a.CloudWatchLogs(context.Background(), raw); e != nil {
		t.Fatal(e)
	}
	_ = tel.Flush()
	sink.mu.Lock()
	defer sink.mu.Unlock()
	count := 0
	for _, r := range sink.logs {
		for _, rs := range r.ResourceLogs {
			for _, scope := range rs.ScopeLogs {
				for _, record := range scope.LogRecords {
					if record.Body.GetStringValue() == "REPORT RequestId: seeded request" {
						count++
						if record.TimeUnixNano != 1700000000123000000 {
							t.Fatal("source timestamp lost")
						}
					}
				}
			}
		}
	}
	if count != 1 {
		t.Fatalf("expected deduplicated platform event, got %d", count)
	}
}
func TestAWSQueueAndDynamoAdapter(t *testing.T) {
	a, _ := testApp(t)
	var gotTrace string
	var queried bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		target := r.Header.Get("X-Amz-Target")
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		switch {
		case strings.Contains(target, "SendMessage"):
			attrs := body["MessageAttributes"].(map[string]any)
			gotTrace = attrs["traceparent"].(map[string]any)["StringValue"].(string)
			_, _ = w.Write([]byte(`{"MessageId":"fixture"}`))
		case strings.Contains(target, "Query"):
			queried = body["IndexName"] == "ReviewsByDate" && body["Limit"] == float64(25)
			_, _ = w.Write([]byte(`{"Items":[]}`))
		default:
			t.Error("unexpected AWS operation", target)
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	defer server.Close()
	cfg := aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("mock", "mock", "")}
	s := &AWSStore{c: Config{Table: "reviews", Queue: server.URL + "/queue"}, t: a.T, db: dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) { o.BaseEndpoint = aws.String(server.URL) }), q: sqs.NewFromConfig(cfg, func(o *sqs.Options) { o.BaseEndpoint = aws.String(server.URL) })}
	if e := s.Enqueue(context.Background(), Message{ReviewID: "fixture", Created: 1}); e != nil {
		t.Fatal(e)
	}
	if gotTrace == "" {
		t.Fatal("SQS context attribute missing")
	}
	if _, e := s.Reviews(context.Background()); e != nil {
		t.Fatal(e)
	}
	if !queried {
		t.Fatal("unbounded/incorrect review query")
	}
}
func TestNativeMetricConversionUnitWindowAndSourceTime(t *testing.T) {
	server, sink := capture(t)
	c := testConfig()
	c.Endpoint = server.URL
	c.IngestKey = "local-key"
	tel, _ := NewTelemetry(context.Background(), c)
	defer tel.Shutdown(context.Background())
	a := New(c, tel, NewMemoryStore())
	a.RecordNativeMetric(context.Background(), cwtypes.MetricDataResult{Id: aws.String("m2"), Label: aws.String("Duration"), Values: []float64{1, 7}, Timestamps: []time.Time{time.Unix(1700000000, 0), time.Unix(1700000900, 0)}})
	_ = tel.Flush()
	sink.mu.Lock()
	defer sink.mu.Unlock()
	found := false
	for _, export := range sink.metrics {
		for _, rs := range export.ResourceMetrics {
			for _, scope := range rs.ScopeMetrics {
				for _, m := range scope.Metrics {
					if m.Name == "aws.native.window.value" {
						for _, point := range m.GetGauge().DataPoints {
							found = true
							if point.GetAsDouble() != 7 {
								t.Fatal("old point selected")
							}
							attrs := map[string]string{}
							period := int64(0)
							for _, kv := range point.Attributes {
								attrs[kv.Key] = kv.Value.GetStringValue()
								if kv.Key == "aws.metric.period_seconds" {
									period = kv.Value.GetIntValue()
								}
							}
							if attrs["aws.metric.source_unit"] != "Milliseconds" || attrs["aws.metric.statistic"] != "Sum" || period != 900 {
								t.Fatal("native units/window semantics missing")
							}
						}
					}
				}
			}
		}
	}
	if !found {
		t.Fatal("native gauge missing")
	}
	sourceTime := false
	for _, export := range sink.logs {
		for _, rs := range export.ResourceLogs {
			for _, scope := range rs.ScopeLogs {
				for _, r := range scope.LogRecords {
					if r.TimeUnixNano == 1700000900000000000 {
						sourceTime = true
					}
				}
			}
		}
	}
	if !sourceTime {
		t.Fatal("native source timestamp not preserved in log")
	}
}
func TestControlledDownstreamPropagationAndNoCredentialLeak(t *testing.T) {
	a, _ := testApp(t)
	a.C.IngestKey = "never-forward-backend"
	a.C.DownstreamToken = "dependency-only"
	seen := false
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = true
		if r.URL.Path != "/api/recommendations" || r.URL.Query().Get("product_id") != "demo-product" || r.Header.Get("traceparent") == "" || r.Header.Get("Authorization") != "Bearer dependency-only" || r.Header.Get("X-CORRIDORA-KEY") != "" {
			t.Error("incorrect dependency contract/context/credential separation")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"items":[{"id":"demo-product","score":0.9}],"model_revision":"fixture-v1"}`))
	}))
	defer target.Close()
	a.C.RecommenderURL = target.URL
	if _, e := a.Recommendations(context.Background()); e != nil || !seen {
		t.Fatal("controlled call failed", e)
	}
	redirected := false
	untrusted := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirected = true }))
	defer untrusted.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, untrusted.URL, 302) }))
	defer source.Close()
	a.C.RecommenderURL = source.URL
	if _, e := a.Recommendations(context.Background()); e == nil || redirected {
		t.Fatal("followed dependency redirect or failed to flag status")
	}
}
