package app

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// LoadSecret happens before OTel initialization, using IAM rather than static credentials.
func LoadSecret(ctx context.Context, region, arn string) (map[string]string, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	cfg, e := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if e != nil {
		return nil, e
	}
	out, e := secretsmanager.NewFromConfig(cfg).GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: aws.String(arn)})
	if e != nil {
		return nil, e
	}
	if out.SecretString == nil {
		return nil, fmt.Errorf("configuration must be a JSON SecretString")
	}
	var values map[string]string
	e = json.Unmarshal([]byte(*out.SecretString), &values)
	return values, e
}

func (a *App) Lambda(ctx context.Context, raw json.RawMessage) (any, error) {
	defer a.T.Flush()
	var shape map[string]json.RawMessage
	if e := json.Unmarshal(raw, &shape); e != nil {
		return nil, e
	}
	if _, ok := shape["awslogs"]; ok {
		return a.CloudWatchLogs(ctx, raw)
	}
	if _, ok := shape["Records"]; ok {
		var batch events.SQSEvent
		if e := json.Unmarshal(raw, &batch); e != nil {
			return nil, e
		}
		result := events.SQSEventResponse{BatchItemFailures: []events.SQSBatchItemFailure{}}
		for _, r := range batch.Records {
			var msg Message
			if e := json.Unmarshal([]byte(r.Body), &msg); e != nil {
				result.BatchItemFailures = append(result.BatchItemFailures, events.SQSBatchItemFailure{ItemIdentifier: r.MessageId})
				continue
			}
			if msg.Headers == nil {
				msg.Headers = map[string]string{}
			}
			for k, v := range r.MessageAttributes {
				if v.StringValue != nil && (k == "traceparent" || k == "tracestate") {
					msg.Headers[k] = *v.StringValue
				}
			}
			attempt, _ := strconv.Atoi(r.Attributes["ApproximateReceiveCount"])
			if attempt < 1 {
				attempt = 1
			}
			if a.Moderate(ctx, msg, attempt) != nil {
				result.BatchItemFailures = append(result.BatchItemFailures, events.SQSBatchItemFailure{ItemIdentifier: r.MessageId})
			}
		}
		return result, nil
	}
	if _, ok := shape["requestContext"]; ok {
		var e events.APIGatewayV2HTTPRequest
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, err
		}
		body := []byte(e.Body)
		if e.IsBase64Encoded {
			var err error
			body, err = base64.StdEncoding.DecodeString(e.Body)
			if err != nil {
				return nil, err
			}
		}
		target := e.RawPath
		if e.RawQueryString != "" {
			target += "?" + e.RawQueryString
		}
		r, err := http.NewRequestWithContext(ctx, e.RequestContext.HTTP.Method, "https://"+e.RequestContext.DomainName+target, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		for k, v := range e.Headers {
			r.Header.Set(k, v)
		}
		rec := httptest.NewRecorder()
		a.Handler().ServeHTTP(rec, r)
		headers := map[string]string{}
		for k, v := range rec.Header() {
			headers[k] = strings.Join(v, ",")
		} // Binary assets must preserve arbitrary bytes through HTTP API.
		return events.APIGatewayV2HTTPResponse{StatusCode: rec.Code, Headers: headers, Body: base64.StdEncoding.EncodeToString(rec.Body.Bytes()), IsBase64Encoded: true}, nil
	}
	var event struct {
		Action string `json:"action"`
	}
	if e := json.Unmarshal(raw, &event); e != nil {
		return nil, e
	}
	switch event.Action {
	case "metrics":
		return map[string]any{"ok": true}, a.PollMetrics(ctx)
	case "daily", "watchdog":
		return a.Scheduled(ctx, event.Action)
	default:
		return nil, fmt.Errorf("unsupported Lambda event")
	}
}

func (a *App) Scheduled(ctx context.Context, action string) (any, error) {
	ctx, span := a.T.Tracer.Start(ctx, "scheduler."+action, trace.WithSpanKind(trace.SpanKindInternal))
	defer span.End()
	// Scheduler exercises the seeded action even without a browser. Browser evidence remains separate.
	if action == "watchdog" {
		var h Heartbeat
		_ = a.Store.Load(ctx, "heartbeat", &h)
		if h.Day == a.Now().UTC().Format("2006-01-02") && h.Success && h.Browser {
			return map[string]any{"skipped": "browser heartbeat present"}, nil
		}
		if _, e := a.SetScenario(ctx, "regression", 180); e != nil {
			return nil, e
		}
		a.T.Log(ctx, "synthetic.watchdog", "browser evidence missing; bounded API replay started", attribute.Bool("demo.simulated", true))
	}
	bad, e := a.Submit(ctx, "demo-product", "Daily seeded moderation regression", 4)
	if e != nil {
		return nil, e
	}
	control, e := a.Submit(ctx, "control-product", "Healthy seeded moderation control", 5)
	if e != nil {
		return nil, e
	}
	a.T.Log(ctx, "synthetic.api_probe", "scheduled review and healthy control enqueued", attribute.String("review.id", bad.ID), attribute.Bool("browser", false))
	return map[string]any{"review_id": bad.ID, "control_id": control.ID, "browser_evidence": false}, nil
}

func DecodeCloudWatch(raw json.RawMessage) (events.CloudwatchLogsData, error) {
	var envelope events.CloudwatchLogsEvent
	if e := json.Unmarshal(raw, &envelope); e != nil {
		return events.CloudwatchLogsData{}, e
	}
	b, e := base64.StdEncoding.DecodeString(envelope.AWSLogs.Data)
	if e != nil {
		return events.CloudwatchLogsData{}, e
	}
	gz, e := gzip.NewReader(bytes.NewReader(b))
	if e != nil {
		return events.CloudwatchLogsData{}, e
	}
	defer gz.Close()
	var out events.CloudwatchLogsData
	e = json.NewDecoder(io.LimitReader(gz, 2<<20)).Decode(&out)
	return out, e
}
func (a *App) CloudWatchLogs(ctx context.Context, raw json.RawMessage) (any, error) {
	payload, e := DecodeCloudWatch(raw)
	if e != nil {
		return nil, e
	}
	if payload.MessageType == "CONTROL_MESSAGE" {
		return map[string]bool{"ok": true}, nil
	}
	var checkpoints []string
	for _, r := range payload.LogEvents { // Only Lambda platform records: application OTLP records already export directly.
		if !strings.HasPrefix(r.Message, "REPORT ") && !strings.HasPrefix(r.Message, "START ") && !strings.HasPrefix(r.Message, "END ") {
			continue
		}
		id := "cloudwatch#" + payload.LogGroup + "#" + r.ID
		var checkpoint map[string]any
		e := a.Store.Load(ctx, id, &checkpoint)
		if e != nil && !errors.Is(e, ErrMissing) {
			return nil, e
		}
		if e == nil {
			continue
		}
		checkpoints = append(checkpoints, id)
		a.T.LogAt(ctx, time.UnixMilli(r.Timestamp), "aws.cloudwatch.platform", r.Message, attribute.String("aws.log.group", payload.LogGroup), attribute.String("aws.log.stream", payload.LogStream), attribute.String("cloud.account.id", payload.Owner), attribute.String("aws.log.event.id", r.ID), attribute.Int64("aws.log.timestamp_ms", r.Timestamp))
	}
	// Persist checkpoints only after SDK export acceptance. Reserved bridge concurrency
	// is one; retries after a crash can still duplicate records, identified by source ID.
	if len(checkpoints) > 0 {
		if e := a.T.Flush(); e != nil {
			return nil, e
		}
		for _, id := range checkpoints {
			if e := a.Store.Save(ctx, id, map[string]bool{"delivered": true}); e != nil {
				return nil, e
			}
		}
	}
	return map[string]any{"received": len(payload.LogEvents)}, nil
}
func (a *App) PollMetrics(ctx context.Context) error {
	return a.T.Operation(ctx, "CloudWatch.GetMetricData", trace.SpanKindClient, func(ctx context.Context) error {
		cfg, e := config.LoadDefaultConfig(ctx, config.WithRegion(a.C.Region))
		if e != nil {
			return e
		}
		queries := []cwtypes.MetricDataQuery{}
		for i, name := range []string{"Invocations", "Errors", "Duration"} {
			queries = append(queries, cwtypes.MetricDataQuery{Id: aws.String(fmt.Sprintf("m%d", i)), MetricStat: &cwtypes.MetricStat{Metric: &cwtypes.Metric{Namespace: aws.String("AWS/Lambda"), MetricName: aws.String(name), Dimensions: []cwtypes.Dimension{{Name: aws.String("FunctionName"), Value: aws.String(env("API_FUNCTION_NAME", "sample-reviews-api"))}}}, Period: aws.Int32(900), Stat: aws.String("Sum")}})
		}
		queries = append(queries, cwtypes.MetricDataQuery{Id: aws.String("dlq"), MetricStat: &cwtypes.MetricStat{Metric: &cwtypes.Metric{Namespace: aws.String("AWS/SQS"), MetricName: aws.String("ApproximateNumberOfMessagesVisible"), Dimensions: []cwtypes.Dimension{{Name: aws.String("QueueName"), Value: aws.String(env("DLQ_NAME", "sample-reviews-dlq"))}}}, Period: aws.Int32(900), Stat: aws.String("Maximum")}})
		out, e := cloudwatch.NewFromConfig(cfg).GetMetricData(ctx, &cloudwatch.GetMetricDataInput{StartTime: aws.Time(a.Now().Add(-30 * time.Minute)), EndTime: aws.Time(a.Now()), MetricDataQueries: queries, MaxDatapoints: aws.Int32(20)})
		if e != nil {
			return e
		}
		for _, r := range out.MetricDataResults {
			a.RecordNativeMetric(ctx, r)
		}
		return nil
	})
}

func (a *App) RecordNativeMetric(ctx context.Context, r cwtypes.MetricDataResult) {
	if len(r.Values) == 0 || len(r.Timestamps) == 0 {
		return
	}
	latest := 0
	for i, ts := range r.Timestamps {
		if i < len(r.Values) && ts.After(r.Timestamps[latest]) {
			latest = i
		}
	}
	unit, statistic := "Count", "Sum"
	if aws.ToString(r.Id) == "m2" {
		unit = "Milliseconds"
	}
	if aws.ToString(r.Id) == "dlq" {
		statistic = "Maximum"
	}
	attrs := []attribute.KeyValue{attribute.String("aws.metric.id", aws.ToString(r.Id)), attribute.String("aws.metric.label", aws.ToString(r.Label)), attribute.String("aws.metric.source_unit", unit), attribute.String("aws.metric.statistic", statistic), attribute.Int("aws.metric.period_seconds", 900)}
	a.T.Native.Record(ctx, r.Values[latest], metric.WithAttributes(attrs...))
	a.T.LogAt(ctx, r.Timestamps[latest], "aws.cloudwatch.metric_window", fmt.Sprintf("native window value %g", r.Values[latest]), attrs...)
}
