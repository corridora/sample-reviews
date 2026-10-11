package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	qtypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"go.opentelemetry.io/otel/trace"
)

var ErrMissing = errors.New("record not found")

type Review struct {
	ID        string `json:"id"`
	ProductID string `json:"product_id"`
	Text      string `json:"text"`
	Rating    int    `json:"rating"`
	Status    string `json:"status"`
	Attempts  int    `json:"attempts"`
	TraceID   string `json:"trace_id"`
	Created   string `json:"created"`
}
type Message struct {
	ReviewID string            `json:"review_id"`
	Headers  map[string]string `json:"headers"`
	Created  int64             `json:"created"`
}
type Store interface {
	Save(context.Context, string, any) error
	Load(context.Context, string, any) error
	Reviews(context.Context) ([]Review, error)
	Enqueue(context.Context, Message) error
	Thumbnail(context.Context) ([]byte, error)
}

const thumbnail = `<svg xmlns="http://www.w3.org/2000/svg" width="240" height="160"><rect width="240" height="160" fill="#153f3b"/><circle cx="120" cy="75" r="45" fill="#d7ed9d"/><text x="120" y="145" text-anchor="middle" fill="white">Trace headphones</text></svg>`

type MemoryStore struct {
	mu   sync.RWMutex
	data map[string][]byte
	Jobs chan Message
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{data: map[string][]byte{}, Jobs: make(chan Message, 64)}
}
func (m *MemoryStore) Save(_ context.Context, id string, v any) error {
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	m.mu.Lock()
	m.data[id] = b
	m.mu.Unlock()
	return nil
}
func (m *MemoryStore) Load(_ context.Context, id string, v any) error {
	m.mu.RLock()
	b, ok := m.data[id]
	m.mu.RUnlock()
	if !ok {
		return ErrMissing
	}
	return json.Unmarshal(b, v)
}
func (m *MemoryStore) Reviews(_ context.Context) ([]Review, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := []Review{}
	for k, b := range m.data {
		if strings.HasPrefix(k, "review#") {
			var r Review
			_ = json.Unmarshal(b, &r)
			out = append(out, r)
		}
	}
	return out, nil
}
func (m *MemoryStore) Enqueue(ctx context.Context, msg Message) error {
	select {
	case m.Jobs <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		return errors.New("local queue capacity exceeded")
	}
}
func (m *MemoryStore) Thumbnail(context.Context) ([]byte, error) { return []byte(thumbnail), nil }

type AWSStore struct {
	db *dynamodb.Client
	q  *sqs.Client
	s3 *s3.Client
	c  Config
	t  *Telemetry
}

func NewAWSStore(ctx context.Context, c Config, t *Telemetry) (*AWSStore, error) {
	cfg, e := config.LoadDefaultConfig(ctx, config.WithRegion(c.Region))
	if e != nil {
		return nil, e
	}
	return &AWSStore{dynamodb.NewFromConfig(cfg), sqs.NewFromConfig(cfg), s3.NewFromConfig(cfg), c, t}, nil
}
func (s *AWSStore) Save(ctx context.Context, id string, v any) error {
	return s.t.Operation(ctx, "DynamoDB.PutItem", trace.SpanKindClient, func(ctx context.Context) error {
		b, e := json.Marshal(v)
		if e != nil {
			return e
		}
		item := map[string]dbtypes.AttributeValue{"id": &dbtypes.AttributeValueMemberS{Value: id}, "payload": &dbtypes.AttributeValueMemberS{Value: string(b)}, "expires": &dbtypes.AttributeValueMemberN{Value: fmt.Sprint(time.Now().Add(7 * 24 * time.Hour).Unix())}}
		if r, ok := v.(Review); ok {
			item["kind"] = &dbtypes.AttributeValueMemberS{Value: "review"}
			item["created"] = &dbtypes.AttributeValueMemberS{Value: r.Created + "#" + r.ID}
		}
		_, e = s.db.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(s.c.Table), Item: item})
		return e
	})
}
func (s *AWSStore) Load(ctx context.Context, id string, v any) error {
	return s.t.Operation(ctx, "DynamoDB.GetItem", trace.SpanKindClient, func(ctx context.Context) error {
		out, e := s.db.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(s.c.Table), Key: map[string]dbtypes.AttributeValue{"id": &dbtypes.AttributeValueMemberS{Value: id}}, ConsistentRead: aws.Bool(true)})
		if e != nil {
			return e
		}
		p, ok := out.Item["payload"].(*dbtypes.AttributeValueMemberS)
		if !ok {
			return ErrMissing
		}
		return json.Unmarshal([]byte(p.Value), v)
	})
}
func (s *AWSStore) Reviews(ctx context.Context) (out []Review, err error) {
	out = []Review{}
	err = s.t.Operation(ctx, "DynamoDB.Query", trace.SpanKindClient, func(ctx context.Context) error {
		o, e := s.db.Query(ctx, &dynamodb.QueryInput{TableName: aws.String(s.c.Table), IndexName: aws.String("ReviewsByDate"), Limit: aws.Int32(25), ScanIndexForward: aws.Bool(false), KeyConditionExpression: aws.String("kind = :p"), ExpressionAttributeValues: map[string]dbtypes.AttributeValue{":p": &dbtypes.AttributeValueMemberS{Value: "review"}}})
		if e != nil {
			return e
		}
		for _, i := range o.Items {
			if p, ok := i["payload"].(*dbtypes.AttributeValueMemberS); ok {
				var r Review
				if e = json.Unmarshal([]byte(p.Value), &r); e != nil {
					return e
				}
				out = append(out, r)
			}
		}
		return nil
	})
	return
}
func (s *AWSStore) Enqueue(ctx context.Context, msg Message) error {
	return s.t.Operation(ctx, "SQS.SendMessage", trace.SpanKindProducer, func(ctx context.Context) error { // Producer span context must replace the request's context.
		msg.Headers = Inject(ctx)
		b, e := json.Marshal(msg)
		if e != nil {
			return e
		}
		attrs := map[string]qtypes.MessageAttributeValue{}
		for k, v := range msg.Headers {
			attrs[k] = qtypes.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(v)}
		}
		_, e = s.q.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: aws.String(s.c.Queue), MessageBody: aws.String(string(b)), MessageAttributes: attrs})
		return e
	})
}
func (s *AWSStore) Thumbnail(ctx context.Context) (out []byte, err error) {
	err = s.t.Operation(ctx, "S3.GetObject", trace.SpanKindClient, func(ctx context.Context) error {
		o, e := s.s3.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.c.Bucket), Key: aws.String("fixtures/headphones.svg")})
		if e != nil { // Small deterministic fixture, safe to create idempotently on first read.
			var missing *s3types.NoSuchKey
			if !errors.As(e, &missing) {
				return e
			}
			if e = s.t.Operation(ctx, "S3.PutObject", trace.SpanKindClient, func(ctx context.Context) error {
				_, e := s.s3.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(s.c.Bucket), Key: aws.String("fixtures/headphones.svg"), Body: strings.NewReader(thumbnail), ContentType: aws.String("image/svg+xml")})
				return e
			}); e != nil {
				return e
			}
			out = []byte(thumbnail)
			return nil
		}
		defer o.Body.Close()
		out, e = io.ReadAll(io.LimitReader(o.Body, 65536))
		return e
	})
	return
}
