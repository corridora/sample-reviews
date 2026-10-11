package app

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type Scenario struct {
	Phase     string `json:"phase"`
	Release   string `json:"release"`
	Expires   int64  `json:"expires"`
	Simulated bool   `json:"simulated"`
	Version   string `json:"service_version"`
}
type Heartbeat struct {
	Day     string `json:"day"`
	Success bool   `json:"success"`
	TraceID string `json:"trace_id"`
	Browser bool   `json:"browser"`
	At      string `json:"at"`
}
type App struct {
	C           Config
	T           *Telemetry
	Store       Store
	Now         func() time.Time
	mu          sync.Mutex
	lastRelease string
}

func New(c Config, t *Telemetry, s Store) *App { return &App{C: c, T: t, Store: s, Now: time.Now} }
func Inject(ctx context.Context) map[string]string {
	m := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, m)
	return m
}
func ID() string { b := make([]byte, 12); _, _ = rand.Read(b); return hex.EncodeToString(b) }
func (a *App) Scenario(ctx context.Context) (Scenario, error) {
	now := a.Now().UTC()
	var s Scenario
	e := a.Store.Load(ctx, "control", &s)
	if e != nil && !errors.Is(e, ErrMissing) {
		return s, e
	}
	if e != nil || s.Expires <= now.Unix() {
		start, _ := time.Parse("15:04", a.C.DailyStart)
		daily := time.Date(now.Year(), now.Month(), now.Day(), start.Hour(), start.Minute(), 0, 0, time.UTC)
		if now.Before(daily) {
			daily = daily.AddDate(0, 0, -1)
		}
		bad := !now.Before(daily) && now.Before(daily.Add(a.C.Window))
		s = Scenario{Phase: "healthy", Release: "schema-v1-" + now.Format("2006-01-02"), Simulated: true, Version: a.C.Version}
		if bad {
			s.Phase = "regression"
			s.Release = "schema-v2-" + daily.Format("2006-01-02")
			s.Expires = daily.Add(a.C.Window).Unix()
		}
	}
	s.Version = a.C.Version
	a.mu.Lock()
	changed := a.lastRelease != s.Release
	a.lastRelease = s.Release
	a.mu.Unlock()
	if changed {
		a.T.Log(ctx, "deployment", "simulated moderation rollout", scenarioAttrs(s)...)
	}
	return s, nil
}
func scenarioAttrs(s Scenario) []attribute.KeyValue {
	return []attribute.KeyValue{attribute.Bool("demo.simulated", true), attribute.String("demo.scenario", "moderation-schema"), attribute.String("demo.release", s.Release), attribute.String("demo.phase", s.Phase), attribute.String("service.version", s.Version)}
}
func (a *App) SetScenario(ctx context.Context, phase string, seconds float64) (Scenario, error) {
	if phase != "healthy" && phase != "regression" && phase != "recover" {
		return Scenario{}, errors.New("phase must be healthy, regression or recover")
	}
	if seconds < 1 || seconds > 600 || seconds != float64(int64(seconds)) {
		return Scenario{}, errors.New("duration_seconds must be an integer in 1..600")
	}
	if phase == "recover" {
		phase = "healthy"
	}
	s := Scenario{phase, "schema-" + phase + "-" + ID(), a.Now().Add(time.Duration(seconds) * time.Second).Unix(), true, a.C.Version}
	if e := a.Store.Save(ctx, "control", s); e != nil {
		return s, e
	}
	a.T.Log(ctx, "deployment", "simulated deployment changed", scenarioAttrs(s)...)
	return s, nil
}
func (a *App) Submit(ctx context.Context, product, text string, rating int) (Review, error) {
	r := Review{ID: ID(), ProductID: product, Text: text, Rating: rating, Status: "pending", TraceID: trace.SpanContextFromContext(ctx).TraceID().String(), Created: a.Now().UTC().Format(time.RFC3339)}
	if product != "demo-product" && product != "control-product" {
		return r, errors.New("choose a seeded product")
	}
	if len(strings.TrimSpace(text)) < 3 || len(text) > 500 || rating < 1 || rating > 5 {
		return r, errors.New("review text 3..500 characters; rating 1..5")
	}
	if e := a.Store.Save(ctx, "review#"+r.ID, r); e != nil {
		return r, e
	}
	e := a.T.Operation(ctx, "review.publish", trace.SpanKindProducer, func(ctx context.Context) error {
		return a.Store.Enqueue(ctx, Message{r.ID, Inject(ctx), a.Now().Unix()})
	})
	return r, e
}
func (a *App) Moderate(ctx context.Context, msg Message, attempt int) error {
	ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(msg.Headers))
	ctx, span := a.T.Tracer.Start(ctx, "moderation.process", trace.WithSpanKind(trace.SpanKindConsumer))
	defer span.End()
	var r Review
	if e := a.Store.Load(ctx, "review#"+msg.ReviewID, &r); e != nil {
		return e
	}
	if r.Status == "approved" {
		return nil
	}
	s, e := a.Scenario(ctx)
	if e != nil {
		return e
	}
	span.SetAttributes(scenarioAttrs(s)...)
	span.SetAttributes(attribute.Int("messaging.message.receive_count", attempt), attribute.String("messaging.system", "aws_sqs"))
	r.Attempts = attempt
	bad := s.Phase == "regression" && r.ProductID == "demo-product" && a.Now().Unix()-msg.Created <= 600
	if bad {
		r.Status = "retrying"
		if attempt >= 3 {
			r.Status = "failed"
		}
		e = errors.New("simulated schema-v2 rejects seeded review; bounded retry")
		span.RecordError(e)
		span.SetStatus(codes.Error, e.Error())
		a.T.Log(ctx, "moderation.failure", e.Error(), append(scenarioAttrs(s), attribute.Int("attempt", attempt), attribute.String("review.id", r.ID))...)
	} else {
		r.Status = "approved"
		a.T.Log(ctx, "moderation.approved", "review approved", attribute.String("review.id", r.ID))
	}
	if saveErr := a.Store.Save(ctx, "review#"+r.ID, r); saveErr != nil {
		return saveErr
	}
	a.T.Counter.Add(ctx, 1)
	return e
}
func (a *App) LocalWorker(ctx context.Context, m *MemoryStore) {
	for worker := 0; worker < 2; worker++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case msg := <-m.Jobs:
					func() {
						for attempt := 1; attempt <= 3; attempt++ {
							if a.Moderate(ctx, msg, attempt) == nil {
								break
							}
							select {
							case <-ctx.Done():
								return
							case <-time.After(time.Second):
							}
						}
						a.T.Flush()
					}()
				}
			}
		}()
	}
}

type responseRecorder struct {
	http.ResponseWriter
	status int
}

func (w *responseRecorder) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}
func (a *App) Authorized(r *http.Request, token string) bool {
	header := r.Header.Get("Authorization")
	if !strings.HasPrefix(header, "Bearer ") {
		return false
	}
	candidate := strings.TrimPrefix(header, "Bearer ")
	return token != "" && subtle.ConstantTimeCompare([]byte(candidate), []byte(token)) == 1
}
func (a *App) json(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func (a *App) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := &responseRecorder{w, 200}
		w = response
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		if origin := r.Header.Get("Origin"); origin != "" && origin == a.C.Origin {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Headers", "content-type,traceparent,authorization")
			w.Header().Set("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(204)
			return
		}
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		name := r.Method + " " + routeName(r.URL.Path)
		ctx, span := a.T.Tracer.Start(ctx, name, trace.WithSpanKind(trace.SpanKindServer), trace.WithAttributes(attribute.String("http.request.method", r.Method), attribute.String("http.route", routeName(r.URL.Path))))
		defer span.End()
		started := time.Now()
		defer func() {
			span.SetAttributes(attribute.Int("http.response.status_code", response.status))
			if response.status >= 500 {
				span.SetStatus(codes.Error, "server response failed")
			}
			a.T.Counter.Add(ctx, 1, metric.WithAttributes(attribute.String("operation", name)))
			a.T.Duration.Record(ctx, time.Since(started).Seconds(), metric.WithAttributes(attribute.String("operation", name)))
		}()
		r = r.WithContext(ctx)
		s, e := a.Scenario(ctx)
		if e != nil {
			span.RecordError(e)
			a.json(w, 503, map[string]string{"error": "state unavailable"})
			return
		}
		span.SetAttributes(scenarioAttrs(s)...)
		sc := span.SpanContext()
		w.Header().Set("X-Trace-ID", sc.TraceID().String())
		if strings.HasPrefix(r.URL.Path, "/internal/") && a.C.DownstreamToken != "" && !a.Authorized(r, a.C.DownstreamToken) {
			a.json(w, 401, map[string]string{"error": "dependency authentication required"})
			return
		}
		switch {
		case r.Method == "GET" && r.URL.Path == "/healthz":
			a.json(w, 200, map[string]any{"ok": true, "telemetry": a.T.Health(), "service_version": a.C.Version})
		case r.Method == "GET" && r.URL.Path == "/api/trace-check":
			a.json(w, 200, map[string]string{"trace_id": sc.TraceID().String(), "span_id": sc.SpanID().String()})
		case r.Method == "GET" && r.URL.Path == "/api/demo/status":
			a.json(w, 200, struct {
				Scenario
				DailyStart    string `json:"daily_start_utc"`
				WindowSeconds int    `json:"window_seconds"`
			}{s, a.C.DailyStart, int(a.C.Window.Seconds())})
		case r.Method == "POST" && r.URL.Path == "/api/demo/scenario":
			if !a.Authorized(r, a.C.ControlToken) {
				a.json(w, 401, map[string]string{"error": "authentication required"})
				return
			}
			var b struct {
				Phase    string  `json:"phase"`
				Duration float64 `json:"duration_seconds"`
			}
			if json.NewDecoder(io.LimitReader(r.Body, 1024)).Decode(&b) != nil {
				a.json(w, 400, map[string]string{"error": "invalid JSON"})
				return
			}
			s, e := a.SetScenario(ctx, b.Phase, b.Duration)
			if e != nil {
				a.json(w, 400, map[string]string{"error": e.Error()})
				return
			}
			a.json(w, 200, s)
		case r.URL.Path == "/api/demo/heartbeat":
			if !a.Authorized(r, a.C.ControlToken) {
				a.json(w, 401, map[string]string{"error": "authentication required"})
				return
			}
			if r.Method == "POST" {
				var h Heartbeat
				if json.NewDecoder(io.LimitReader(r.Body, 1024)).Decode(&h) != nil {
					a.json(w, 400, map[string]string{"error": "invalid heartbeat"})
					return
				}
				h.Day = a.Now().UTC().Format("2006-01-02")
				h.At = a.Now().UTC().Format(time.RFC3339)
				if e := a.Store.Save(ctx, "heartbeat", h); e != nil {
					a.json(w, 503, map[string]string{"error": "heartbeat write failed"})
					return
				}
				a.T.Log(ctx, "synthetic.heartbeat", "browser journey result", attribute.Bool("success", h.Success), attribute.Bool("browser", h.Browser))
				a.json(w, 200, h)
			} else if r.Method == "GET" {
				var h Heartbeat
				_ = a.Store.Load(ctx, "heartbeat", &h)
				a.json(w, 200, h)
			} else {
				a.json(w, 405, map[string]string{"error": "method"})
			}
		case r.Method == "GET" && r.URL.Path == "/internal/review-summary":
			a.T.Operation(ctx, "reviews.summary.fixture", trace.SpanKindInternal, func(context.Context) error { return nil })
			a.json(w, 200, map[string]any{"product_id": envProduct(r.URL.Query().Get("product_id")), "rating": 4.5, "count": 12})
		case r.Method == "GET" && r.URL.Path == "/internal/price-fixture":
			a.json(w, 200, map[string]any{"prices": map[string]float64{"DEMO": 42.5, "CLOUD": 17.25, "TRACE": 9.75}, "fictional": true})
		case r.Method == "GET" && r.URL.Path == "/api/recommendations":
			out, e := a.Recommendations(ctx)
			if e != nil {
				a.T.Log(ctx, "dependency.fallback", "recommender unavailable; bounded fixture fallback")
				out = fixtureRecommendations()
			}
			a.json(w, 200, out)
		case r.Method == "GET" && r.URL.Path == "/api/reviews":
			out, e := a.Store.Reviews(ctx)
			if e != nil {
				a.json(w, 503, map[string]string{"error": "review read failed"})
				return
			}
			sort.Slice(out, func(i, j int) bool { return out[i].Created > out[j].Created })
			if len(out) > 25 {
				out = out[:25]
			}
			a.json(w, 200, out)
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/api/reviews/"):
			var v Review
			if a.Store.Load(ctx, "review#"+strings.TrimPrefix(r.URL.Path, "/api/reviews/"), &v) != nil {
				a.json(w, 404, map[string]string{"error": "unknown review"})
				return
			}
			a.json(w, 200, v)
		case r.Method == "POST" && r.URL.Path == "/api/reviews":
			var b struct {
				Product string `json:"product_id"`
				Text    string `json:"text"`
				Rating  int    `json:"rating"`
			}
			if json.NewDecoder(io.LimitReader(r.Body, 2048)).Decode(&b) != nil {
				a.json(w, 400, map[string]string{"error": "invalid review"})
				return
			}
			v, e := a.Submit(ctx, b.Product, b.Text, b.Rating)
			if e != nil {
				a.json(w, 400, map[string]string{"error": e.Error()})
				return
			}
			a.T.Log(ctx, "review.submitted", "seeded review queued", attribute.String("review.id", v.ID))
			a.json(w, 202, v)
		case r.Method == "GET" && r.URL.Path == "/api/thumbnail":
			b, e := a.Store.Thumbnail(ctx)
			if e != nil {
				a.json(w, 503, map[string]string{"error": "thumbnail unavailable"})
				return
			}
			w.Header().Set("Content-Type", "image/svg+xml")
			_, _ = w.Write(b)
		case r.Method == "GET" && !strings.HasPrefix(r.URL.Path, "/api/") && !strings.HasPrefix(r.URL.Path, "/internal/"):
			a.frontend(w, r)
		default:
			a.json(w, 404, map[string]string{"error": "not found"})
		}
	})
}
func routeName(p string) string {
	if strings.HasPrefix(p, "/api/reviews/") {
		return "/api/reviews/{id}"
	}
	if strings.HasPrefix(p, "/assets/") {
		return "/assets/{asset}"
	}
	return p
}
func envProduct(p string) string {
	if p == "" {
		return "demo-product"
	}
	return p
}
func fixtureRecommendations() map[string]any {
	return map[string]any{"items": []map[string]any{{"id": "demo-product", "score": 0.9}}, "model_revision": "fixture-v1"}
}
func (a *App) Recommendations(ctx context.Context) (out map[string]any, err error) {
	err = a.T.Operation(ctx, "recommender.GET", trace.SpanKindClient, func(ctx context.Context) error {
		if a.C.RecommenderURL == "" {
			out = fixtureRecommendations()
			trace.SpanFromContext(ctx).SetAttributes(attribute.Bool("dependency.fixture", true))
			return nil
		}
		u, _ := url.Parse(a.C.RecommenderURL)
		u.Path = strings.TrimSuffix(u.Path, "/") + "/api/recommendations"
		q := u.Query()
		q.Set("product_id", "demo-product")
		u.RawQuery = q.Encode()
		req, e := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
		if e != nil {
			return e
		}
		otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
		if a.C.DownstreamToken != "" {
			req.Header.Set("Authorization", "Bearer "+a.C.DownstreamToken)
		}
		client := &http.Client{Timeout: 1500 * time.Millisecond, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		resp, e := client.Do(req)
		if e != nil {
			return e
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			return fmt.Errorf("dependency status %d", resp.StatusCode)
		}
		return json.NewDecoder(io.LimitReader(resp.Body, 32768)).Decode(&out)
	})
	return
}
func (a *App) frontend(w http.ResponseWriter, r *http.Request) {
	scriptURL, _ := url.Parse(a.C.RUMScript)
	ingestURL, _ := url.Parse(a.C.Endpoint)
	w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self' "+scriptURL.Scheme+"://"+scriptURL.Host+"; connect-src 'self' "+ingestURL.Scheme+"://"+ingestURL.Host+"; img-src 'self' data:; style-src 'self'; base-uri 'none'; frame-ancestors 'none'")
	root := env("WEB_ROOT", "web/dist")
	path := filepath.Join(root, filepath.Clean("/"+r.URL.Path))
	if r.URL.Path != "/" {
		if info, e := os.Stat(path); e == nil && !info.IsDir() {
			http.ServeFile(w, r, path)
			return
		}
	}
	b, e := os.ReadFile(filepath.Join(root, "index.html"))
	if e != nil {
		http.Error(w, "Build UI first: npm run build", 503)
		return
	}
	rum := ""
	if a.C.RUMKey != "" {
		u, _ := url.Parse(a.C.RUMScript)
		q := u.Query()
		q.Set("ckid", a.C.RUMKey)
		u.RawQuery = q.Encode()
		attr := func(s string) string { return html.EscapeString(s) }
		rum = fmt.Sprintf(`<script src="%s" data-endpoint="%s" data-service="sample-reviews-web" data-service-namespace="%s" data-env="%s" data-version="%s" data-trace-sample-rate="1" data-long-tasks="true" data-trace-origins="%s"></script>`, attr(u.String()), attr(a.C.Endpoint), attr(a.C.Namespace), attr(a.C.Environment), attr(a.C.Version), attr(a.C.Origin))
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = io.WriteString(w, strings.Replace(string(b), "<!--RUM-->", rum, 1))
}
