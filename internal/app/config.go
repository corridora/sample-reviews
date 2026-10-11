package app

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Mode, Port, Endpoint, IngestKey, RUMKey, RUMScript, Service, Namespace, Environment, Version, Region string
	ControlToken, DownstreamToken, RecommenderURL, Origin, DailyStart, Table, Queue, Bucket              string
	Window                                                                                               time.Duration
}

func env(k, d string) string {
	if s := os.Getenv(k); s != "" {
		return s
	}
	return d
}
func LoadConfig() (Config, error) {
	c := Config{Mode: env("SAMPLE_MODE", "local"), Port: env("PORT", "4102"), Endpoint: strings.TrimRight(env("CORRIDORA_OTLP_ENDPOINT", "https://ingest.corridora.com"), "/"), IngestKey: os.Getenv("CORRIDORA_INGEST_KEY"), RUMKey: os.Getenv("CORRIDORA_RUM_KEY"), RUMScript: env("CORRIDORA_RUM_SCRIPT_URL", "https://rum.corridora.com/corridora-rum.js"), Service: env("OTEL_SERVICE_NAME", "sample-reviews-api"), Namespace: env("OTEL_SERVICE_NAMESPACE", "corridora-samples.reviews"), Environment: env("DEPLOYMENT_ENVIRONMENT", "development"), Version: env("SERVICE_VERSION", "local"), Region: env("AWS_REGION", "us-east-1"), ControlToken: os.Getenv("DEMO_CONTROL_TOKEN"), DownstreamToken: os.Getenv("DOWNSTREAM_AUTH_TOKEN"), RecommenderURL: os.Getenv("RECOMMENDER_URL"), Origin: os.Getenv("PUBLIC_ORIGIN"), DailyStart: env("DEMO_DAILY_START_UTC", "13:20"), Table: os.Getenv("REVIEWS_TABLE"), Queue: os.Getenv("MODERATION_QUEUE_URL"), Bucket: os.Getenv("THUMBNAIL_BUCKET")}
	n, err := strconv.Atoi(env("DEMO_WINDOW_SECONDS", "600"))
	if err != nil || n < 1 || n > 600 {
		return c, fmt.Errorf("DEMO_WINDOW_SECONDS must be 1..600")
	}
	c.Window = time.Duration(n) * time.Second
	if _, err := time.Parse("15:04", c.DailyStart); err != nil {
		return c, fmt.Errorf("invalid daily UTC start")
	}
	if c.Origin == "" && c.Mode == "local" {
		c.Origin = "http://127.0.0.1:" + c.Port
	}
	for _, raw := range []string{c.Endpoint, c.RUMScript, c.RecommenderURL, c.Origin} {
		if raw == "" {
			continue
		}
		u, e := url.Parse(raw)
		if e != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return c, fmt.Errorf("invalid endpoint URL")
		}
		if u.Scheme != "https" && !(c.Mode == "local" && u.Scheme == "http" && (u.Hostname() == "localhost" || u.Hostname() == "127.0.0.1")) {
			return c, fmt.Errorf("endpoint requires HTTPS or local loopback")
		}
	}
	if c.ControlToken == "" {
		return c, fmt.Errorf("DEMO_CONTROL_TOKEN is required; run ./setup configure")
	}
	if env("CORRIDORA_TARGET", "own") == "demo" {
		if c.IngestKey == "" || c.RUMKey == "" {
			return c, fmt.Errorf("Demo requires operator-provisioned ingest and RUM keys")
		}
		c.Environment = "demo"
	}
	return c, nil
}
