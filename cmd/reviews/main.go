package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/corridora/sample-reviews/internal/app"
)

var buildVersion = "local"

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if arn := os.Getenv("CONFIG_SECRET_ARN"); arn != "" {
		values, e := app.LoadSecret(ctx, os.Getenv("AWS_REGION"), arn)
		if e != nil {
			fmt.Fprintln(os.Stderr, "cannot load configuration secret")
			os.Exit(1)
		}
		for _, k := range []string{"CORRIDORA_INGEST_KEY", "CORRIDORA_RUM_KEY", "DEMO_CONTROL_TOKEN", "DOWNSTREAM_AUTH_TOKEN"} {
			_ = os.Setenv(k, values[k])
		}
	}
	if buildVersion != "local" || os.Getenv("SERVICE_VERSION") == "" {
		_ = os.Setenv("SERVICE_VERSION", buildVersion)
	}
	c, e := app.LoadConfig()
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
	t, e := app.NewTelemetry(ctx, c)
	if e != nil {
		fmt.Fprintln(os.Stderr, "telemetry initialization failed")
		os.Exit(1)
	}
	defer t.Shutdown(context.Background())
	var store app.Store
	if c.Mode == "cloud" {
		store, e = app.NewAWSStore(ctx, c, t)
		if e != nil {
			fmt.Fprintln(os.Stderr, "AWS store initialization failed")
			os.Exit(1)
		}
	} else {
		store = app.NewMemoryStore()
	}
	a := app.New(c, t, store)
	if os.Getenv("AWS_LAMBDA_RUNTIME_API") != "" {
		lambda.Start(a.Lambda)
		return
	}
	if m, ok := store.(*app.MemoryStore); ok {
		a.LocalWorker(ctx, m)
	}
	server := &http.Server{Addr: "127.0.0.1:" + c.Port, Handler: a.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8192}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	fmt.Println("Reviews: http://" + server.Addr)
	if e := server.ListenAndServe(); e != nil && e != http.ErrServerClosed {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
	t.Flush()
}
