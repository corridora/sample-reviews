// Local OTLP capture for honest validation. Never forwards records to a tenant.
package main

import (
	"encoding/json"
	"fmt"
	logpb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	metricpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"io"
	"net/http"
	"os"
	"sync"
)

func main() {
	file, e := os.OpenFile(os.Getenv("CAPTURE_FILE"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if e != nil {
		panic(e)
	}
	defer file.Close()
	var mu sync.Mutex
	http.HandleFunc("/corridora-rum.js", func(w http.ResponseWriter, r *http.Request) { http.ServeFile(w, r, os.Getenv("RUM_BUNDLE_PATH")) })
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", r.Header.Get("Origin"))
		w.Header().Set("Vary", "Origin")
		w.Header().Set("Access-Control-Allow-Methods", "POST,OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "content-type,x-corridora-key,x-corridora-ingest-source")
		if r.Method == "OPTIONS" {
			w.WriteHeader(204)
			return
		}
		if r.Method == "GET" && r.URL.Path == "/healthz" {
			w.WriteHeader(200)
			return
		}
		body, e := io.ReadAll(io.LimitReader(r.Body, 4<<20))
		if e != nil {
			w.WriteHeader(400)
			return
		}
		if r.Header.Get("Content-Type") == "application/x-protobuf" {
			var record proto.Message
			switch r.URL.Path {
			case "/v1/traces":
				record = &tracepb.ExportTraceServiceRequest{}
			case "/v1/logs":
				record = &logpb.ExportLogsServiceRequest{}
			case "/v1/metrics":
				record = &metricpb.ExportMetricsServiceRequest{}
			default:
				w.WriteHeader(404)
				return
			}
			if e = proto.Unmarshal(body, record); e != nil {
				w.WriteHeader(400)
				return
			}
			body, e = protojson.Marshal(record)
			if e != nil {
				w.WriteHeader(400)
				return
			}
		}
		var data any
		if json.Unmarshal(body, &data) != nil {
			w.WriteHeader(400)
			return
		}
		mu.Lock()
		_ = json.NewEncoder(file).Encode(map[string]any{"path": r.URL.Path, "rum": r.Header.Get("X-Corridora-Ingest-Source") == "rum", "data": data})
		_ = file.Sync()
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	})
	fmt.Println("Local capture ready")
	if e = http.ListenAndServe("127.0.0.1:"+os.Getenv("CAPTURE_PORT"), nil); e != nil {
		panic(e)
	}
}
