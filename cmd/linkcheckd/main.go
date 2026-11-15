// Command linkcheckd serves linkcheck over HTTP to untrusted callers.
//
//	POST /v1/checks   {"url": "https://example.com", "depth": 1}
//	GET  /healthz
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"mime"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Inn-Keeper/linkcheck"
)

const (
	maxRequestBody = 4 << 10 // a url and a depth fit easily
	crawlRPS       = 10      // per target host, so we stay polite to the sites we crawl
	busyRetryAfter = "5"     // seconds
)

type config struct {
	timeout      time.Duration // hard deadline per crawl
	maxDepth     int           // highest depth a caller may ask for
	maxURLs      int           // urls checked per crawl
	maxCrawls    int           // crawls running at once, server-wide
	allowPrivate bool          // local development only: lets callers reach internal addresses
}

type checkRequest struct {
	URL   string `json:"url"`
	Depth *int   `json:"depth"` // optional; defaults to 1
}

type server struct {
	cfg   config
	slots chan struct{} // one token per running crawl
}

func newHandler(cfg config) http.Handler {
	s := &server{cfg: cfg, slots: make(chan struct{}, cfg.maxCrawls)}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/checks", s.check)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func (s *server) check(w http.ResponseWriter, r *http.Request) {
	// Requiring JSON also forces browsers to preflight, so a page on another
	// origin cannot trigger a crawl with a plain form post.
	if mt, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type")); mt != "application/json" {
		writeError(w, http.StatusUnsupportedMediaType, "Content-Type must be application/json")
		return
	}
	var req checkRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	depth := 1
	if req.Depth != nil {
		depth = *req.Depth
	}
	if depth < 0 || depth > s.cfg.maxDepth {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("depth must be between 0 and %d", s.cfg.maxDepth))
		return
	}

	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		w.Header().Set("Retry-After", busyRetryAfter)
		writeError(w, http.StatusServiceUnavailable, "server is busy, try again shortly")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.timeout)
	defer cancel()
	start := time.Now()
	rep, err := linkcheck.Check(ctx, req.URL, linkcheck.Options{
		MaxDepth:            depth,
		RPS:                 crawlRPS,
		MaxURLs:             s.cfg.maxURLs,
		DenyPrivateNetworks: !s.cfg.allowPrivate,
	})
	switch {
	case r.Context().Err() != nil:
		return // the client went away; nobody to answer
	case err != nil && !errors.Is(err, context.DeadlineExceeded):
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	// Hitting the deadline is not an error: the report says interrupted.
	log.Printf("check %q from %s: %d checked, %d broken, interrupted=%t, %s",
		req.URL, r.RemoteAddr, rep.Checked, len(rep.Broken), rep.Interrupted, time.Since(start).Round(time.Millisecond))
	writeJSON(w, http.StatusOK, rep)
}

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	timeout := flag.Duration("timeout", 25*time.Second, "hard deadline per crawl; keep it under your load balancer's")
	maxDepth := flag.Int("max-depth", 2, "highest crawl depth a caller may request")
	maxURLs := flag.Int("max-urls", 200, "most urls checked per crawl")
	maxCrawls := flag.Int("max-crawls", 8, "crawls running at once; further requests get 503")
	allowPrivate := flag.Bool("allow-private", false, "UNSAFE: allow crawling private/loopback addresses (local development)")
	flag.Parse()

	if *timeout <= 0 || *maxDepth < 0 || *maxURLs < 1 || *maxCrawls < 1 {
		log.Fatal("linkcheckd: -timeout, -max-urls and -max-crawls must be positive, -max-depth not negative")
	}
	cfg := config{
		timeout:      *timeout,
		maxDepth:     *maxDepth,
		maxURLs:      *maxURLs,
		maxCrawls:    *maxCrawls,
		allowPrivate: *allowPrivate,
	}
	srv := &http.Server{
		Addr:              *addr,
		Handler:           newHandler(cfg),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      cfg.timeout + 10*time.Second, // a crawl plus writing its report
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		<-ctx.Done()
		// Let running crawls reach their deadline and answer.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.timeout+5*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()

	log.Printf("linkcheckd listening on %s", *addr)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
	<-drained
}
