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
	"math"
	"mime"
	"net/http"
	"net/netip"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Inn-Keeper/linkcheck"
	"golang.org/x/time/rate"
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
	ratePerMin   float64       // checks per minute per client address
	burst        int           // checks a client may make back to back
	origins      []string      // browser origins allowed by CORS; "*" for any
	allowPrivate bool          // local development only: lets callers reach internal addresses
}

// ipLimiter gives every client address its own token bucket.
// ponytail: in-memory and per-process; move to a shared store if this runs
// as more than one instance.
type ipLimiter struct {
	limit rate.Limit
	burst int
	ttl   time.Duration // idle time after which a bucket is full again and can go

	mu        sync.Mutex
	clients   map[netip.Addr]*client
	lastSweep time.Time
}

type client struct {
	lim  *rate.Limiter
	seen time.Time
}

func newIPLimiter(perMinute float64, burst int) *ipLimiter {
	limit := rate.Limit(perMinute / 60)
	return &ipLimiter{
		limit:   limit,
		burst:   burst,
		ttl:     time.Duration(float64(burst)/float64(limit)*float64(time.Second)) + time.Minute,
		clients: map[netip.Addr]*client{},
	}
}

// wait returns how long addr must wait before its next check; 0 means go.
func (l *ipLimiter) wait(addr netip.Addr) time.Duration {
	addr = addr.Unmap()
	if addr.Is6() {
		// One subscriber usually controls a whole /64, so limit the block.
		if p, err := addr.Prefix(64); err == nil {
			addr = p.Addr()
		}
	}
	now := time.Now()

	l.mu.Lock()
	if now.Sub(l.lastSweep) > time.Minute {
		for a, c := range l.clients {
			if now.Sub(c.seen) > l.ttl {
				delete(l.clients, a)
			}
		}
		l.lastSweep = now
	}
	c := l.clients[addr]
	if c == nil {
		c = &client{lim: rate.NewLimiter(l.limit, l.burst)}
		l.clients[addr] = c
	}
	c.seen = now
	l.mu.Unlock()

	r := c.lim.ReserveN(now, 1)
	d := r.DelayFrom(now)
	if d > 0 {
		r.CancelAt(now) // refused, so give the token back
	}
	return d
}

// clientIP is the peer address of the connection. Forwarding headers are
// ignored on purpose: with no trusted proxy in front, callers could forge
// them to dodge the limit.
// ponytail: behind a load balancer every caller shares its address; read
// the balancer's header here once there is one.
func clientIP(r *http.Request) netip.Addr {
	ap, err := netip.ParseAddrPort(r.RemoteAddr)
	if err != nil {
		return netip.Addr{} // unparseable peers share one bucket
	}
	return ap.Addr()
}

type checkRequest struct {
	URL   string `json:"url"`
	Depth *int   `json:"depth"` // optional; defaults to 1
}

type server struct {
	cfg     config
	slots   chan struct{} // one token per running crawl
	limiter *ipLimiter
}

func newHandler(cfg config) http.Handler {
	s := &server{
		cfg:     cfg,
		slots:   make(chan struct{}, cfg.maxCrawls),
		limiter: newIPLimiter(cfg.ratePerMin, cfg.burst),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/checks", s.check)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	return cors(cfg.origins, mux)
}

// cors lets browser pages on the allowed origins call the API. "*" allows
// any origin. Other origins get no CORS headers, so the browser refuses the
// preflight and the POST is never sent.
func cors(origins []string, next http.Handler) http.Handler {
	if len(origins) == 0 {
		return next
	}
	allowAll := slices.Contains(origins, "*")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Add("Vary", "Origin") // responses differ by origin; keep caches honest
		origin := r.Header.Get("Origin")
		if origin == "" || !(allowAll || slices.Contains(origins, origin)) {
			next.ServeHTTP(w, r)
			return
		}
		h.Set("Access-Control-Allow-Origin", origin)
		h.Set("Access-Control-Expose-Headers", "Retry-After")
		if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
			h.Set("Access-Control-Allow-Methods", "POST")
			h.Set("Access-Control-Allow-Headers", "Content-Type")
			h.Set("Access-Control-Max-Age", "600")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// parseOrigins splits the -cors-origins flag: comma-separated, spaces and a
// trailing slash forgiven.
func parseOrigins(s string) []string {
	var out []string
	for _, o := range strings.Split(s, ",") {
		if o = strings.TrimSuffix(strings.TrimSpace(o), "/"); o != "" {
			out = append(out, o)
		}
	}
	return out
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
	// First, before any work: every request counts, valid or not.
	if d := s.limiter.wait(clientIP(r)); d > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(d.Seconds()))))
		writeError(w, http.StatusTooManyRequests, "rate limit exceeded")
		return
	}
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
	ratePerMin := flag.Float64("rate", 6, "checks per minute allowed per client IP")
	burst := flag.Int("burst", 3, "checks a client IP may make back to back")
	origins := flag.String("cors-origins", "", `comma-separated browser origins allowed to call the API, e.g. "https://app.example.com"; "*" allows any`)
	allowPrivate := flag.Bool("allow-private", false, "UNSAFE: allow crawling private/loopback addresses (local development)")
	flag.Parse()

	if *timeout <= 0 || *maxDepth < 0 || *maxURLs < 1 || *maxCrawls < 1 || *ratePerMin <= 0 || *burst < 1 {
		log.Fatal("linkcheckd: -timeout, -max-urls, -max-crawls, -rate and -burst must be positive, -max-depth not negative")
	}
	cfg := config{
		timeout:      *timeout,
		maxDepth:     *maxDepth,
		maxURLs:      *maxURLs,
		maxCrawls:    *maxCrawls,
		ratePerMin:   *ratePerMin,
		burst:        *burst,
		origins:      parseOrigins(*origins),
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
