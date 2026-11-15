package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Inn-Keeper/linkcheck"
)

func testConfig() config {
	return config{
		timeout:      5 * time.Second,
		maxDepth:     2,
		maxURLs:      50,
		maxCrawls:    4,
		ratePerMin:   6000, // out of the way unless a test lowers it
		burst:        100,
		allowPrivate: true, // the sites under test listen on loopback
	}
}

func post(h http.Handler, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/checks", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decodeReport(t *testing.T, rec *httptest.ResponseRecorder) linkcheck.Report {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, body %s; want 200", rec.Code, rec.Body)
	}
	var rep linkcheck.Report
	if err := json.Unmarshal(rec.Body.Bytes(), &rep); err != nil {
		t.Fatalf("body is not a report: %v\n%s", err, rec.Body)
	}
	return rep
}

// site serves a page with one good and one broken link.
func site() *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, `<a href="/ok">ok</a> <a href="/missing">bad</a>`)
	})
	mux.HandleFunc("/ok", func(w http.ResponseWriter, r *http.Request) {})
	return httptest.NewServer(mux)
}

func TestCheckReturnsReport(t *testing.T) {
	target := site()
	defer target.Close()

	rep := decodeReport(t, post(newHandler(testConfig()), `{"url": "`+target.URL+`"}`))
	if rep.Checked != 3 || len(rep.Broken) != 1 || rep.Broken[0].Status != http.StatusNotFound {
		t.Errorf("report = %+v, want 3 checked and one 404", rep)
	}
}

func TestPrivateTargetsRefusedByDefault(t *testing.T) {
	target := site()
	defer target.Close()

	cfg := testConfig()
	cfg.allowPrivate = false
	rep := decodeReport(t, post(newHandler(cfg), `{"url": "`+target.URL+`"}`))
	if len(rep.Broken) != 1 || !strings.Contains(rep.Broken[0].Err, "not public") {
		t.Errorf("report = %+v, want the loopback target refused", rep)
	}
}

func TestBadRequests(t *testing.T) {
	h := newHandler(testConfig())
	tests := []struct {
		name, body string
		want       int
	}{
		{"not json", `nope`, http.StatusBadRequest},
		{"unknown field", `{"url": "http://example.com", "workers": 999}`, http.StatusBadRequest},
		{"depth over cap", `{"url": "http://example.com", "depth": 3}`, http.StatusBadRequest},
		{"negative depth", `{"url": "http://example.com", "depth": -1}`, http.StatusBadRequest},
		{"missing url", `{}`, http.StatusBadRequest},
		{"non-http url", `{"url": "file:///etc/passwd"}`, http.StatusBadRequest},
		{"oversized body", `{"url": "http://example.com/` + strings.Repeat("a", maxRequestBody) + `"}`, http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := post(h, tt.body)
			if rec.Code != tt.want || !strings.Contains(rec.Body.String(), `"error"`) {
				t.Errorf("status %d, body %s; want %d with an error", rec.Code, rec.Body, tt.want)
			}
		})
	}

	t.Run("wrong content type", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/checks", strings.NewReader(`{"url": "http://example.com"}`))
		req.Header.Set("Content-Type", "text/plain")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnsupportedMediaType {
			t.Errorf("status %d, want 415", rec.Code)
		}
	})
}

func TestRateLimitIsPerIP(t *testing.T) {
	cfg := testConfig()
	cfg.ratePerMin, cfg.burst = 1, 2
	h := newHandler(cfg)

	from := func(remoteAddr string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/v1/checks", strings.NewReader(`{}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Forwarded-For", "203.0.113.99") // must not buy a fresh bucket
		req.RemoteAddr = remoteAddr
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	for i := range 2 {
		if rec := from("198.51.100.1:1000"); rec.Code == http.StatusTooManyRequests {
			t.Fatalf("request %d limited, want the burst of 2 allowed", i+1)
		}
	}
	rec := from("198.51.100.1:2000") // same IP, another connection
	if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") == "" {
		t.Errorf("status %d, Retry-After %q; want 429 with Retry-After", rec.Code, rec.Header().Get("Retry-After"))
	}
	if rec := from("198.51.100.2:1000"); rec.Code == http.StatusTooManyRequests {
		t.Error("a different IP was limited")
	}

	// IPv6 clients are limited per /64.
	from("[2001:db8:1:2::1]:1000")
	from("[2001:db8:1:2::2]:1000")
	if rec := from("[2001:db8:1:2::3]:1000"); rec.Code != http.StatusTooManyRequests {
		t.Errorf("status %d for a third address in one /64, want 429", rec.Code)
	}
}

// hangingSite never answers, and reports each request it receives on hit.
func hangingSite(t *testing.T) (target *httptest.Server, hit chan struct{}) {
	block := make(chan struct{})
	hit = make(chan struct{}, 16)
	target = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hit <- struct{}{}
		<-block
	}))
	t.Cleanup(target.Close)
	t.Cleanup(func() { close(block) }) // runs first, releasing the handlers
	return target, hit
}

func TestDeadlineReturnsPartialReport(t *testing.T) {
	target, _ := hangingSite(t)
	cfg := testConfig()
	cfg.timeout = 100 * time.Millisecond

	start := time.Now()
	rep := decodeReport(t, post(newHandler(cfg), `{"url": "`+target.URL+`"}`))
	if !rep.Interrupted {
		t.Errorf("report = %+v, want interrupted", rep)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("answered after %v, want close to the 100ms deadline", elapsed)
	}
}

func TestBusyServerAnswers503(t *testing.T) {
	target, hit := hangingSite(t)
	cfg := testConfig()
	cfg.maxCrawls = 1
	cfg.timeout = time.Second
	h := newHandler(cfg)

	first := make(chan struct{})
	go func() {
		defer close(first)
		post(h, `{"url": "`+target.URL+`"}`)
	}()
	<-hit // the first crawl now holds the only slot

	rec := post(h, `{"url": "`+target.URL+`"}`)
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") == "" {
		t.Errorf("status %d, Retry-After %q; want 503 with Retry-After", rec.Code, rec.Header().Get("Retry-After"))
	}
	<-first
}
