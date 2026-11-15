package linkcheck

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestRetryRecoversFromTransientFailure(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html")
	}))
	defer srv.Close()

	root, _ := url.Parse(srv.URL)
	c := newCrawler(root, 1, 5*time.Second)
	c.backoff = time.Millisecond
	_, broken := c.run(context.Background(), 2)

	if len(broken) != 0 {
		t.Errorf("got broken %v after recovery, want none", broken)
	}
	if got := calls.Load(); got != 3 {
		t.Errorf("server saw %d requests, want 3", got)
	}
}

func TestRetryGivesUpAfterMaxAttempts(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	root, _ := url.Parse(srv.URL)
	c := newCrawler(root, 1, 5*time.Second)
	c.backoff = time.Millisecond
	_, broken := c.run(context.Background(), 2)

	if len(broken) != 1 || broken[0].Status != http.StatusInternalServerError {
		t.Fatalf("got broken %v, want one HTTP 500", broken)
	}
	if got := calls.Load(); got != maxAttempts {
		t.Errorf("server saw %d requests, want %d", got, maxAttempts)
	}
}

func TestRateLimitSpacesRequests(t *testing.T) {
	mux := http.NewServeMux()
	page := func(html string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/html")
			fmt.Fprint(w, html)
		}
	}
	mux.HandleFunc("/{$}", page(`<a href="/a">a</a> <a href="/b">b</a>`))
	mux.HandleFunc("/a", page(``))
	mux.HandleFunc("/b", page(``))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	root, _ := url.Parse(srv.URL)
	c := newCrawler(root, 2, 5*time.Second)
	c.rps = 10 // 3 urls at 10 rps, burst 1 → at least ~200ms

	start := time.Now()
	checked, broken := c.run(context.Background(), 4)
	elapsed := time.Since(start)

	if checked != 3 || len(broken) != 0 {
		t.Fatalf("checked %d, broken %v; want 3 checked, none broken", checked, broken)
	}
	if elapsed < 150*time.Millisecond {
		t.Errorf("crawl took %v, want >= 150ms with rate limiting", elapsed)
	}
}

func TestReportJSONRoundTrip(t *testing.T) {
	rep := Report{
		Checked: 3,
		Broken: []Result{
			{URL: "http://x/a", Status: 404, FoundOn: "http://x/"},
			{URL: "http://x/b", Err: "dial tcp: no such host", FoundOn: "http://x/"},
		},
	}
	out, err := json.Marshal(rep)
	if err != nil {
		t.Fatal(err)
	}
	var got Report
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, out)
	}
	if !reflect.DeepEqual(got, rep) {
		t.Errorf("round-trip mismatch:\ngot  %+v\nwant %+v", got, rep)
	}
}

func TestCheck(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/{$}", htmlPage(`<a href="/ok">ok</a> <a href="/missing">bad</a>`))
	mux.HandleFunc("/ok", htmlPage(``))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	var calls int
	rep, err := Check(context.Background(), srv.URL, Options{
		MaxDepth:   1,
		OnProgress: func(checked, broken int) { calls++ },
	})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Checked != 3 || len(rep.Broken) != 1 || rep.Broken[0].Status != http.StatusNotFound || rep.Interrupted {
		t.Errorf("report = %+v, want 3 checked, one 404, not interrupted", rep)
	}
	if calls != 3 {
		t.Errorf("OnProgress called %d times, want 3", calls)
	}
}

func TestCheckRejectsBadInput(t *testing.T) {
	tests := []struct {
		name, url string
		opts      Options
	}{
		{"no host", "not a url", Options{}},
		{"no scheme", "example.com", Options{}},
		{"non-http scheme", "ftp://example.com/", Options{}},
		{"file scheme", "file:///etc/passwd", Options{}},
		{"negative workers", "http://example.com/", Options{Workers: -1}},
		{"negative depth", "http://example.com/", Options{MaxDepth: -1}},
		{"negative timeout", "http://example.com/", Options{Timeout: -time.Second}},
		{"negative rps", "http://example.com/", Options{RPS: -1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Check(context.Background(), tt.url, tt.opts); err == nil {
				t.Error("Check accepted bad input")
			}
		})
	}
}

func TestCheckCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rep, err := Check(ctx, "http://example.invalid/", Options{})
	if !errors.Is(err, context.Canceled) || !rep.Interrupted {
		t.Errorf("got report %+v, err %v; want Interrupted and context.Canceled", rep, err)
	}
}

func TestCrawlFindsBrokenLinks(t *testing.T) {
	mux := http.NewServeMux()
	page := func(html string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/html")
			fmt.Fprint(w, html)
		}
	}
	mux.HandleFunc("/{$}", page(`<a href="/ok">ok</a> <a href="/missing">bad</a>`))
	mux.HandleFunc("/ok", page(`<a href="/">home</a> <a href="/also-missing">bad2</a>`))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	root, _ := url.Parse(srv.URL)
	checked, broken := newCrawler(root, 3, 5*time.Second).run(context.Background(), 4)

	if checked != 4 {
		t.Errorf("checked %d urls, want 4", checked)
	}
	if len(broken) != 2 {
		t.Fatalf("got %d broken links %v, want 2", len(broken), broken)
	}
	if !strings.HasSuffix(broken[0].URL, "/also-missing") || broken[0].Status != http.StatusNotFound {
		t.Errorf("broken[0] = %+v, want 404 on /also-missing", broken[0])
	}
	if !strings.HasSuffix(broken[1].URL, "/missing") || broken[1].Status != http.StatusNotFound {
		t.Errorf("broken[1] = %+v, want 404 on /missing", broken[1])
	}
}

func TestCrawlAllGood(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, `<a href="/">self</a>`)
	}))
	defer srv.Close()

	root, _ := url.Parse(srv.URL)
	checked, broken := newCrawler(root, 2, 5*time.Second).run(context.Background(), 4)
	if checked != 1 || len(broken) != 0 {
		t.Fatalf("checked %d, broken %v; want 1 checked, none broken", checked, broken)
	}
	out, _ := json.Marshal(Report{Checked: checked, Broken: broken})
	if !strings.Contains(string(out), `"broken":[]`) {
		t.Errorf("clean run JSON = %s, want an empty broken array", out)
	}
}

func TestCancelStopsCrawl(t *testing.T) {
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-block // hang every request until the test ends
	}))
	defer srv.Close()
	defer close(block)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	root, _ := url.Parse(srv.URL)
	start := time.Now()
	_, broken := newCrawler(root, 2, 30*time.Second).run(ctx, 2)

	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("run took %v after cancel, want prompt return", elapsed)
	}
	if len(broken) != 0 {
		t.Errorf("cancelled requests reported as broken: %v", broken)
	}
}

func htmlPage(body string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, body)
	}
}

func TestRedirectResolvesLinksAgainstFinalURL(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/{$}", htmlPage(`<a href="/docs">docs</a>`))
	mux.HandleFunc("/docs", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/docs/", http.StatusMovedPermanently)
	})
	mux.HandleFunc("/docs/{$}", htmlPage(`<a href="page.html">page</a>`))
	mux.HandleFunc("/docs/page.html", htmlPage(``))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	root, _ := url.Parse(srv.URL)
	checked, broken := newCrawler(root, 3, 5*time.Second).run(context.Background(), 4)
	if checked != 3 || len(broken) != 0 {
		t.Fatalf("checked %d, broken %v; want 3 checked, none broken", checked, broken)
	}
}

func TestSendsUserAgent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.UserAgent() != defaultUserAgent {
			w.WriteHeader(http.StatusForbidden)
		}
	}))
	defer srv.Close()

	root, _ := url.Parse(srv.URL)
	_, broken := newCrawler(root, 0, 5*time.Second).run(context.Background(), 1)
	if len(broken) != 0 {
		t.Fatalf("server rejected the User-Agent: %v", broken)
	}
}

func TestRetryHonoursRetryAfter(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
		}
	}))
	defer srv.Close()

	root, _ := url.Parse(srv.URL)
	c := newCrawler(root, 0, 5*time.Second)
	c.backoff = time.Millisecond
	start := time.Now()
	_, broken := c.run(context.Background(), 1)

	if len(broken) != 0 {
		t.Errorf("got broken %v, want recovery after Retry-After", broken)
	}
	if elapsed := time.Since(start); elapsed < 900*time.Millisecond {
		t.Errorf("retried after %v, want >= 1s from Retry-After", elapsed)
	}
}

func TestMissingHostIsNotRetried(t *testing.T) {
	notFound := &url.Error{Op: "Get", Err: &net.OpError{Op: "dial", Err: &net.DNSError{IsNotFound: true}}}
	if retryable(nil, notFound) {
		t.Error("DNS not-found is retried, want a single attempt")
	}
	if !retryable(nil, &url.Error{Op: "Get", Err: errors.New("connection reset")}) {
		t.Error("connection reset is not retried, want a retry")
	}
}

func TestDepthLimitStopsCrawl(t *testing.T) {
	var deepHits atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/{$}", htmlPage(`<a href="/1">1</a>`))
	mux.HandleFunc("/1", htmlPage(`<a href="/2">2</a>`))
	mux.HandleFunc("/2", htmlPage(`<a href="/3">3</a>`))
	mux.HandleFunc("/3", func(w http.ResponseWriter, r *http.Request) { deepHits.Add(1) })
	srv := httptest.NewServer(mux)
	defer srv.Close()

	root, _ := url.Parse(srv.URL)
	checked, _ := newCrawler(root, 2, 5*time.Second).run(context.Background(), 4)
	// Depth 2 checks /2 but does not read its links.
	if checked != 3 || deepHits.Load() != 0 {
		t.Fatalf("checked %d, /3 fetched %d times; want 3 checked, /3 never fetched", checked, deepHits.Load())
	}
}

func TestExternalLinksCheckedNotCrawled(t *testing.T) {
	var deepHits atomic.Int32
	extMux := http.NewServeMux()
	extMux.HandleFunc("/page", htmlPage(`<a href="/deeper">x</a>`))
	extMux.HandleFunc("/deeper", func(w http.ResponseWriter, r *http.Request) { deepHits.Add(1) })
	ext := httptest.NewServer(extMux) // different port, so a different host
	defer ext.Close()

	srv := httptest.NewServer(htmlPage(`<a href="` + ext.URL + `/page">ext</a>`))
	defer srv.Close()

	root, _ := url.Parse(srv.URL)
	checked, broken := newCrawler(root, 5, 5*time.Second).run(context.Background(), 4)
	if checked != 2 || len(broken) != 0 || deepHits.Load() != 0 {
		t.Fatalf("checked %d, broken %v, external /deeper fetched %d times; want 2 checked, none broken, never fetched",
			checked, broken, deepHits.Load())
	}
}

func TestDenyPrivateNetworksBlocksLoopback(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
	}))
	defer srv.Close() // listens on 127.0.0.1

	rep, err := Check(context.Background(), srv.URL, Options{DenyPrivateNetworks: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Broken) != 1 || rep.Broken[0].Err != errBlocked.Error() {
		t.Errorf("report = %+v, want the loopback url refused without echoing the address", rep)
	}
	if hits.Load() != 0 {
		t.Errorf("loopback server got %d requests, want 0", hits.Load())
	}
}

func TestMaxURLsTruncates(t *testing.T) {
	var links strings.Builder
	for i := range 10 {
		fmt.Fprintf(&links, `<a href="/p%d">x</a>`, i)
	}
	srv := httptest.NewServer(htmlPage(links.String()))
	defer srv.Close()

	rep, err := Check(context.Background(), srv.URL, Options{MaxDepth: 1, MaxURLs: 3})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Checked != 3 || !rep.Truncated {
		t.Errorf("report = %+v, want 3 checked and Truncated", rep)
	}
}

func TestPublicAddr(t *testing.T) {
	tests := []struct {
		addr string
		want bool
	}{
		{"8.8.8.8", true},
		{"2606:4700:4700::1111", true},
		{"127.0.0.1", false},
		{"::1", false},
		{"::ffff:127.0.0.1", false}, // IPv4-mapped loopback
		{"10.0.0.1", false},
		{"172.16.0.1", false},
		{"192.168.1.1", false},
		{"169.254.169.254", false}, // cloud metadata
		{"100.64.0.1", false},
		{"0.0.0.0", false},
		{"fd00::1", false},
		{"fe80::1", false},
		{"64:ff9b::a00:1", false}, // NAT64 for 10.0.0.1
		{"224.0.0.1", false},
		{"255.255.255.255", false},
	}
	for _, tt := range tests {
		if got := publicAddr(netip.MustParseAddr(tt.addr)); got != tt.want {
			t.Errorf("publicAddr(%s) = %v, want %v", tt.addr, got, tt.want)
		}
	}
}

func FuzzExtractLinks(f *testing.F) {
	for _, seed := range []string{
		`<a href="other.html">x</a>`,
		`<a href='/top#sec'>x</a>`,
		`<a href="mailto:a@b.c">x</a>`,
		`<a href="http://other.org/p">x</a>`,
		`<a href="http:///no-host">x</a>`,
		`href = " ://%%bad% "`,
		`no links here`,
	} {
		f.Add(seed)
	}
	base, _ := url.Parse("https://example.com/dir/page.html")
	f.Fuzz(func(t *testing.T, body string) {
		// Must never panic, and every extracted URL must be fetchable-shaped.
		for _, u := range extractLinks(base, body) {
			if u.Scheme != "http" && u.Scheme != "https" {
				t.Errorf("bad scheme %q from body %q", u.Scheme, body)
			}
			if u.Host == "" {
				t.Errorf("empty host in %q from body %q", u, body)
			}
			if u.Fragment != "" {
				t.Errorf("fragment kept in %q from body %q", u, body)
			}
		}
	})
}

func TestExtractLinks(t *testing.T) {
	base, _ := url.Parse("https://example.com/dir/page.html")
	tests := []struct {
		name, body string
		want       []string
	}{
		{"relative", `<a href="other.html">x</a>`, []string{"https://example.com/dir/other.html"}},
		{"rooted", `<a href="/top">x</a>`, []string{"https://example.com/top"}},
		{"absolute", `<a href="http://other.org/p">x</a>`, []string{"http://other.org/p"}},
		{"fragment stripped", `<a href="/top#sec">x</a>`, []string{"https://example.com/top"}},
		{"single quotes", `<a href='/top'>x</a>`, []string{"https://example.com/top"}},
		{"mailto skipped", `<a href="mailto:a@b.c">x</a>`, nil},
		{"none", `no links here`, nil},
		{"entities decoded", `<a href="/a?x=1&amp;y=2">x</a>`, []string{"https://example.com/a?x=1&y=2"}},
		{"comment ignored", `<!-- <a href="/old">x</a> -->`, nil},
		{"script ignored", `<script>var s = '<a href="/js">x</a>';</script>`, nil},
		{"src checked", `<img src="/logo.png">`, []string{"https://example.com/logo.png"}},
		{"preconnect skipped", `<link rel="preconnect" href="https://cdn.example.net">`, nil},
		{"dns-prefetch skipped", `<link rel="DNS-Prefetch preload" href="//cdn.example.net">`, nil},
		{"stylesheet kept", `<link rel="stylesheet" href="/s.css">`, []string{"https://example.com/s.css"}},
		{"unquoted", `<a href=/bare>x</a>`, []string{"https://example.com/bare"}},
		{"base href", `<base href="/docs/"><a href="p.html">x</a>`, []string{"https://example.com/docs/p.html"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got []string
			for _, u := range extractLinks(base, tt.body) {
				got = append(got, u.String())
			}
			if fmt.Sprint(got) != fmt.Sprint(tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}
