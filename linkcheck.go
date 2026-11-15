// Package linkcheck crawls a site and reports its broken links.
package linkcheck

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/html"
	"golang.org/x/time/rate"
)

const (
	maxBodySize      = 2 << 20                // don't slurp unbounded pages into memory
	maxAttempts      = 3                      // total tries per url on transient failures
	retryBackoff     = 500 * time.Millisecond // doubles per retry
	maxRetryAfter    = 30 * time.Second       // longest Retry-After we will honour
	defaultUserAgent = "linkcheck/1.0"        // Go's default UA gets blocked a lot
	defaultWorkers   = 8
	defaultTimeout   = 10 * time.Second
)

// Result is the outcome of checking one URL.
type Result struct {
	URL     string `json:"url"`
	Status  int    `json:"status,omitempty"` // 0 when Err is set
	Err     string `json:"error,omitempty"`
	FoundOn string `json:"found_on"`
}

// Broken reports whether the URL failed: a transport error or an HTTP
// status of 400 or above.
func (r Result) Broken() bool { return r.Err != "" || r.Status >= 400 }

// Report summarises a crawl. Its JSON encoding is the wire format.
type Report struct {
	Checked     int      `json:"checked"`
	Interrupted bool     `json:"interrupted,omitempty"`
	Broken      []Result `json:"broken"` // sorted by URL; never nil after Check
}

// Options tunes a Check. The zero value is usable.
type Options struct {
	MaxDepth  int           // levels of same-host pages to crawl; 0 checks only the root URL
	Workers   int           // concurrent fetchers; 0 means 8
	Timeout   time.Duration // per request; 0 means 10s
	RPS       float64       // max requests per second per host; 0 means unlimited
	UserAgent string        // "" means "linkcheck/1.0"

	// OnProgress, if set, is called after every checked URL with the running
	// totals. Calls are sequential, from a single goroutine.
	OnProgress func(checked, broken int)
}

// Check crawls same-host HTML pages starting at rootURL and checks every link
// it finds, external ones included. It is safe to call concurrently.
//
// To bound a crawl, give ctx a deadline. When ctx ends early, Check returns
// the partial Report (Interrupted set) together with ctx's error.
func Check(ctx context.Context, rootURL string, opts Options) (Report, error) {
	root, err := url.Parse(rootURL)
	if err != nil || (root.Scheme != "http" && root.Scheme != "https") || root.Host == "" {
		return Report{}, fmt.Errorf("linkcheck: %q is not an absolute http(s) url", rootURL)
	}
	if opts.MaxDepth < 0 || opts.Workers < 0 || opts.Timeout < 0 || opts.RPS < 0 {
		return Report{}, errors.New("linkcheck: MaxDepth, Workers, Timeout and RPS must not be negative")
	}
	if opts.Workers == 0 {
		opts.Workers = defaultWorkers
	}
	if opts.Timeout == 0 {
		opts.Timeout = defaultTimeout
	}

	c := newCrawler(root, opts.MaxDepth, opts.Timeout)
	c.rps = opts.RPS
	c.onProgress = opts.OnProgress
	if opts.UserAgent != "" {
		c.userAgent = opts.UserAgent
	}
	checked, broken := c.run(ctx, opts.Workers)
	return Report{Checked: checked, Interrupted: ctx.Err() != nil, Broken: broken}, ctx.Err()
}

type job struct {
	u       *url.URL
	depth   int
	foundOn string
}

type crawler struct {
	client     *http.Client
	root       *url.URL
	host       string // same-host scope; follows the root's redirect, if any
	maxDepth   int
	onProgress func(checked, broken int) // may be nil
	rps        float64                   // per-host request rate; <= 0 means unlimited
	backoff    time.Duration             // first retry delay, doubles per attempt
	userAgent  string

	mu      sync.Mutex
	visited map[string]bool

	limMu    sync.Mutex
	limiters map[string]*rate.Limiter // one per host

	jobs    chan job
	results chan Result
	wg      sync.WaitGroup // counts outstanding jobs, not workers
}

func newCrawler(root *url.URL, maxDepth int, timeout time.Duration) *crawler {
	return &crawler{
		client:    &http.Client{Timeout: timeout},
		root:      root,
		host:      root.Host,
		maxDepth:  maxDepth,
		backoff:   retryBackoff,
		userAgent: defaultUserAgent,
		visited:   map[string]bool{},
		limiters:  map[string]*rate.Limiter{},
		jobs:      make(chan job, 64),
		results:   make(chan Result, 64),
	}
}

// run crawls until no jobs remain or ctx is cancelled. It returns how many
// urls were checked and the broken ones sorted by URL.
func (c *crawler) run(ctx context.Context, workers int) (int, []Result) {
	var checked int
	broken := []Result{} // non-nil so JSON says [] rather than null
	collectorDone := make(chan struct{})
	go func() {
		defer close(collectorDone)
		for r := range c.results {
			checked++
			if r.Broken() {
				broken = append(broken, r)
			}
			if c.onProgress != nil {
				c.onProgress(checked, len(broken))
			}
		}
	}()

	c.enqueue(ctx, job{u: c.root, depth: 0, foundOn: "(root)"})
	go func() {
		c.wg.Wait()
		close(c.jobs)
	}()
	var workersWG sync.WaitGroup
	for range workers {
		workersWG.Add(1)
		go func() {
			defer workersWG.Done()
			for j := range c.jobs {
				c.process(ctx, j)
				c.wg.Done()
			}
		}()
	}
	workersWG.Wait()
	close(c.results)
	<-collectorDone

	sort.Slice(broken, func(i, j int) bool { return broken[i].URL < broken[j].URL })
	return checked, broken
}

func (c *crawler) enqueue(ctx context.Context, j job) {
	if ctx.Err() != nil {
		return
	}
	if j.u.Path == "" {
		j.u.Path = "/" // so http://host and http://host/ dedupe to one url
	}
	key := j.u.String()
	c.mu.Lock()
	seen := c.visited[key]
	c.visited[key] = true
	c.mu.Unlock()
	if seen {
		return
	}
	c.wg.Add(1)
	// ponytail: goroutine per enqueue so workers never deadlock on a full
	// channel; a proper queue only matters for very large sites.
	go func() { c.jobs <- j }()
}

func (c *crawler) process(ctx context.Context, j job) {
	if ctx.Err() != nil {
		return // shutting down; skip so cancellations aren't counted as broken
	}
	resp, err := c.fetch(ctx, j.u)
	if err != nil {
		if ctx.Err() != nil {
			return // cancelled mid-flight, not a broken link
		}
		c.results <- Result{URL: j.u.String(), Err: err.Error(), FoundOn: j.foundOn}
		return
	}
	defer resp.Body.Close()

	// Redirects may have moved us: scope and relative links follow the final
	// URL, not the one we asked for.
	final := resp.Request.URL
	if j.depth == 0 {
		// Only the root job is running at this point, so this write is
		// ordered before every later read (e.g. example.com -> www.example.com).
		c.host = final.Host
	}

	r := Result{URL: j.u.String(), Status: resp.StatusCode, FoundOn: j.foundOn}
	// Recurse only into same-host HTML pages within the depth limit;
	// external links get checked but never crawled.
	crawlable := resp.StatusCode < 400 && j.depth < c.maxDepth &&
		final.Host == c.host &&
		strings.Contains(resp.Header.Get("Content-Type"), "text/html")
	if !crawlable {
		c.results <- r
		return
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodySize))
	if err != nil {
		r.Err = "read body: " + err.Error()
		c.results <- r
		return
	}
	c.results <- r
	for _, link := range extractLinks(final, string(body)) {
		c.enqueue(ctx, job{u: link, depth: j.depth + 1, foundOn: final.String()})
	}
}

// retryable reports whether a fetch outcome is worth another attempt:
// transport errors (except a host that doesn't exist), 429, or any 5xx.
func retryable(resp *http.Response, err error) bool {
	if err != nil {
		var dnsErr *net.DNSError
		return !(errors.As(err, &dnsErr) && dnsErr.IsNotFound)
	}
	return resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500
}

// retryDelay is how long to wait before the next attempt: the server's
// Retry-After if it sent one (capped at maxRetryAfter), never less than backoff.
func retryDelay(resp *http.Response, backoff time.Duration) time.Duration {
	if resp == nil {
		return backoff
	}
	var d time.Duration
	ra := resp.Header.Get("Retry-After")
	if secs, err := strconv.Atoi(ra); err == nil {
		d = time.Duration(secs) * time.Second
	} else if t, err := http.ParseTime(ra); err == nil {
		d = time.Until(t)
	}
	return min(max(d, backoff), maxRetryAfter)
}

// fetch GETs u after waiting for the host's rate limiter, retrying transient
// failures with doubling backoff. The last attempt's response or error is
// returned as-is.
func (c *crawler) fetch(ctx context.Context, u *url.URL) (*http.Response, error) {
	for attempt := 1; ; attempt++ {
		if err := c.limiter(u.Host).Wait(ctx); err != nil {
			return nil, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("User-Agent", c.userAgent)
		resp, err := c.client.Do(req)
		if !retryable(resp, err) || attempt == maxAttempts {
			return resp, err
		}
		delay := retryDelay(resp, c.backoff<<(attempt-1))
		if resp != nil {
			resp.Body.Close()
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}
}

// limiter returns the rate limiter for host, creating it on first use.
func (c *crawler) limiter(host string) *rate.Limiter {
	c.limMu.Lock()
	defer c.limMu.Unlock()
	l, ok := c.limiters[host]
	if !ok {
		limit := rate.Inf
		if c.rps > 0 {
			limit = rate.Limit(c.rps)
		}
		// ponytail: burst 1 keeps requests evenly spaced; bump if a host
		// tolerates small bursts and the crawl feels slow.
		l = rate.NewLimiter(limit, 1)
		c.limiters[host] = l
	}
	return l
}

// extractLinks returns absolute http(s) URLs referenced by href and src
// attributes, resolved against base (or the page's <base href>), with
// fragments stripped. The tokenizer decodes entities and skips comments and
// script bodies.
// ponytail: srcset and CSS url() are not followed; add if images in those
// matter.
func extractLinks(base *url.URL, body string) []*url.URL {
	var out []*url.URL
	z := html.NewTokenizer(strings.NewReader(body))
	for {
		switch z.Next() {
		case html.ErrorToken:
			return out // io.EOF, or malformed input: keep what we found
		case html.StartTagToken, html.SelfClosingTagToken:
			t := z.Token()
			for _, a := range t.Attr {
				if a.Key != "href" && a.Key != "src" {
					continue
				}
				ref, err := url.Parse(strings.TrimSpace(a.Val))
				if err != nil {
					continue
				}
				abs := base.ResolveReference(ref)
				if t.Data == "base" {
					base = abs // later links resolve against it; not a link itself
					continue
				}
				if (abs.Scheme != "http" && abs.Scheme != "https") || abs.Host == "" {
					continue
				}
				abs.Fragment = ""
				out = append(out, abs)
			}
		}
	}
}
