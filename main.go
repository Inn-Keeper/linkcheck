package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// ponytail: regex link extraction; swap for golang.org/x/net/html if unquoted
// or malformed attributes start to matter.
var hrefRe = regexp.MustCompile(`(?i)href\s*=\s*["']([^"']+)["']`)

const (
	maxBodySize      = 2 << 20 // don't slurp unbounded pages into memory
	progressInterval = 200 * time.Millisecond
	maxAttempts      = 3                      // total tries per url on transient failures
	retryBackoff     = 500 * time.Millisecond // doubles per retry
)

type result struct {
	URL     string `json:"url"`
	Status  int    `json:"status,omitempty"` // 0 when Err is set
	Err     string `json:"error,omitempty"`
	FoundOn string `json:"found_on"`
}

func (r result) broken() bool { return r.Err != "" || r.Status >= 400 }

type report struct {
	Checked     int      `json:"checked"`
	Interrupted bool     `json:"interrupted,omitempty"`
	Broken      []result `json:"broken"`
}

func (rep report) write(w io.Writer, asJSON bool) error {
	if asJSON {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(rep)
	}
	if rep.Interrupted {
		fmt.Fprintln(w, "interrupted; partial results:")
	}
	fmt.Fprintf(w, "checked %d urls, %d broken\n", rep.Checked, len(rep.Broken))
	for _, r := range rep.Broken {
		if r.Err != "" {
			fmt.Fprintf(w, "  %s (on %s): %s\n", r.URL, r.FoundOn, r.Err)
		} else {
			fmt.Fprintf(w, "  %s (on %s): HTTP %d\n", r.URL, r.FoundOn, r.Status)
		}
	}
	return nil
}

type job struct {
	u       *url.URL
	depth   int
	foundOn string
}

type crawler struct {
	client   *http.Client
	root     *url.URL
	host     string // same-host scope; follows the root's redirect, if any
	maxDepth int
	progress io.Writer     // nil disables the live progress line
	rps      float64       // per-host request rate; <= 0 means unlimited
	backoff  time.Duration // first retry delay, doubles per attempt

	mu      sync.Mutex
	visited map[string]bool

	limMu    sync.Mutex
	limiters map[string]*rate.Limiter // one per host

	jobs    chan job
	results chan result
	wg      sync.WaitGroup // counts outstanding jobs, not workers
}

func newCrawler(root *url.URL, maxDepth int, timeout time.Duration) *crawler {
	return &crawler{
		client:   &http.Client{Timeout: timeout},
		root:     root,
		host:     root.Host,
		maxDepth: maxDepth,
		backoff:  retryBackoff,
		visited:  map[string]bool{},
		limiters: map[string]*rate.Limiter{},
		jobs:     make(chan job, 64),
		results:  make(chan result, 64),
	}
}

// run crawls until no jobs remain or ctx is cancelled. It returns how many
// urls were checked and the broken ones sorted by URL.
func (c *crawler) run(ctx context.Context, workers int) (int, []result) {
	var checked int
	var broken []result
	collectorDone := make(chan struct{})
	go func() {
		defer close(collectorDone)
		tick := time.NewTicker(progressInterval)
		defer tick.Stop()
		for {
			select {
			case r, ok := <-c.results:
				if !ok {
					return
				}
				checked++
				if r.broken() {
					broken = append(broken, r)
				}
			case <-tick.C:
				if c.progress != nil {
					fmt.Fprintf(c.progress, "\rchecked %d, %d broken", checked, len(broken))
				}
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
	if c.progress != nil {
		fmt.Fprint(c.progress, "\r\x1b[2K") // erase the progress line
	}

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
		c.results <- result{URL: j.u.String(), Err: err.Error(), FoundOn: j.foundOn}
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

	r := result{URL: j.u.String(), Status: resp.StatusCode, FoundOn: j.foundOn}
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
// transport errors, 429, or any 5xx.
func retryable(resp *http.Response, err error) bool {
	return err != nil || resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500
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
		resp, err := c.client.Do(req)
		if !retryable(resp, err) || attempt == maxAttempts {
			return resp, err
		}
		if resp != nil {
			resp.Body.Close()
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(c.backoff << (attempt - 1)):
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

// extractLinks returns absolute http(s) URLs referenced by href attributes,
// resolved against base, with fragments stripped.
func extractLinks(base *url.URL, body string) []*url.URL {
	var out []*url.URL
	for _, m := range hrefRe.FindAllStringSubmatch(body, -1) {
		ref, err := url.Parse(strings.TrimSpace(m[1]))
		if err != nil {
			continue
		}
		abs := base.ResolveReference(ref)
		if (abs.Scheme != "http" && abs.Scheme != "https") || abs.Host == "" {
			continue
		}
		abs.Fragment = ""
		out = append(out, abs)
	}
	return out
}

func main() {
	depth := flag.Int("depth", 2, "how many levels of same-host pages to crawl")
	workers := flag.Int("workers", 8, "number of concurrent fetchers")
	timeout := flag.Duration("timeout", 10*time.Second, "per-request timeout")
	jsonOut := flag.Bool("json", false, "emit results as JSON")
	rps := flag.Float64("rps", 10, "max requests per second per host (0 = unlimited)")
	flag.Parse()

	if flag.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "usage: linkcheck [flags] <url>")
		flag.PrintDefaults()
		os.Exit(2)
	}
	root, err := url.Parse(flag.Arg(0))
	if err != nil || root.Host == "" {
		fmt.Fprintf(os.Stderr, "linkcheck: invalid url %q\n", flag.Arg(0))
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	c := newCrawler(root, *depth, *timeout)
	c.rps = *rps
	if st, err := os.Stderr.Stat(); err == nil && st.Mode()&os.ModeCharDevice != 0 {
		c.progress = os.Stderr
	}
	checked, broken := c.run(ctx, *workers)

	rep := report{Checked: checked, Interrupted: ctx.Err() != nil, Broken: broken}
	if err := rep.write(os.Stdout, *jsonOut); err != nil {
		fmt.Fprintln(os.Stderr, "linkcheck:", err)
		os.Exit(2)
	}
	if len(broken) > 0 {
		os.Exit(1)
	}
}
