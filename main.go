package main

import (
	"context"
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
)

// ponytail: regex link extraction; swap for golang.org/x/net/html if unquoted
// or malformed attributes start to matter.
var hrefRe = regexp.MustCompile(`(?i)href\s*=\s*["']([^"']+)["']`)

const (
	maxBodySize      = 2 << 20 // don't slurp unbounded pages into memory
	progressInterval = 200 * time.Millisecond
)

type result struct {
	URL     string
	Status  int // 0 when Err is set
	Err     string
	FoundOn string
}

func (r result) broken() bool { return r.Err != "" || r.Status >= 400 }

type job struct {
	u       *url.URL
	depth   int
	foundOn string
}

type crawler struct {
	client   *http.Client
	root     *url.URL
	maxDepth int
	timeout  time.Duration
	progress io.Writer // nil disables the live progress line

	mu      sync.Mutex
	visited map[string]bool

	jobs    chan job
	results chan result
	wg      sync.WaitGroup // counts outstanding jobs, not workers
}

func newCrawler(root *url.URL, maxDepth int, timeout time.Duration) *crawler {
	return &crawler{
		client:   &http.Client{},
		root:     root,
		maxDepth: maxDepth,
		timeout:  timeout,
		visited:  map[string]bool{},
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
	reqCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, j.u.String(), nil)
	if err != nil {
		c.results <- result{URL: j.u.String(), Err: err.Error(), FoundOn: j.foundOn}
		return
	}
	resp, err := c.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return // cancelled mid-flight, not a broken link
		}
		c.results <- result{URL: j.u.String(), Err: err.Error(), FoundOn: j.foundOn}
		return
	}
	defer resp.Body.Close()

	r := result{URL: j.u.String(), Status: resp.StatusCode, FoundOn: j.foundOn}
	// Recurse only into same-host HTML pages within the depth limit;
	// external links get checked but never crawled.
	crawlable := resp.StatusCode < 400 && j.depth < c.maxDepth &&
		j.u.Host == c.root.Host &&
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
	for _, link := range extractLinks(j.u, string(body)) {
		c.enqueue(ctx, job{u: link, depth: j.depth + 1, foundOn: j.u.String()})
	}
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
		if abs.Scheme != "http" && abs.Scheme != "https" {
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
	if st, err := os.Stderr.Stat(); err == nil && st.Mode()&os.ModeCharDevice != 0 {
		c.progress = os.Stderr
	}
	checked, broken := c.run(ctx, *workers)

	if ctx.Err() != nil {
		fmt.Println("interrupted; partial results:")
	}
	fmt.Printf("checked %d urls, %d broken\n", checked, len(broken))
	for _, r := range broken {
		if r.Err != "" {
			fmt.Printf("  %s (on %s): %s\n", r.URL, r.FoundOn, r.Err)
		} else {
			fmt.Printf("  %s (on %s): HTTP %d\n", r.URL, r.FoundOn, r.Status)
		}
	}
	if len(broken) > 0 {
		os.Exit(1)
	}
}
