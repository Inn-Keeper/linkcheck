# linkcheck

Concurrent broken-link checker. Point it at a URL; it crawls same-host HTML
pages and reports every link that fails — external links included.

Built as a Go study project: worker pools, channels, `context` cancellation,
table-driven tests. Two dependencies: `golang.org/x/time/rate` and
`golang.org/x/net/html`.

It is both a command and an importable package.

## Usage

```sh
go install github.com/Inn-Keeper/linkcheck/cmd/linkcheck@latest
# or, from a checkout:
go build -o linkcheck ./cmd/linkcheck
./linkcheck [flags] <url>
```

| Flag | Default | Meaning |
| ---- | ------- | ------- |
| `-depth` | `2` | how many levels of same-host pages to crawl |
| `-workers` | `8` | number of concurrent fetchers |
| `-timeout` | `10s` | per-request timeout |
| `-json` | `false` | emit results as JSON |
| `-rps` | `10` | max requests per second per host (`0` = unlimited) |

```console
$ ./linkcheck -depth 2 https://example.com
checked 107 urls, 2 broken
  https://example.com/old-post (on https://example.com/blog): HTTP 404
  https://gone.example.org/ (on https://example.com/links): dial tcp: no such host
```

Exit codes: `0` all good, `1` broken links found, `2` bad invocation,
`130` interrupted (partial results printed).

A live `checked N, M broken` line updates on stderr while crawling (only when
stderr is a terminal). Ctrl+C stops cleanly and prints partial results.

## As a library

```go
import "github.com/Inn-Keeper/linkcheck"

ctx, cancel := context.WithTimeout(ctx, 30*time.Second) // bounds the crawl
defer cancel()

rep, err := linkcheck.Check(ctx, "https://example.com", linkcheck.Options{
    MaxDepth:            2,
    RPS:                 10,
    DenyPrivateNetworks: true, // required when the URL comes from a user
})
```

- `Check` is safe to call concurrently and writes nothing to stdout/stderr.
- `err` is non-nil for bad input (not an absolute http(s) URL, negative
  options) and when `ctx` ends early. In the second case `rep` still holds
  the partial results, with `Interrupted` set.
- `Report` marshals to the same JSON the CLI prints with `-json`.
- `DenyPrivateNetworks` refuses loopback, private, link-local (cloud
  metadata) and reserved addresses. The check runs at connect time, so it
  also covers redirects and DNS names that resolve to internal addresses.
  Refused URLs show up as broken.

## How it works

- A fixed pool of worker goroutines pulls URLs from a jobs channel; a
  `sync.WaitGroup` counting outstanding jobs closes the channel when the
  crawl is exhausted.
- Every check result flows through a channel to a single collector goroutine
  that owns the counters — no shared state, no locks on the hot path.
- One `context` from `signal.NotifyContext` threads through every request;
  interrupting cancels in-flight fetches without reporting them as broken.
- Transient failures (transport errors, 429, 5xx) are retried up to twice
  with doubling backoff; the backoff wait aborts on Ctrl+C via `select`.
  `Retry-After` is honoured up to 30s, and a host that doesn't resolve is
  not retried.
- A `rate.Limiter` per host spaces requests out (`-rps`), so the tool is
  polite to real sites.
- Links (`href` and `src`) are extracted with an HTML tokenizer and resolved
  against the page's final URL after redirects, or its `<base href>`.
  Same-host HTML pages within the depth limit are crawled; everything else
  is only checked. If the root URL redirects to another host
  (`example.com` → `www.example.com`), that host becomes the crawl scope.

## Tests

```sh
go test -race ./...
```

Crawl behavior runs against `httptest` servers (broken links, redirects,
depth limit, external links, retries, cancellation, the private-network
guard); link extraction is table-driven and fuzzed.
