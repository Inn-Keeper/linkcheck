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

## As a service

`linkcheckd` exposes the checker over REST for public callers.

```sh
go build -o linkcheckd ./cmd/linkcheckd
./linkcheckd -addr :8080 -cors-origins https://app.example.com
```

```console
$ curl -s localhost:8080/v1/checks -H 'Content-Type: application/json' \
    -d '{"url": "https://example.com", "depth": 1}'
{"checked":107,"broken":[{"url":"https://example.com/old-post","status":404,"found_on":"https://example.com/blog"}]}
```

`depth` is optional (default `1`). The response is the same report as
`linkcheck -json`, plus two flags a client should surface:
`"interrupted": true` when the crawl hit the server deadline and
`"truncated": true` when it hit the URL cap. Both mean partial results.

| Status | Meaning |
| ------ | ------- |
| `200` | report (possibly partial, see above) |
| `400` | bad JSON, unknown field, bad URL, depth out of range: `{"error": "..."}` |
| `415` | `Content-Type` is not `application/json` |
| `429` | this IP is over its rate limit; see `Retry-After` |
| `503` | all crawl slots are busy; see `Retry-After` |

| Flag | Default | Meaning |
| ---- | ------- | ------- |
| `-addr` | `:8080` | listen address |
| `-timeout` | `25s` | hard deadline per crawl; keep it under your load balancer's |
| `-max-depth` | `2` | highest depth a caller may request |
| `-max-urls` | `200` | most URLs checked per crawl |
| `-max-crawls` | `8` | crawls running at once, server-wide |
| `-rate` | `6` | checks per minute per client IP (IPv6: per /64) |
| `-burst` | `3` | checks a client IP may make back to back |
| `-cors-origins` | none | comma-separated browser origins allowed to call the API; `*` for any |
| `-allow-private` | `false` | **unsafe**, local development only: allow private/loopback targets |

Things to know before deploying:

- Callers can only reach public addresses; the private-network guard is
  always on unless `-allow-private` is set.
- The client IP is the TCP peer. Forwarding headers are ignored because
  anyone can forge them. If you later put a proxy or load balancer in
  front, every caller will share its IP until `clientIP` is taught to
  trust that proxy's header.
- Rate-limit state is in memory, per process. Run one instance, or move it
  to a shared store first.
- There is no authentication and no TLS; terminate TLS in front or add it.
- `GET /healthz` answers `204`.

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
