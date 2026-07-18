# linkcheck

Concurrent broken-link checker. Point it at a URL; it crawls same-host HTML
pages and reports every link that fails — external links included.

Stdlib only, no dependencies. Built as a Go study project: worker pools,
channels, `context` cancellation, table-driven tests.

## Usage

```sh
go build -o linkcheck .
./linkcheck [flags] <url>
```

| Flag | Default | Meaning |
| ---- | ------- | ------- |
| `-depth` | `2` | how many levels of same-host pages to crawl |
| `-workers` | `8` | number of concurrent fetchers |
| `-timeout` | `10s` | per-request timeout |
| `-json` | `false` | emit results as JSON |

```console
$ ./linkcheck -depth 2 https://example.com
checked 107 urls, 2 broken
  https://example.com/old-post (on https://example.com/blog): HTTP 404
  https://gone.example.org/ (on https://example.com/links): dial tcp: no such host
```

Exit codes: `0` all good, `1` broken links found, `2` bad invocation.

A live `checked N, M broken` line updates on stderr while crawling (only when
stderr is a terminal). Ctrl+C stops cleanly and prints partial results.

## How it works

- A fixed pool of worker goroutines pulls URLs from a jobs channel; a
  `sync.WaitGroup` counting outstanding jobs closes the channel when the
  crawl is exhausted.
- Every check result flows through a channel to a single collector goroutine
  that owns the counters — no shared state, no locks on the hot path.
- One `context` from `signal.NotifyContext` threads through every request;
  interrupting cancels in-flight fetches without reporting them as broken.
- Links are extracted with a regex and resolved against the page URL.
  Same-host HTML pages within the depth limit are crawled; everything else
  is only checked.

## Tests

```sh
go test -race ./...
```

Crawl behavior runs against `httptest` servers (broken links, clean site,
cancellation); link extraction is table-driven.
