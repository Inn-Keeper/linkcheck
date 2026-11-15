# Use cases

linkcheck comes in three forms that share one engine:

| Form | Where | Use it when |
| ---- | ----- | ----------- |
| Command line | `cmd/linkcheck` | you check a site by hand or in CI |
| Go package | `github.com/Inn-Keeper/linkcheck` | a Go program needs link checking built in |
| REST service | `cmd/linkcheckd` | frontends or non-Go backends call it over HTTP |

All three do the same thing: start at one URL, crawl HTML pages on the same
host down to a depth limit, and check every link found on those pages,
external ones included. External pages are checked but never crawled.

Contents:

- [What counts as broken](#what-counts-as-broken)
- [Command line](#command-line)
- [Go package](#go-package)
- [REST service](#rest-service)
- [Deploying the service on Render](#deploying-the-service-on-render)
- [What it does not do](#what-it-does-not-do)

## What counts as broken

A URL is reported as broken when the request fails (DNS, connection,
timeout) or the final HTTP status is 400 or above. Redirects are followed,
so a link that redirects to a working page is fine.

Depth decides how far the crawl goes:

| Depth | What is checked |
| ----- | --------------- |
| `0` | only the URL you gave |
| `1` | that page and every link on it |
| `2` | the same, plus every link on the same-host pages it links to |

Links are read from `href` and `src` attributes, so images, scripts and
stylesheets are checked too.

## Command line

Build it once from a checkout, or install it:

```sh
go build -o linkcheck ./cmd/linkcheck
# or
go install github.com/Inn-Keeper/linkcheck/cmd/linkcheck@latest
```

Quote the URL whenever it contains `?` or `&`, or the shell will mangle it.

### Check a whole site

```console
$ ./linkcheck https://example.com
checked 107 urls, 2 broken
  https://example.com/old-post (on https://example.com/blog): HTTP 404
  https://gone.example.org/ (on https://example.com/links): lookup gone.example.org: no such host
```

The default depth is 2. Raise it with `-depth` for a deeper crawl.

### Check the links on one page

```sh
./linkcheck -depth 1 'https://example.com/blog/my-post?ref=home'
```

### Check whether one URL is alive

```sh
./linkcheck -depth 0 https://example.com/download.zip
```

### Check a site running on your machine

The command line tool has no restriction on private addresses, so it works
against a local dev server:

```sh
./linkcheck http://localhost:3000
```

### Fail a CI build on broken links

The exit code carries the result:

| Exit code | Meaning |
| --------- | ------- |
| `0` | no broken links |
| `1` | broken links found |
| `2` | bad invocation (bad URL or flag) |
| `130` | interrupted; the results printed are partial |

```yaml
# .github/workflows/links.yml
- uses: actions/setup-go@v5
  with:
    go-version: stable
- run: go run github.com/Inn-Keeper/linkcheck/cmd/linkcheck@latest https://example.com
```

### Feed the results to another program

`-json` prints a report instead of text:

```console
$ ./linkcheck -json https://example.com
{
  "checked": 107,
  "broken": [
    {
      "url": "https://example.com/old-post",
      "status": 404,
      "found_on": "https://example.com/blog"
    },
    {
      "url": "https://gone.example.org/",
      "error": "lookup gone.example.org: no such host",
      "found_on": "https://example.com/links"
    }
  ]
}
```

Each broken entry has either `status` or `error`, never both. `broken` is
always an array, empty on a clean run.

```sh
./linkcheck -json https://example.com | jq -r '.broken[] | "\(.url)\t\(.status // .error)"'
```

### Slow down or speed up

| Flag | Default | Effect |
| ---- | ------- | ------ |
| `-rps` | `10` | requests per second per host; `0` removes the limit |
| `-workers` | `8` | concurrent requests across all hosts |
| `-timeout` | `10s` | per request |

Lower `-rps` for a small site you do not want to load. Requests that fail
with a transport error, 429 or 5xx are retried twice with backoff, and
`Retry-After` is respected up to 30 seconds.

## Go package

```go
import "github.com/Inn-Keeper/linkcheck"

ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

rep, err := linkcheck.Check(ctx, "https://example.com", linkcheck.Options{
    MaxDepth: 2,
    RPS:      10,
})
switch {
case errors.Is(err, context.DeadlineExceeded):
    // rep holds what was checked before the deadline; rep.Interrupted is true.
case err != nil:
    return err // bad input: not an absolute http(s) URL, or a negative option
}
for _, b := range rep.Broken {
    fmt.Println(b.URL, b.Status, b.Err, b.FoundOn)
}
```

### Options

| Field | Zero value means | Purpose |
| ----- | ---------------- | ------- |
| `MaxDepth` | check only the root URL | crawl depth |
| `Workers` | 8 | concurrent requests |
| `Timeout` | 10s | per request |
| `RPS` | unlimited | requests per second per host |
| `UserAgent` | `linkcheck/1.0` | sent with every request |
| `MaxURLs` | unlimited | stop queueing new URLs after this many |
| `DenyPrivateNetworks` | off | refuse non-public addresses |
| `OnProgress` | none | callback with running totals |

### Bound a crawl

There are two limits, and they report differently:

- **Time:** give `ctx` a deadline. `Check` returns the partial report with
  `Interrupted` set, together with the context error.
- **Size:** set `MaxURLs`. `Check` returns normally with `Truncated` set
  when links were found but not checked.

### Check URLs that come from users

Set `DenyPrivateNetworks: true`. Without it, a caller can make your server
fetch `localhost`, internal hosts or the cloud metadata address. With it,
those are refused at connect time, which also covers redirects and DNS
names that resolve to internal addresses. A refused URL appears in the
report as broken with the error `linkcheck: address is not public`.

### Show progress

```go
linkcheck.Options{
    OnProgress: func(checked, broken int) {
        log.Printf("%d checked, %d broken", checked, broken)
    },
}
```

The callback runs after every checked URL, from a single goroutine.

`Check` is safe to call from many goroutines at once and never writes to
stdout or stderr.

## REST service

### Run it locally

```sh
go run ./cmd/linkcheckd -addr :8080
```

Then, from another terminal:

```console
$ curl -s localhost:8080/v1/checks \
    -H 'Content-Type: application/json' \
    -d '{"url": "https://example.com", "depth": 1}'
{"checked":12,"broken":[]}
```

To check a site that is itself running on your machine, start the service
with `-allow-private`. Never use that flag on a public deployment.

### Request

`POST /v1/checks` with a JSON body:

| Field | Required | Meaning |
| ----- | -------- | ------- |
| `url` | yes | absolute `http` or `https` URL |
| `depth` | no | `0` to the server's `-max-depth`; default `1` |

The `Content-Type` header must be `application/json`. Unknown fields are
rejected.

### Response

A `200` carries the same report as `linkcheck -json`, with two extra flags
that both mean the results are partial:

| Field | Set when |
| ----- | -------- |
| `"interrupted": true` | the crawl hit the server deadline (`-timeout`, 25s) |
| `"truncated": true` | the crawl hit the URL cap (`-max-urls`, 200) |

| Status | Meaning | What the client should do |
| ------ | ------- | ------------------------- |
| `200` | report | show it; flag partial results |
| `400` | bad JSON, unknown field, bad URL, depth out of range | show the `error` text |
| `415` | wrong `Content-Type` | fix the request |
| `429` | this client is over its rate limit | wait `Retry-After` seconds |
| `503` | all crawl slots are busy | wait `Retry-After` seconds |

Errors have the shape `{"error": "..."}`.

### Call it from a browser

Start the service with the page's origin allowed:

```sh
./linkcheckd -cors-origins https://app.example.com,https://staging.example.com
```

```js
async function checkLinks(url) {
  const res = await fetch("https://links.example.com/v1/checks", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ url, depth: 1 }),
  });
  if (res.status === 429 || res.status === 503) {
    const seconds = Number(res.headers.get("Retry-After")) || 5;
    throw new Error(`Busy, try again in ${seconds}s`);
  }
  const body = await res.json();
  if (!res.ok) throw new Error(body.error);
  return body; // { checked, broken, interrupted?, truncated? }
}
```

With `-cors-origins` unset, no browser page can call the API. `*` allows
any origin.

### Call it from another backend

Any HTTP client works; CORS does not apply to server-to-server calls. Allow
at least the server deadline plus a few seconds as the client timeout.
Note that all calls from one backend come from one IP address and share
one rate-limit bucket, so raise `-rate` and `-burst` to fit.

### Protect the service from abuse

These are on by default and set by flags:

| Protection | Flag | Default |
| ---------- | ---- | ------- |
| Private and loopback targets refused | `-allow-private` turns it off | on |
| Checks per minute per client IP | `-rate` | `6` |
| Back-to-back checks per client IP | `-burst` | `3` |
| Crawl deadline | `-timeout` | `25s` |
| Highest depth a caller may ask for | `-max-depth` | `2` |
| URLs per crawl | `-max-urls` | `200` |
| Crawls at once, server-wide | `-max-crawls` | `8` |

IPv6 clients are limited per /64 block. Every request counts against the
limit, including rejected ones.

### Monitor it

`GET /healthz` answers `204`. Each completed check writes one log line with
the URL, client IP, totals and duration.

## Deploying the service on Render

1. Push the branch you want deployed to GitHub.
2. In Render, create a **Web Service** from the repo, language **Go**.
3. Build command:
   ```sh
   go build -tags netgo -ldflags '-s -w' -o linkcheckd ./cmd/linkcheckd
   ```
4. Start command:
   ```sh
   ./linkcheckd -addr :$PORT -cors-origins https://your-frontend.example -client-ip-header CF-Connecting-IP
   ```
5. Health check path: `/healthz`.
6. Keep it at one instance. Rate-limit state lives in memory, so each extra
   instance multiplies the limit.

Why `-client-ip-header`: Render routes requests through its own proxy, so
without the flag every caller looks like the same address and shares one
rate-limit bucket. Render's responses show Cloudflare in front, which sets
`CF-Connecting-IP`. Confirm it on your service before relying on it: send
two requests with a forged `CF-Connecting-IP` header from `curl` and check
that the log line still shows your real address.

Render terminates HTTPS, so the service needs no TLS of its own. Its native
Go runtime always uses the latest stable Go; deploy with a Dockerfile if
you need a fixed version.

## What it does not do

- **Links added by JavaScript.** Only the HTML the server returns is read,
  so single-page apps show few links.
- **`srcset` and CSS `url()`.** Images referenced only there are not checked.
- **Pages behind a login.** No cookies or credentials are sent.
- **Soft 404s.** A "not found" page served with status 200 counts as fine.
- **robots.txt.** It is not consulted; use `-rps` to stay polite.
- **Sites that block automated clients.** Some answer 403 or 429 to
  anything that is not a browser, and those links show up as broken.
- **Pages over 2 MB.** Links beyond the first 2 MB of a page are not seen.
