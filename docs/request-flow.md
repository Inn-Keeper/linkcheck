# Request flow

What `linkcheckd` does with a `POST /v1/checks` request, from arrival to response.
Flag names refer to the `linkcheckd` flags; see the README for their defaults.

```mermaid
flowchart TD
    A["POST /v1/checks"] --> B{"CORS preflight?"}
    B -- yes --> B1["204 with CORS headers"]
    B -- no --> C{"Client IP within rate limit?"}
    C -- no --> C1["429 + Retry-After"]
    C -- yes --> D{"Content-Type is application/json?"}
    D -- no --> D1["415"]
    D -- yes --> E{"Body valid JSON, under 4 KB, depth in range?"}
    E -- no --> E1["400"]
    E -- yes --> F{"Free crawl slot?"}
    F -- no --> F1["503 + Retry-After: 5"]
    F -- yes --> G{"URL is absolute http(s)?"}
    G -- no --> G1["400"]
    G -- yes --> H["Start crawl with -timeout deadline"]

    H --> I["Queue root URL"]
    I --> J["Worker takes next URL"]
    J --> K["Wait for per-host limiter (10 rps)"]
    K --> L{"Address public, or -allow-private?"}
    L -- no --> L1["Record as broken: address is not public"]
    L -- yes --> M["GET the URL"]
    M --> N{"Transport error, 429 or 5xx, attempts left?"}
    N -- yes --> N1["Back off, honour Retry-After"] --> K
    N -- no --> O["Record status or error"]
    O --> P{"Under 400, HTML, same host, within depth?"}
    P -- yes --> Q["Extract links, queue unseen ones up to -max-urls"]
    P -- no --> R{"Queue empty or deadline hit?"}
    Q --> R
    L1 --> R
    R -- no --> J
    R -- yes --> S{"Client still connected?"}
    S -- no --> S1["Write nothing"]
    S -- yes --> T["200 with JSON report: checked, broken, truncated, interrupted"]
```

Notes:

- The rate limit counts every request, valid or not.
- External links are checked but never crawled.
- Hitting the deadline is not an error: the response is still 200, with a partial report and `interrupted: true`.
