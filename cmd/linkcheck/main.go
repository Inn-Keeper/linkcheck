// Command linkcheck crawls a site and prints its broken links.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"time"

	"github.com/Inn-Keeper/linkcheck"
)

const progressInterval = 200 * time.Millisecond

func write(w io.Writer, rep linkcheck.Report, asJSON bool) error {
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
	// The library treats 0 as "use the default"; on the command line it is a mistake.
	if *workers < 1 || *timeout <= 0 {
		fmt.Fprintln(os.Stderr, "linkcheck: -workers must be at least 1 and -timeout positive")
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	opts := linkcheck.Options{MaxDepth: *depth, Workers: *workers, Timeout: *timeout, RPS: *rps}
	st, err := os.Stderr.Stat()
	tty := err == nil && st.Mode()&os.ModeCharDevice != 0
	if tty {
		var last time.Time
		opts.OnProgress = func(checked, broken int) {
			if time.Since(last) < progressInterval {
				return
			}
			last = time.Now()
			fmt.Fprintf(os.Stderr, "\rchecked %d, %d broken", checked, broken)
		}
	}

	rep, err := linkcheck.Check(ctx, flag.Arg(0), opts)
	if tty {
		fmt.Fprint(os.Stderr, "\r\x1b[2K") // erase the progress line
	}
	if err != nil && ctx.Err() == nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if err := write(os.Stdout, rep, *jsonOut); err != nil {
		fmt.Fprintln(os.Stderr, "linkcheck:", err)
		os.Exit(2)
	}
	if len(rep.Broken) > 0 {
		os.Exit(1)
	}
}
