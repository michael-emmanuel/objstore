// Command loadgen drives a workload against a gateway and reports
// throughput and latency percentiles. It never fabricates numbers: every
// figure it prints is measured during the run, and it states the conditions
// (object size, concurrency, key space, hot ratio) alongside the results.
//
// Server-side resource figures (disk bytes written, replication traffic,
// cache hits, repair bytes) are taken by scraping the components' /metrics
// endpoints before and after the run and printing counter deltas.
//
// Examples:
//
//	loadgen -workload write-only -size 4KiB -keys 10000 -concurrency 32 -duration 30s
//	loadgen -workload read-heavy -size 64KiB -keys 1000 -hot-ratio 0.9
//	loadgen -workload mixed -size 8MiB -concurrency 8 -scrape http://localhost:8080/metrics
package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"os"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/michael-emmanuel/objstore/pkg/client"
)

func main() {
	var (
		gateway     = flag.String("gateway", "http://localhost:8080", "gateway URL")
		tokenEnv    = flag.String("token-env", "OBJSTORE_TOKEN", "environment variable holding the API token")
		bucket      = flag.String("bucket", "loadgen", "bucket (created if missing)")
		workload    = flag.String("workload", "mixed", "write-only | read-only | read-heavy (95/5) | write-heavy (20/80) | mixed (50/50)")
		sizeFlag    = flag.String("size", "64KiB", "object size, e.g. 1KiB, 1MiB, 100MiB; or a distribution small|medium|large")
		keys        = flag.Int("keys", 1000, "key space size (number of unique objects)")
		hotRatio    = flag.Float64("hot-ratio", 0, "fraction of reads aimed at a single hot key (0..1)")
		concurrency = flag.Int("concurrency", 16, "concurrent workers")
		duration    = flag.Duration("duration", 30*time.Second, "run time (after preload)")
		preload     = flag.Bool("preload", true, "write every key once before measuring reads")
		verify      = flag.Bool("verify", false, "verify SHA-256 of every read (costs client CPU)")
		jsonOut     = flag.Bool("json", false, "print the report as JSON")
		scrape      = flag.String("scrape", "", "comma-separated /metrics URLs to diff before/after the run")
		scrapeMatch = flag.String("scrape-match", `_(bytes|total)\b`, "regexp selecting scraped counters to report")
	)
	flag.Parse()

	token := os.Getenv(*tokenEnv)
	if token == "" {
		fmt.Fprintf(os.Stderr, "set %s to an API token\n", *tokenEnv)
		os.Exit(2)
	}
	sizer, err := parseSize(*sizeFlag)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	readFrac, err := readFraction(*workload)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	c := client.New(*gateway, token)
	ctx := context.Background()
	if err := c.CreateBucket(ctx, *bucket, client.BucketOptions{}); err != nil && !client.IsStatus(err, http.StatusConflict) {
		fmt.Fprintln(os.Stderr, "create bucket:", err)
		os.Exit(1)
	}

	r := &runner{c: c, bucket: *bucket, sizer: sizer, keys: *keys, hot: *hotRatio, verify: *verify}
	if readFrac > 0 && *preload {
		start := time.Now()
		r.preload(ctx, *concurrency)
		fmt.Fprintf(os.Stderr, "preloaded %d keys in %s\n", *keys, time.Since(start).Round(time.Millisecond))
	}

	var re *regexp.Regexp
	if *scrape != "" {
		re = regexp.MustCompile(*scrapeMatch)
	}
	before := scrapeAll(*scrape, re)
	var ms0 runtime.MemStats
	runtime.ReadMemStats(&ms0)

	rep := r.run(ctx, *concurrency, *duration, readFrac)

	var ms1 runtime.MemStats
	runtime.ReadMemStats(&ms1)
	after := scrapeAll(*scrape, re)

	rep.Conditions = map[string]string{
		"workload": *workload, "size": *sizeFlag, "keys": strconv.Itoa(*keys), "hot_ratio": fmt.Sprint(*hotRatio),
		"concurrency": strconv.Itoa(*concurrency), "duration": duration.String(), "gateway": *gateway,
		"verify": fmt.Sprint(*verify), "client_gomaxprocs": strconv.Itoa(runtime.GOMAXPROCS(0)),
	}
	rep.ClientHeapMB = float64(ms1.HeapAlloc) / (1 << 20)
	rep.ClientGCCycles = ms1.NumGC - ms0.NumGC
	rep.ServerDeltas = diff(before, after)

	if *jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		enc.Encode(rep)
		return
	}
	rep.print(os.Stdout)
}

// ---------------------------------------------------------------------------
// Workload

type sizer func(rng *rand.Rand) int64

func parseSize(s string) (sizer, error) {
	switch s {
	case "small": // 1-64 KiB
		return func(r *rand.Rand) int64 { return 1<<10 + r.Int64N(63<<10) }, nil
	case "medium": // 256 KiB - 8 MiB
		return func(r *rand.Rand) int64 { return 256<<10 + r.Int64N(8<<20-256<<10) }, nil
	case "large": // 32-128 MiB
		return func(r *rand.Rand) int64 { return 32<<20 + r.Int64N(96<<20) }, nil
	}
	n, err := parseBytes(s)
	if err != nil {
		return nil, err
	}
	return func(*rand.Rand) int64 { return n }, nil
}

func parseBytes(s string) (int64, error) {
	units := []struct {
		suf string
		mul int64
	}{{"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10}, {"GB", 1e9}, {"MB", 1e6}, {"KB", 1e3}, {"B", 1}}
	for _, u := range units {
		if v, ok := strings.CutSuffix(s, u.suf); ok {
			f, err := strconv.ParseFloat(v, 64)
			if err != nil || f < 0 {
				return 0, fmt.Errorf("bad size %q", s)
			}
			return int64(f * float64(u.mul)), nil
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("bad size %q", s)
	}
	return n, nil
}

func readFraction(w string) (float64, error) {
	switch w {
	case "write-only":
		return 0, nil
	case "read-only":
		return 1, nil
	case "read-heavy":
		return 0.95, nil
	case "write-heavy":
		return 0.2, nil
	case "mixed":
		return 0.5, nil
	}
	return 0, fmt.Errorf("unknown workload %q", w)
}

type runner struct {
	c      *client.Client
	bucket string
	sizer  sizer
	keys   int
	hot    float64
	verify bool
}

func key(i int) string { return fmt.Sprintf("obj/%03d/%08d", i%997, i) }

func (r *runner) put(ctx context.Context, rng *rand.Rand, i int) (int64, error) {
	size := r.sizer(rng)
	src := newDataReader(rng.Uint64(), size)
	_, err := r.c.Put(ctx, r.bucket, key(i), src, size, client.PutOptions{ContentType: "application/octet-stream"})
	return size, err
}

func (r *runner) get(ctx context.Context, i int) (int64, string, error) {
	body, info, err := r.c.Get(ctx, r.bucket, key(i), client.GetOptions{})
	if err != nil {
		return 0, "", err
	}
	defer body.Close()
	var n int64
	if r.verify {
		// For single-part objects the ETag is the SHA-256 of the content, so
		// the check is self-contained and stays correct under concurrent
		// overwrites (each response carries the ETag of the version served).
		h := sha256.New()
		n, err = io.Copy(h, body)
		if got := hex.EncodeToString(h.Sum(nil)); err == nil && !strings.Contains(info.ETag, "-") && got != info.ETag {
			err = fmt.Errorf("checksum mismatch: body %s, etag %s", got, info.ETag)
		}
	} else {
		n, err = io.Copy(io.Discard, body)
	}
	return n, info.Cache, err
}

func (r *runner) preload(ctx context.Context, conc int) {
	var next atomic.Int64
	var wg sync.WaitGroup
	var failed atomic.Int64
	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(seed, 99))
			for {
				i := int(next.Add(1) - 1)
				if i >= r.keys {
					return
				}
				if _, err := r.put(ctx, rng, i); err != nil {
					failed.Add(1)
				}
			}
		}(uint64(w))
	}
	wg.Wait()
	if f := failed.Load(); f > 0 {
		fmt.Fprintf(os.Stderr, "warning: %d preload writes failed\n", f)
	}
}

func (r *runner) run(ctx context.Context, conc int, d time.Duration, readFrac float64) *report {
	rep := &report{Ops: map[string]*opStats{"PUT": newOpStats(), "GET": newOpStats()}}
	ctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(seed, 7))
			for ctx.Err() == nil {
				i := rng.IntN(r.keys)
				if rng.Float64() < readFrac {
					if r.hot > 0 && rng.Float64() < r.hot {
						i = 0
					}
					t := time.Now()
					n, cache, err := r.get(ctx, i)
					if ctx.Err() != nil && err != nil {
						return // interrupted by end of run, not a real failure
					}
					rep.Ops["GET"].record(time.Since(t), n, err, cache)
				} else {
					t := time.Now()
					n, err := r.put(ctx, rng, i)
					if ctx.Err() != nil && err != nil {
						return
					}
					rep.Ops["PUT"].record(time.Since(t), n, err, "")
				}
			}
		}(uint64(w) + 1000)
	}
	wg.Wait()
	rep.Elapsed = time.Since(start)
	return rep
}

// dataReader produces deterministic pseudo-random bytes without holding the
// object in memory.
type dataReader struct {
	rng  *rand.Rand
	left int64
}

func newDataReader(seed uint64, n int64) *dataReader {
	return &dataReader{rng: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)), left: n}
}

func (d *dataReader) Read(p []byte) (int, error) {
	if d.left == 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > d.left {
		p = p[:d.left]
	}
	i := 0
	for ; i+8 <= len(p); i += 8 {
		v := d.rng.Uint64()
		for b := 0; b < 8; b++ {
			p[i+b] = byte(v >> (8 * b))
		}
	}
	for ; i < len(p); i++ {
		p[i] = byte(d.rng.Uint32())
	}
	d.left -= int64(len(p))
	return len(p), nil
}

// ---------------------------------------------------------------------------
// Statistics

// Latencies go into log-spaced buckets (2% relative width) from 10µs to
// ~10 min, so memory is constant regardless of run length and percentile
// error is bounded by the bucket width.
const (
	histMin    = 10e-6
	histGrowth = 1.02
	histN      = 1000
)

type opStats struct {
	mu       sync.Mutex
	buckets  [histN]uint64
	count    uint64
	errors   uint64
	bytes    int64
	max      time.Duration
	cache    map[string]uint64
	errKinds map[string]uint64
}

func newOpStats() *opStats {
	return &opStats{cache: map[string]uint64{}, errKinds: map[string]uint64{}}
}

func bucketOf(d time.Duration) int {
	s := d.Seconds()
	if s <= histMin {
		return 0
	}
	i := int(math.Log(s/histMin) / math.Log(histGrowth))
	if i >= histN {
		i = histN - 1
	}
	return i
}

func (o *opStats) record(d time.Duration, n int64, err error, cache string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err != nil {
		o.errors++
		msg := err.Error()
		var ae *client.APIError
		if errors.As(err, &ae) {
			msg = fmt.Sprintf("%d %s", ae.Status, ae.Code)
		} else if len(msg) > 60 {
			msg = msg[:60]
		}
		o.errKinds[msg]++
		return
	}
	o.count++
	o.bytes += n
	o.buckets[bucketOf(d)]++
	if d > o.max {
		o.max = d
	}
	if cache != "" {
		o.cache[cache]++
	}
}

func (o *opStats) quantile(q float64) time.Duration {
	if o.count == 0 {
		return 0
	}
	target := uint64(math.Ceil(q * float64(o.count)))
	var seen uint64
	for i, c := range o.buckets {
		seen += c
		if seen >= target {
			// Report the bucket's upper edge, but never more than the
			// largest latency actually observed.
			upper := time.Duration(histMin * math.Pow(histGrowth, float64(i+1)) * float64(time.Second))
			return min(upper, o.max)
		}
	}
	return o.max
}

type opSummary struct {
	Count      uint64            `json:"count"`
	Errors     uint64            `json:"errors"`
	ErrorKinds map[string]uint64 `json:"error_kinds,omitempty"`
	OpsPerSec  float64           `json:"ops_per_sec"`
	MBPerSec   float64           `json:"mib_per_sec"`
	P50ms      float64           `json:"p50_ms"`
	P95ms      float64           `json:"p95_ms"`
	P99ms      float64           `json:"p99_ms"`
	Maxms      float64           `json:"max_ms"`
	Cache      map[string]uint64 `json:"cache_sources,omitempty"`
}

type report struct {
	Conditions     map[string]string    `json:"conditions"`
	Elapsed        time.Duration        `json:"elapsed_ns"`
	Ops            map[string]*opStats  `json:"-"`
	Summary        map[string]opSummary `json:"ops"`
	ClientHeapMB   float64              `json:"client_heap_mib"`
	ClientGCCycles uint32               `json:"client_gc_cycles"`
	ServerDeltas   map[string]float64   `json:"server_counter_deltas,omitempty"`
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func (r *report) summarize() {
	r.Summary = map[string]opSummary{}
	secs := r.Elapsed.Seconds()
	for name, o := range r.Ops {
		if o.count == 0 && o.errors == 0 {
			continue
		}
		r.Summary[name] = opSummary{
			Count: o.count, Errors: o.errors, ErrorKinds: o.errKinds,
			OpsPerSec: float64(o.count) / secs, MBPerSec: float64(o.bytes) / secs / (1 << 20),
			P50ms: ms(o.quantile(0.50)), P95ms: ms(o.quantile(0.95)), P99ms: ms(o.quantile(0.99)), Maxms: ms(o.max),
			Cache: o.cache,
		}
	}
}

func (r *report) MarshalJSON() ([]byte, error) {
	r.summarize()
	type alias report
	return json.Marshal((*alias)(r))
}

func (r *report) print(w io.Writer) {
	r.summarize()
	fmt.Fprintln(w, "conditions:")
	ks := make([]string, 0, len(r.Conditions))
	for k := range r.Conditions {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	for _, k := range ks {
		fmt.Fprintf(w, "  %-18s %s\n", k, r.Conditions[k])
	}
	fmt.Fprintf(w, "elapsed: %s\n\n", r.Elapsed.Round(time.Millisecond))
	fmt.Fprintf(w, "%-4s %9s %7s %10s %9s %9s %9s %9s %9s\n", "op", "count", "errors", "ops/s", "MiB/s", "p50 ms", "p95 ms", "p99 ms", "max ms")
	for _, name := range []string{"PUT", "GET"} {
		s, ok := r.Summary[name]
		if !ok {
			continue
		}
		fmt.Fprintf(w, "%-4s %9d %7d %10.1f %9.2f %9.2f %9.2f %9.2f %9.2f\n",
			name, s.Count, s.Errors, s.OpsPerSec, s.MBPerSec, s.P50ms, s.P95ms, s.P99ms, s.Maxms)
	}
	for _, name := range []string{"PUT", "GET"} {
		s := r.Summary[name]
		for k, v := range s.ErrorKinds {
			fmt.Fprintf(w, "  %s error %q x%d\n", name, k, v)
		}
		if len(s.Cache) > 0 {
			var total uint64
			for _, v := range s.Cache {
				total += v
			}
			fmt.Fprintf(w, "  %s cache sources:", name)
			for k, v := range s.Cache {
				fmt.Fprintf(w, " %s=%d (%.1f%%)", k, v, 100*float64(v)/float64(total))
			}
			fmt.Fprintln(w)
		}
	}
	fmt.Fprintf(w, "\nclient: heap %.1f MiB, %d GC cycles\n", r.ClientHeapMB, r.ClientGCCycles)
	if len(r.ServerDeltas) > 0 {
		fmt.Fprintln(w, "\nserver counter deltas over the run (summed across label values):")
		names := make([]string, 0, len(r.ServerDeltas))
		for k := range r.ServerDeltas {
			names = append(names, k)
		}
		sort.Strings(names)
		for _, k := range names {
			fmt.Fprintf(w, "  %-70s %16.0f\n", k, r.ServerDeltas[k])
		}
	}
}

// ---------------------------------------------------------------------------
// Prometheus text scraping (counters only, summed over labels).

func scrapeAll(urls string, re *regexp.Regexp) map[string]float64 {
	out := map[string]float64{}
	if re == nil {
		return out
	}
	for _, u := range strings.Split(urls, ",") {
		u = strings.TrimSpace(u)
		if u == "" {
			continue
		}
		resp, err := http.Get(u)
		if err != nil {
			fmt.Fprintf(os.Stderr, "scrape %s: %v\n", u, err)
			continue
		}
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			line := sc.Text()
			if line == "" || line[0] == '#' {
				continue
			}
			sp := strings.LastIndexByte(line, ' ')
			if sp < 0 {
				continue
			}
			name := line[:sp]
			if i := strings.IndexByte(name, '{'); i >= 0 {
				name = name[:i]
			}
			if strings.HasSuffix(name, "_bucket") || !re.MatchString(name) {
				continue
			}
			v, err := strconv.ParseFloat(line[sp+1:], 64)
			if err != nil {
				continue
			}
			host := strings.TrimPrefix(strings.TrimPrefix(u, "http://"), "https://")
			host, _, _ = strings.Cut(host, "/")
			out[host+" "+name] += v
		}
		resp.Body.Close()
	}
	return out
}

func diff(a, b map[string]float64) map[string]float64 {
	out := map[string]float64{}
	for k, v := range b {
		if d := v - a[k]; d != 0 {
			out[k] = d
		}
	}
	return out
}
