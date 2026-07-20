package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/net/proxy"
)

// ──────────────────────────────────────────────
//  CONFIG
// ──────────────────────────────────────────────

const (
	MaxGroupID      int64 = 32_000_000
	MinGroupID      int64 = 0
	BatchSize             = 100
	BatchAPIBase          = "https://groups.roblox.com/v2/groups?groupIds="
	SingleAPIBase         = "https://groups.roblox.com/v1/groups/"
	RobloxGroupURL        = "https://www.roblox.com/groups/"
	MaxRetries            = 2
	RetryBaseDelay        = 500 * time.Millisecond
	WebhookCooldown       = 600 * time.Millisecond
)

// ──────────────────────────────────────────────
//  API TYPES
// ──────────────────────────────────────────────

type BatchResponse struct {
	Data []BatchGroup `json:"data"`
}

type BatchGroup struct {
	ID          int64           `json:"id"`
	Name        string          `json:"name"`
	MemberCount int64           `json:"memberCount"`
	Owner       json.RawMessage `json:"owner"`
}

type GroupDetail struct {
	ID                 int64           `json:"id"`
	Name               string          `json:"name"`
	MemberCount        int64           `json:"memberCount"`
	Owner              json.RawMessage `json:"owner"`
	PublicEntryAllowed bool            `json:"publicEntryAllowed"`
	IsLocked           bool            `json:"isLocked"`
}

// ──────────────────────────────────────────────
//  DISCORD
// ──────────────────────────────────────────────

type DiscordPayload struct {
	Username string         `json:"username,omitempty"`
	Embeds   []DiscordEmbed `json:"embeds"`
}

type DiscordEmbed struct {
	Title     string       `json:"title"`
	URL       string       `json:"url,omitempty"`
	Color     int          `json:"color"`
	Fields    []EmbedField `json:"fields,omitempty"`
	Footer    *EmbedFooter `json:"footer,omitempty"`
	Timestamp string       `json:"timestamp,omitempty"`
}

type EmbedField struct {
	Name   string `json:"name"`
	Value  string `json:"value"`
	Inline bool   `json:"inline"`
}

type EmbedFooter struct {
	Text string `json:"text"`
}

// ──────────────────────────────────────────────
//  HIT
// ──────────────────────────────────────────────

type Hit struct {
	ID          int64
	Name        string
	MemberCount int64
}

// ──────────────────────────────────────────────
//  OWNERLESS CHECK
// ──────────────────────────────────────────────

func isOwnerless(raw json.RawMessage) bool {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "null" || trimmed == "{}" {
		return true // explicit null or empty object
	}
	return false
}

// ──────────────────────────────────────────────
//  PROXY POOL
// ──────────────────────────────────────────────

type ProxyPool struct {
	clients []*http.Client
	counter uint64
}

func NewProxyPool(path string, timeout time.Duration) (*ProxyPool, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open proxy file: %w", err)
	}
	defer file.Close()

	pool := &ProxyPool{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		client, err := buildSOCKS5Client(line, timeout)
		if err != nil {
			log.Printf("[WARN] skip proxy %s: %v", line, err)
			continue
		}
		pool.clients = append(pool.clients, client)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(pool.clients) == 0 {
		return nil, fmt.Errorf("no valid proxies")
	}
	return pool, nil
}

func (p *ProxyPool) Next() *http.Client {
	idx := atomic.AddUint64(&p.counter, 1) % uint64(len(p.clients))
	return p.clients[idx]
}

func (p *ProxyPool) Size() int { return len(p.clients) }

func buildSOCKS5Client(raw string, timeout time.Duration) (*http.Client, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	var auth *proxy.Auth
	if parsed.User != nil {
		pass, _ := parsed.User.Password()
		auth = &proxy.Auth{User: parsed.User.Username(), Password: pass}
	}
	forward := &net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second}
	dialer, err := proxy.SOCKS5("tcp", parsed.Host, auth, forward)
	if err != nil {
		return nil, err
	}
	cd, ok := dialer.(proxy.ContextDialer)
	if !ok {
		return nil, fmt.Errorf("no DialContext support")
	}
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			DialContext:           cd.DialContext,
			MaxIdleConns:          20,
			MaxIdleConnsPerHost:   5,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   timeout,
			ResponseHeaderTimeout: timeout,
			DisableCompression:    true,
		},
	}, nil
}

// ──────────────────────────────────────────────
//  STATS
// ──────────────────────────────────────────────

type Stats struct {
	totalChecked int64
	totalFails   int64
	totalHits    int64
	groupsFound  int64
	totalPruned  int64
	currentPass  int64
	remaining    int64
	startTime    time.Time
	lastChecked  int64
}

func NewStats() *Stats                  { return &Stats{startTime: time.Now()} }
func (s *Stats) AddChecked(n int64)     { atomic.AddInt64(&s.totalChecked, n) }
func (s *Stats) AddFail()               { atomic.AddInt64(&s.totalFails, 100) }
func (s *Stats) AddHit()                { atomic.AddInt64(&s.totalHits, 1) }
func (s *Stats) AddGroupsFound(n int64) { atomic.AddInt64(&s.groupsFound, n) }
func (s *Stats) AddPruned(n int64)      { atomic.AddInt64(&s.totalPruned, n) }
func (s *Stats) SetPass(n int64)        { atomic.StoreInt64(&s.currentPass, n) }
func (s *Stats) SetRemaining(n int64)   { atomic.StoreInt64(&s.remaining, n) }

func (s *Stats) SnapshotCPM() int64 {
	cur := atomic.LoadInt64(&s.totalChecked)
	prev := atomic.SwapInt64(&s.lastChecked, cur)
	return (cur - prev) * 60
}

// ──────────────────────────────────────────────
//  HTTP HELPERS
// ──────────────────────────────────────────────

func doGet(client *http.Client, url string) ([]byte, int, error) {
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return body, resp.StatusCode, nil
}

func doGetRetry(pool *ProxyPool, url string) ([]byte, error) {
	var lastErr error
	for attempt := 0; attempt <= MaxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(RetryBaseDelay * time.Duration(1<<uint(attempt-1)))
		}
		body, code, err := doGet(pool.Next(), url)
		if err != nil {
			lastErr = err
			continue
		}
		if code == 200 {
			return body, nil
		}
		if code == 429 {
			lastErr = fmt.Errorf("429")
			time.Sleep(2 * time.Second * time.Duration(1<<uint(attempt)))
			continue
		}
		lastErr = fmt.Errorf("status %d", code)
	}
	return nil, lastErr
}

// ──────────────────────────────────────────────
//  URL BUILDER
// ──────────────────────────────────────────────

func batchToURL(ids []int64) string {
	var b strings.Builder
	b.Grow(len(BatchAPIBase) + len(ids)*10)
	b.WriteString(BatchAPIBase)
	for i, id := range ids {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatInt(id, 10))
	}
	return b.String()
}

// ──────────────────────────────────────────────
//  ID FILE I/O
// ──────────────────────────────────────────────

func loadIDs(path string) ([]int64, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer file.Close()

	seen := make(map[int64]struct{})
	var ids []int64
	sc := bufio.NewScanner(file)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		id, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			continue
		}
		if _, dup := seen[id]; !dup {
			seen[id] = struct{}{}
			ids = append(ids, id)
		}
	}
	return ids, sc.Err()
}

func writeIDs(path string, ids []int64) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(f, 256*1024)
	for _, id := range ids {
		fmt.Fprintf(w, "%d\n", id)
	}
	w.Flush()
	f.Close()
	return os.Rename(tmp, path)
}

// ──────────────────────────────────────────────
//  WEBHOOK
// ──────────────────────────────────────────────

func sendWebhook(webhookURL string, hit Hit) {
	link := fmt.Sprintf("%s%d", RobloxGroupURL, hit.ID)
	payload := DiscordPayload{
		Username: "Group Finder v3",
		Embeds: []DiscordEmbed{{
			Title: "\U0001F513 Ownerless Group Found",
			URL:   link,
			Color: 0x00FF00,
			Fields: []EmbedField{
				{Name: "\U0001F4DB Name", Value: hit.Name, Inline: false},
				{Name: "\U0001F194 Group ID", Value: strconv.FormatInt(hit.ID, 10), Inline: true},
				{Name: "\U0001F465 Members", Value: formatNumber(hit.MemberCount), Inline: true},
				{Name: "\U0001F517 Link", Value: fmt.Sprintf("[Open Group](%s)", link), Inline: false},
			},
			Footer:    &EmbedFooter{Text: "Group Finder v3"},
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		}},
	}
	data, _ := json.Marshal(payload)
	client := &http.Client{Timeout: 10 * time.Second}
	for i := 0; i < 3; i++ {
		req, _ := http.NewRequest("POST", webhookURL, bytes.NewReader(data))
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode == 200 || resp.StatusCode == 204 {
			return
		}
		if resp.StatusCode == 429 {
			time.Sleep(5 * time.Second)
			continue
		}
		return
	}
}

// ──────────────────────────────────────────────
//  HIT FILE LOG
// ──────────────────────────────────────────────

func logHit(path string, hit Hit) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return
	}
	defer f.Close()
	fmt.Fprintf(f, "[%s] ID=%d | Name=%s | Members=%d | %s%d\n",
		time.Now().Format("2006-01-02 15:04:05"),
		hit.ID, hit.Name, hit.MemberCount, RobloxGroupURL, hit.ID)
}

// ──────────────────────────────────────────────
//  UTIL
// ──────────────────────────────────────────────

func formatNumber(n int64) string {
	s := strconv.FormatInt(n, 10)
	if len(s) <= 3 {
		return s
	}
	var r strings.Builder
	rem := len(s) % 3
	if rem > 0 {
		r.WriteString(s[:rem])
	}
	for i := rem; i < len(s); i += 3 {
		if r.Len() > 0 {
			r.WriteByte(',')
		}
		r.WriteString(s[i : i+3])
	}
	return r.String()
}

// ══════════════════════════════════════════════
//  HARVEST MODE
// ══════════════════════════════════════════════

func runHarvest(pool *ProxyPool, stats *Stats, idFile string, dur time.Duration, workers int) {
	fmt.Printf("\033[32m[+]\033[0m Mode: \033[33mHARVEST\033[0m\n")
	fmt.Printf("\033[32m[+]\033[0m Duration: %s\n", dur)
	fmt.Printf("\033[32m[+]\033[0m Output: %s\n", idFile)
	fmt.Printf("\033[32m[+]\033[0m Workers: %d\n\n", workers)

	idCh := make(chan int64, 50000)
	stopCh := make(chan struct{})
	timer := time.NewTimer(dur)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// ── file writer goroutine ──
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		f, err := os.OpenFile(idFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			log.Fatalf("[FATAL] %v", err)
		}
		defer f.Close()
		w := bufio.NewWriterSize(f, 512*1024)
		defer w.Flush()
		tick := time.NewTicker(5 * time.Second)
		defer tick.Stop()
		for {
			select {
			case id, ok := <-idCh:
				if !ok {
					return
				}
				fmt.Fprintf(w, "%d\n", id)
			case <-tick.C:
				w.Flush()
			}
		}
	}()

	// ── stats display ──
	displayStop := make(chan struct{})
	go func() {
		tick := time.NewTicker(1 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-tick.C:
				checked := atomic.LoadInt64(&stats.totalChecked)
				fails := atomic.LoadInt64(&stats.totalFails)
				found := atomic.LoadInt64(&stats.groupsFound)
				cpm := stats.SnapshotCPM()
				elapsed := time.Since(stats.startTime).Truncate(time.Second)
				left := dur - elapsed
				if left < 0 {
					left = 0
				}
				fmt.Fprintf(os.Stderr,
					"\r\033[K\033[36m[HARVEST %s]\033[0m CPM: \033[33m%s\033[0m | Scanned: \033[32m%s\033[0m | Found: \033[34m%s\033[0m | Fails: \033[31m%s\033[0m | Left: %s",
					elapsed, formatNumber(cpm), formatNumber(checked),
					formatNumber(found), formatNumber(fails),
					left.Truncate(time.Second))
			case <-displayStop:
				fmt.Fprintln(os.Stderr)
				return
			}
		}
	}()

	// ── harvest workers ──
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(seed)*31))
			spread := MaxGroupID - MinGroupID + 1
			for {
				select {
				case <-stopCh:
					return
				default:
				}

				ids := make([]int64, BatchSize)
				for j := range ids {
					ids[j] = MinGroupID + rng.Int63n(spread)
				}

				body, err := doGetRetry(pool, batchToURL(ids))
				stats.AddChecked(int64(BatchSize))
				if err != nil {
					stats.AddFail()
					continue
				}

				var resp BatchResponse
				if err := json.Unmarshal(body, &resp); err != nil {
					stats.AddFail()
					continue
				}

				if len(resp.Data) > 0 {
					stats.AddGroupsFound(int64(len(resp.Data)))
					for _, g := range resp.Data {
						select {
						case idCh <- g.ID:
						case <-stopCh:
							return
						}
					}
				}
			}
		}(i)
	}

	// ── wait for duration or signal ──
	select {
	case <-timer.C:
		fmt.Fprintln(os.Stderr, "\n\033[33m[*]\033[0m Harvest time reached.")
	case <-sigCh:
		fmt.Fprintln(os.Stderr, "\n\033[33m[*]\033[0m Interrupted.")
	}

	close(stopCh)
	wg.Wait()
	close(idCh)
	<-writerDone
	close(displayStop)

	found := atomic.LoadInt64(&stats.groupsFound)
	checked := atomic.LoadInt64(&stats.totalChecked)
	elapsed := time.Since(stats.startTime).Truncate(time.Second)

	fmt.Fprintf(os.Stderr, "\033[36m════════════════════════════════════════\033[0m\n")
	fmt.Fprintf(os.Stderr, "\033[36m  HARVEST COMPLETE\033[0m\n")
	fmt.Fprintf(os.Stderr, "\033[36m════════════════════════════════════════\033[0m\n")
	fmt.Fprintf(os.Stderr, "  Runtime    : %s\n", elapsed)
	fmt.Fprintf(os.Stderr, "  Scanned    : %s IDs\n", formatNumber(checked))
	fmt.Fprintf(os.Stderr, "  Groups     : %s saved to %s\n", formatNumber(found), idFile)
	fmt.Fprintf(os.Stderr, "\033[36m════════════════════════════════════════\033[0m\n")
}

// ══════════════════════════════════════════════
//  SCAN MODE
// ══════════════════════════════════════════════

type scanBatch struct {
	ids []int64
	wg  *sync.WaitGroup
}

func runScan(pool *ProxyPool, stats *Stats, idFile, webhookURL, logFile string, workers int) {
	fmt.Printf("\033[32m[+]\033[0m Mode: \033[33mSCAN\033[0m\n")
	fmt.Printf("\033[32m[+]\033[0m Loading IDs from %s ...\n", idFile)

	ids, err := loadIDs(idFile)
	if err != nil {
		log.Fatalf("[FATAL] %v", err)
	}
	if len(ids) == 0 {
		log.Fatal("[FATAL] ID file is empty. Run harvest first.")
	}

	fmt.Printf("\033[32m[+]\033[0m Loaded \033[33m%s\033[0m unique group IDs\n", formatNumber(int64(len(ids))))
	fmt.Printf("\033[32m[+]\033[0m Workers: %d\n\n", workers)

	stats.SetRemaining(int64(len(ids)))

	batchCh := make(chan scanBatch, workers*2)
	hitCh := make(chan Hit, 500)
	stopCh := make(chan struct{})
	var stopped int32

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Fprintln(os.Stderr, "\n\033[33m[*]\033[0m Shutting down after current batches...")
		atomic.StoreInt32(&stopped, 1)
		close(stopCh)
	}()

	// ── removal tracker ──
	var removals sync.Map

	// ── hit handler: console + webhook + file ──
	hitDone := make(chan struct{})
	go func() {
		defer close(hitDone)
		for hit := range hitCh {
			link := fmt.Sprintf("%s%d", RobloxGroupURL, hit.ID)
			fmt.Fprintf(os.Stderr,
				"\n\033[32m[HIT]\033[0m \033[1m%s\033[0m | ID: \033[33m%d\033[0m | Members: \033[36m%s\033[0m | %s\n",
				hit.Name, hit.ID, formatNumber(hit.MemberCount), link)
			if logFile != "" {
				logHit(logFile, hit)
			}
			if webhookURL != "" {
				sendWebhook(webhookURL, hit)
				time.Sleep(WebhookCooldown)
			}
		}
	}()

	// ── stats display ──
	displayStop := make(chan struct{})
	go func() {
		tick := time.NewTicker(1 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-tick.C:
				checked := atomic.LoadInt64(&stats.totalChecked)
				fails := atomic.LoadInt64(&stats.totalFails)
				hits := atomic.LoadInt64(&stats.totalHits)
				pruned := atomic.LoadInt64(&stats.totalPruned)
				pass := atomic.LoadInt64(&stats.currentPass)
				rem := atomic.LoadInt64(&stats.remaining)
				cpm := stats.SnapshotCPM()
				elapsed := time.Since(stats.startTime).Truncate(time.Second)
				fmt.Fprintf(os.Stderr,
					"\r\033[K\033[36m[SCAN P%d %s]\033[0m CPM: \033[33m%s\033[0m | Checked: \033[32m%s\033[0m | Hits: \033[35m%s\033[0m | Pruned: \033[31m%s\033[0m | Remaining: \033[34m%s\033[0m | Fails: %s",
					pass, elapsed, formatNumber(cpm), formatNumber(checked),
					formatNumber(hits), formatNumber(pruned),
					formatNumber(rem), formatNumber(fails))
			case <-displayStop:
				fmt.Fprintln(os.Stderr)
				return
			}
		}
	}()

	// ── scan workers ──
	var workerWg sync.WaitGroup
	for i := 0; i < workers; i++ {
		workerWg.Add(1)
		go func() {
			defer workerWg.Done()
			for sb := range batchCh {
				processScanBatch(pool, stats, sb.ids, &removals, hitCh, stopCh)
				sb.wg.Done()
			}
		}()
	}

	// ── pass loop ──
	var lastPassWg *sync.WaitGroup

passLoop:
	for pass := int64(1); atomic.LoadInt32(&stopped) == 0; pass++ {
		stats.SetPass(pass)
		stats.SetRemaining(int64(len(ids)))

		// shuffle each pass so we don't always hit same order
		rand.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })

		var passWg sync.WaitGroup
		lastPassWg = &passWg

		for i := 0; i < len(ids); i += BatchSize {
			if atomic.LoadInt32(&stopped) != 0 {
				break passLoop
			}

			end := i + BatchSize
			if end > len(ids) {
				end = len(ids)
			}
			batch := make([]int64, end-i)
			copy(batch, ids[i:end])

			passWg.Add(1)
			select {
			case batchCh <- scanBatch{ids: batch, wg: &passWg}:
			case <-stopCh:
				passWg.Done()
				break passLoop
			}
		}

		// wait for every batch in this pass to finish
		passWg.Wait()

		// ── prune removals ──
		toRemove := make(map[int64]struct{})
		removals.Range(func(k, v interface{}) bool {
			toRemove[k.(int64)] = struct{}{}
			return true
		})

		if len(toRemove) > 0 {
			fresh := make([]int64, 0, len(ids)-len(toRemove))
			for _, id := range ids {
				if _, rm := toRemove[id]; !rm {
					fresh = append(fresh, id)
				}
			}
			pruned := int64(len(ids) - len(fresh))
			stats.AddPruned(pruned)
			ids = fresh

			if err := writeIDs(idFile, ids); err != nil {
				log.Printf("[WARN] rewrite %s failed: %v", idFile, err)
			}

			fmt.Fprintf(os.Stderr,
				"\n\033[33m[PRUNE P%d]\033[0m Removed \033[31m%s\033[0m IDs | Remaining: \033[34m%s\033[0m\n",
				pass, formatNumber(pruned), formatNumber(int64(len(ids))))

			// reset for next pass
			removals = sync.Map{}
		} else {
			fmt.Fprintf(os.Stderr,
				"\n\033[33m[PASS %d]\033[0m Complete | 0 pruned | Remaining: \033[34m%s\033[0m\n",
				pass, formatNumber(int64(len(ids))))
		}

		stats.SetRemaining(int64(len(ids)))

		if len(ids) == 0 {
			fmt.Fprintln(os.Stderr, "\033[33m[*]\033[0m All IDs pruned. Nothing left to scan.")
			break
		}
	}

	// ── shutdown ──
	if lastPassWg != nil {
		lastPassWg.Wait()
	}
	close(batchCh)
	workerWg.Wait()
	close(hitCh)
	<-hitDone
	close(displayStop)

	checked := atomic.LoadInt64(&stats.totalChecked)
	fails := atomic.LoadInt64(&stats.totalFails)
	hits := atomic.LoadInt64(&stats.totalHits)
	pruned := atomic.LoadInt64(&stats.totalPruned)
	pass := atomic.LoadInt64(&stats.currentPass)
	elapsed := time.Since(stats.startTime).Truncate(time.Second)

	fmt.Fprintln(os.Stderr)
	fmt.Fprintf(os.Stderr, "\033[36m════════════════════════════════════════\033[0m\n")
	fmt.Fprintf(os.Stderr, "\033[36m  SCAN RESULTS\033[0m\n")
	fmt.Fprintf(os.Stderr, "\033[36m════════════════════════════════════════\033[0m\n")
	fmt.Fprintf(os.Stderr, "  Runtime    : %s\n", elapsed)
	fmt.Fprintf(os.Stderr, "  Passes     : %s\n", formatNumber(pass))
	fmt.Fprintf(os.Stderr, "  Checked    : %s\n", formatNumber(checked))
	fmt.Fprintf(os.Stderr, "  Hits       : %s\n", formatNumber(hits))
	fmt.Fprintf(os.Stderr, "  Pruned     : %s\n", formatNumber(pruned))
	fmt.Fprintf(os.Stderr, "  Remaining  : %s\n", formatNumber(int64(len(ids))))
	fmt.Fprintf(os.Stderr, "  Fails      : %s\n", formatNumber(fails))
	fmt.Fprintf(os.Stderr, "\033[36m════════════════════════════════════════\033[0m\n")
}

// ──────────────────────────────────────────────
//  SCAN BATCH PROCESSOR
//
//  This is where the real work happens per batch:
//
//  1. v2 batch → find which IDs still exist
//  2. IDs missing from v2 response → deleted → remove from list
//  3. Existing groups with owner in v2 → skip (keep for future passes)
//  4. Existing groups ownerless in v2 → call v1
//  5. v1: locked or not publicly joinable → remove from list
//  6. v1: joinable + not locked → HIT → webhook + console
// ──────────────────────────────────────────────

func processScanBatch(pool *ProxyPool, stats *Stats, ids []int64, removals *sync.Map, hitCh chan Hit, stopCh chan struct{}) {
	// ── step 1: v2 batch call ──
	body, err := doGetRetry(pool, batchToURL(ids))
	stats.AddChecked(int64(len(ids)))
	if err != nil {
		stats.AddFail()
		return
	}

	var resp BatchResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		stats.AddFail()
		return
	}

	// ── step 2: detect deleted groups ──
	returned := make(map[int64]struct{}, len(resp.Data))
	for _, g := range resp.Data {
		returned[g.ID] = struct{}{}
	}
	for _, id := range ids {
		if _, exists := returned[id]; !exists {
			removals.Store(id, struct{}{}) // group deleted → prune
		}
	}

	// ── step 3-6: check ownerless → v1 verify ──
	for _, g := range resp.Data {
		// step 3: has owner in v2 → skip, keep in list
		if !isOwnerless(g.Owner) {
			continue
		}

		// step 4: ownerless in v2 → call v1
		select {
		case <-stopCh:
			return
		default:
		}

		v1Body, err := doGetRetry(pool, SingleAPIBase+strconv.FormatInt(g.ID, 10))
		if err != nil {
			stats.AddFail()
			continue
		}

		var detail GroupDetail
		if err := json.Unmarshal(v1Body, &detail); err != nil {
			stats.AddFail()
			continue
		}

		// step 5: locked → remove
		if detail.IsLocked {
			removals.Store(g.ID, struct{}{})
			continue
		}

		// step 5: not publicly joinable → remove
		if !detail.PublicEntryAllowed {
			removals.Store(g.ID, struct{}{})
			continue
		}

		// step 6: HIT — ownerless + joinable + not locked
		stats.AddHit()
		select {
		case hitCh <- Hit{
			ID:          detail.ID,
			Name:        detail.Name,
			MemberCount: detail.MemberCount,
		}:
		case <-stopCh:
			return
		}
	}
}

// ──────────────────────────────────────────────
//  BANNER
// ──────────────────────────────────────────────

func printBanner() {
	b := `
 ██████╗ ██████╗  ██████╗ ██╗   ██╗██████╗ 
██╔════╝ ██╔══██╗██╔═══██╗██║   ██║██╔══██╗
██║  ███╗██████╔╝██║   ██║██║   ██║██████╔╝
██║   ██║██╔══██╗██║   ██║██║   ██║██╔═══╝ 
╚██████╔╝██║  ██║╚██████╔╝╚██████╔╝██║     
 ╚═════╝ ╚═╝  ╚═╝ ╚═════╝  ╚═════╝ ╚═╝     
 ███████╗██╗███╗   ██╗██████╗ ███████╗██████╗ 
 ██╔════╝██║████╗  ██║██╔══██╗██╔════╝██╔══██╗
 █████╗  ██║██╔██╗ ██║██║  ██║█████╗  ██████╔╝
 ██╔══╝  ██║██║╚██╗██║██║  ██║██╔══╝  ██╔══██╗
 ██║     ██║██║ ╚████║██████╔╝███████╗██║  ██║
 ╚═╝     ╚═╝╚═╝  ╚═══╝╚═════╝ ╚══════╝╚═╝  ╚═╝
                    v3 - RoAutomation`
	fmt.Println("\033[36m" + b + "\033[0m\n")
}

// ──────────────────────────────────────────────
//  MAIN
// ──────────────────────────────────────────────

func main() {
	mode := flag.String("mode", "", "harvest or scan (required)")
	proxyFile := flag.String("proxies", "proxies.txt", "SOCKS5 proxy list")
	webhookURL := flag.String("webhook", "", "Discord webhook (scan mode)")
	idFile := flag.String("ids", "groups.txt", "Group ID file")
	workersPerProxy := flag.Int("workers", 1, "Workers per proxy")
	timeout := flag.Duration("timeout", 12*time.Second, "HTTP timeout")
	dur := flag.Duration("duration", 10*time.Minute, "Harvest duration")
	logFile := flag.String("log", "hits.log", "Hit log file")
	flag.Parse()

	if *mode == "" {
		fmt.Fprintln(os.Stderr, "\033[31m[ERROR]\033[0m --mode required (harvest / scan)")
		flag.Usage()
		os.Exit(1)
	}

	printBanner()

	fmt.Printf("\033[33m[*]\033[0m Loading proxies from %s ...\n", *proxyFile)
	pool, err := NewProxyPool(*proxyFile, *timeout)
	if err != nil {
		log.Fatalf("\033[31m[FATAL]\033[0m %v", err)
	}
	fmt.Printf("\033[32m[+]\033[0m Loaded \033[33m%d\033[0m proxies\n\n", pool.Size())

	totalWorkers := pool.Size() * (*workersPerProxy)
	stats := NewStats()

	switch *mode {
	case "harvest":
		runHarvest(pool, stats, *idFile, *dur, totalWorkers)
	case "scan":
		if *webhookURL == "" {
			fmt.Fprintln(os.Stderr, "\033[31m[ERROR]\033[0m --webhook required for scan mode")
			os.Exit(1)
		}
		runScan(pool, stats, *idFile, *webhookURL, *logFile, totalWorkers)
	default:
		fmt.Fprintf(os.Stderr, "\033[31m[ERROR]\033[0m unknown mode: %s\n", *mode)
		os.Exit(1)
	}
}
