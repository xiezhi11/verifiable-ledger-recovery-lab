package server_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"ledgerlab/ledger"
	"ledgerlab/server"
)

func startServer(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	d := filepath.Join(t.TempDir(), "ledger")
	srv, err := server.New(d)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		srv.Close()
	})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts, d
}

func postJSON(t *testing.T, url string, body any) (int, map[string]any) {
	t.Helper()
	data, _ := json.Marshal(body)
	resp, err := http.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func getJSON(t *testing.T, url string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func batchBody(client, id string, keys ...string) map[string]any {
	ev := make([]map[string]any, len(keys))
	for i, k := range keys {
		ev[i] = map[string]any{
			"key":       k,
			"type":      "created",
			"timestamp": time.Now().UTC().Format(time.RFC3339),
			"payload":   map[string]string{"i": fmt.Sprintf("%d", i)},
		}
	}
	return map[string]any{"client_id": client, "batch_id": id, "events": ev}
}

func TestHTTPAppendQueryAndStatus(t *testing.T) {
	ts, _ := startServer(t)
	code, body := postJSON(t, ts.URL+"/events", batchBody("c1", "b1", "k1", "k2"))
	if code != http.StatusCreated {
		t.Fatalf("create status %d body=%v", code, body)
	}
	if body["start_seq"].(float64) != 1 || body["end_seq"].(float64) != 2 {
		t.Fatalf("bad receipt %v", body)
	}

	code, v := getJSON(t, ts.URL+"/events/seq/2")
	if code != 200 || v["key"] != "k2" {
		t.Fatalf("seq lookup %d %v", code, v)
	}
	code, v = getJSON(t, ts.URL+"/events/key/k1")
	if code != 200 || v["seq"].(float64) != 1 {
		t.Fatalf("key lookup %d %v", code, v)
	}
	code, _ = getJSON(t, ts.URL+"/events/seq/99")
	if code != http.StatusNotFound {
		t.Fatalf("missing seq status %d", code)
	}

	code, st := getJSON(t, ts.URL+"/status")
	if code != 200 || st["tail_seq"].(float64) != 2 {
		t.Fatalf("status %d %v", code, st)
	}
	if cp, ok := st["last_checkpoint"].(map[string]any); !ok || cp["tail_hash"] == nil {
		t.Fatalf("checkpoint missing in status: %v", st)
	}
}

func TestHTTPDuplicateRetryNotDoubled(t *testing.T) {
	ts, _ := startServer(t)
	body := batchBody("c1", "dup", "k1")
	c1, r1 := postJSON(t, ts.URL+"/events", body)
	c2, r2 := postJSON(t, ts.URL+"/events", body)
	if c1 != 201 || c2 != 200 {
		t.Fatalf("codes %d %d", c1, c2)
	}
	if r2["accepted"] != false || r2["existing"] == nil {
		t.Fatalf("retry should report prior receipt: %v", r2)
	}
	if r1["tail_hash"] != r2["tail_hash"] {
		t.Fatal("tail hash differs after retry")
	}
	_, st := getJSON(t, ts.URL+"/status")
	if st["tail_seq"].(float64) != 1 {
		t.Fatalf("retry appended again: %v", st)
	}

	// Same batch id, different body -> 409 conflict.
	diff := batchBody("c1", "dup", "k1", "k9")
	code, cerr := postJSON(t, ts.URL+"/events", diff)
	if code != http.StatusConflict || cerr["kind"] != string(ledger.ErrBatchConflict) {
		t.Fatalf("conflict not surfaced: %d %v", code, cerr)
	}
}

func TestHTTPConcurrentWritersAndReaders(t *testing.T) {
	ts, _ := startServer(t)
	const writers = 8
	const perWriter = 5
	var wg sync.WaitGroup
	errCh := make(chan error, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				key := fmt.Sprintf("w%d-e%d", w, i)
				code, body := postJSON(t, ts.URL+"/events", batchBody(fmt.Sprintf("w%d", w), fmt.Sprintf("b%d", i), key))
				if code != 201 {
					errCh <- fmt.Errorf("append %s code %d body=%v", key, code, body)
					return
				}
			}
		}(w)
	}
	// Concurrent readers must never see partial records.
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				http.Get(ts.URL + "/status")
			}
		}
	}()
	wg.Wait()
	close(errCh)
	close(stop)
	for e := range errCh {
		t.Error(e)
	}

	_, st := getJSON(t, ts.URL+"/status")
	if st["tail_seq"].(float64) != writers*perWriter {
		t.Fatalf("tail %v want %d", st["tail_seq"], writers*perWriter)
	}
	code, vr := postJSON(t, ts.URL+"/verify", map[string]any{})
	if code != 200 || vr["ok"] != true {
		t.Fatalf("verify after concurrency: %d %v", code, vr)
	}
	// Every key exists exactly once at the right seq window.
	for w := 0; w < writers; w++ {
		for i := 0; i < perWriter; i++ {
			code, _ := getJSON(t, ts.URL+"/events/key/"+fmt.Sprintf("w%d-e%d", w, i))
			if code != 200 {
				t.Fatalf("missing key w%d-e%d: %d", w, i, code)
			}
		}
	}
}

func TestHTTPConcurrentSameBatchIdempotent(t *testing.T) {
	ts, _ := startServer(t)
	body := batchBody("c", "same", "only-once")
	const n = 12
	var wg sync.WaitGroup
	codes := make([]int, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, _ := postJSON(t, ts.URL+"/events", body)
			codes[i] = c
		}(i)
	}
	wg.Wait()
	created, accepted := 0, 0
	for _, c := range codes {
		switch c {
		case 201:
			created++
		case 200:
			accepted++
		default:
			t.Fatalf("unexpected code %d", c)
		}
	}
	if created != 1 {
		t.Fatalf("exactly one create expected, got %d creates and %d replays", created, accepted)
	}
	_, st := getJSON(t, ts.URL+"/status")
	if st["tail_seq"].(float64) != 1 {
		t.Fatalf("tail %v", st["tail_seq"])
	}
}

func TestHTTPRecoverFromDirtyTailAndAppend(t *testing.T) {
	ts, d := startServer(t)
	postJSON(t, ts.URL+"/events", batchBody("c", "b1", "k1", "k2"))

	// Stop the process by closing the backing store and corrupt the tail.
	ts.Close()
	p := filepath.Join(d, "log.dat")
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	raw = append(raw, 'X', 'X', 'X')
	if err := os.WriteFile(p, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	srv2, err := server.New(d)
	if err != nil {
		t.Fatal(err)
	}
	ts2 := httptest.NewServer(srv2.Handler())
	defer ts2.Close()
	defer srv2.Close()

	_, st := getJSON(t, ts2.URL+"/status")
	if st["tail_seq"].(float64) != 2 {
		t.Fatalf("recovered tail %v", st["tail_seq"])
	}
	if dm, ok := st["damage"].(map[string]any); !ok || dm["offset"] == nil {
		t.Fatalf("damage offset missing: %v", st)
	}

	code, body := postJSON(t, ts2.URL+"/events", batchBody("c", "b2", "k3"))
	if code != 201 || body["start_seq"].(float64) != 3 {
		t.Fatalf("append after recovery failed: %d %v", code, body)
	}
	code, vr := postJSON(t, ts2.URL+"/verify", map[string]any{})
	if code != 200 || vr["ok"] != true {
		t.Fatalf("verify after recovery: %v", vr)
	}
}

func TestHTTPVerifyFromArbitrarySeq(t *testing.T) {
	ts, _ := startServer(t)
	for i := 0; i < 4; i++ {
		postJSON(t, ts.URL+"/events", batchBody("c", fmt.Sprintf("b%d", i), fmt.Sprintf("k%d", i)))
	}
	code, vr := postJSON(t, ts.URL+"/verify?from=3", map[string]any{})
	if code != 200 || vr["ok"] != true || vr["from_seq"].(float64) != 3 || vr["to_seq"].(float64) != 4 {
		t.Fatalf("range verify %v", vr)
	}
	code, _ = postJSON(t, ts.URL+"/verify?from=99", map[string]any{})
	if code != 404 {
		t.Fatalf("out of range verify code %d", code)
	}
}

func TestHTTPBadRequests(t *testing.T) {
	ts, _ := startServer(t)
	// Not JSON.
	resp, err := http.Post(ts.URL+"/events", "application/json", bytes.NewReader([]byte("{")))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("malformed json code %d", resp.StatusCode)
	}
	// Missing batch id.
	code, body := postJSON(t, ts.URL+"/events", map[string]any{"client_id": "c", "events": []any{}})
	if code != 400 {
		t.Fatalf("missing batch id code %d body=%v", code, body)
	}
}

func TestHTTPRecoverEndpointIdempotent(t *testing.T) {
	ts, _ := startServer(t)
	postJSON(t, ts.URL+"/events", batchBody("c", "b1", "k1"))

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/recover", nil)
	r1, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer r1.Body.Close()
	var out1 struct {
		Status struct {
			TailSeq float64 `json:"tail_seq"`
		} `json:"status"`
		Reopened bool `json:"reopened"`
	}
	json.NewDecoder(r1.Body).Decode(&out1)
	if r1.StatusCode != 200 || !out1.Reopened || out1.Status.TailSeq != 1 {
		t.Fatalf("first recover: %d %+v", r1.StatusCode, out1)
	}

	// Second recovery on the healthy prefix produces the same tail.
	req2, _ := http.NewRequest(http.MethodPost, ts.URL+"/recover", nil)
	r2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Body.Close()
	var out2 struct {
		Status struct {
			TailSeq float64 `json:"tail_seq"`
		} `json:"status"`
	}
	json.NewDecoder(r2.Body).Decode(&out2)
	if r2.StatusCode != 200 || out2.Status.TailSeq != 1 {
		t.Fatalf("second recover: %d %+v", r2.StatusCode, out2)
	}

	// Appends continue with the same contiguous sequence.
	code, body := postJSON(t, ts.URL+"/events", batchBody("c", "b2", "k2"))
	if code != 201 || body["start_seq"].(float64) != 2 {
		t.Fatalf("append after endpoint recovery: %d %v", code, body)
	}
}

func TestHTTPVerifyAppendsMutualExclusionAndReaders(t *testing.T) {
	ts, _ := startServer(t)
	postJSON(t, ts.URL+"/events", batchBody("c", "b0", "k0"))

	// Fire a verify and concurrently append/read. Regardless of scheduling,
	// verify must pass against a consistent tail and no record is half-read.
	var wg sync.WaitGroup
	wg.Add(3)
	var verifyOK bool
	go func() {
		defer wg.Done()
		c, v := postJSON(t, ts.URL+"/verify?from=1", map[string]any{})
		verifyOK = c == 200 && v["ok"] == true
	}()
	go func() {
		defer wg.Done()
		postJSON(t, ts.URL+"/events", batchBody("c", "b1", "k1"))
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			c, _ := getJSON(t, ts.URL+"/status")
			if c != 200 {
				t.Errorf("status read failed %d", c)
			}
		}
	}()
	wg.Wait()
	if !verifyOK {
		t.Fatal("verify observed an inconsistent tail")
	}
	code, v := postJSON(t, ts.URL+"/verify?from=1", map[string]any{})
	if code != 200 || v["ok"] != true || v["to_seq"].(float64) != 2 {
		t.Fatalf("final verify: %v", v)
	}
}
